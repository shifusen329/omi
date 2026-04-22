"""
Local Whisper streaming client.

The self-hosted STT server at ws://host:8000/v1/listen speaks Deepgram's
LiveTranscription JSON protocol but only emits finalized `Results` frames
on endpointing-triggered silence (no per-word timestamps, no speaker IDs).

`LocalWhisperSocket` duck-types the subset of SafeDeepgramSocket that the
rest of the pipeline touches — `.send()`, `.finalize()`, `.finish()`,
`.set_close_reason()`, and the `is_connection_dead` / `death_reason`
properties — so existing code in routers/transcribe.py consumes it
without knowing which provider is on the other end.

Provider dispatch: routers/transcribe.py picks this path when
STT_SERVICE=local_whisper. See utils/stt/streaming.py for the factory.
"""
import json
import logging
import os
import threading
from types import SimpleNamespace
from typing import Callable, Optional
from urllib.parse import urlencode, urlparse, urlunparse

from websockets.sync.client import connect as sync_ws_connect

logger = logging.getLogger(__name__)


DEFAULT_WS_URL = os.getenv('STT_WS_URL', 'ws://192.168.0.107:8000/v1/listen')
DEFAULT_WS_MODEL = os.getenv('STT_WS_MODEL', 'turbo')


def build_ws_url(
    language: str,
    sample_rate: int,
    model: Optional[str] = None,
    base_url: Optional[str] = None,
) -> str:
    base = base_url or DEFAULT_WS_URL
    params = {
        'model': model or DEFAULT_WS_MODEL,
        'language': language,
        'encoding': 'linear16',
        'sample_rate': sample_rate,
    }
    parsed = urlparse(base)
    qs = urlencode(params)
    # Keep any query string the operator baked into STT_WS_URL, merge our params after.
    combined = (parsed.query + '&' if parsed.query else '') + qs
    return urlunparse(parsed._replace(query=combined))


class LocalWhisperSocket:
    """Duck-typed SafeDeepgramSocket over the local Whisper WebSocket."""

    _is_safe_dg_socket = True  # passes downstream duck-type checks

    def __init__(
        self,
        url: str,
        on_message: Callable,
        on_error: Callable,
        on_close: Optional[Callable] = None,
    ):
        self._url = url
        self._on_message = on_message
        self._on_error = on_error
        self._on_close = on_close or (lambda *_args, **_kwargs: None)
        self._ws = None
        self._reader: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._closed = False
        self._dead = False
        self._death_reason: Optional[str] = None

    # ------------------------------------------------------------------
    # Deepgram-compatible surface
    # ------------------------------------------------------------------

    @property
    def is_connection_dead(self) -> bool:
        return self._dead

    @property
    def death_reason(self) -> Optional[str]:
        return self._death_reason

    @property
    def keepalive_count(self) -> int:
        return 0  # server doesn't need keepalives

    def send(self, data: bytes) -> None:
        with self._lock:
            if self._dead or self._closed or self._ws is None:
                return
            try:
                self._ws.send(data)
            except Exception as e:
                self._death_reason = f'send {type(e).__name__}: {e}'
                self._dead = True
                logger.warning('local_ws send failed: %s', self._death_reason)

    def keep_alive(self) -> bool:  # parity with Deepgram SDK
        return True

    def finalize(self) -> None:
        # The local server endpoints on server-side silence detection; nothing
        # to do here. Left as a no-op so VAD gate can call it without branching.
        pass

    def finish(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass

    def set_close_reason(self, reason: str) -> None:
        if self._death_reason is None:
            self._death_reason = reason

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def start(self) -> bool:
        try:
            self._ws = sync_ws_connect(self._url, max_size=2**24, open_timeout=10)
        except Exception as e:
            self._dead = True
            self._death_reason = f'connect {type(e).__name__}: {e}'
            logger.error('local_ws connect failed: %s', self._death_reason)
            return False

        self._reader = threading.Thread(target=self._read_loop, daemon=True, name='local-whisper-reader')
        self._reader.start()
        return True

    def _read_loop(self) -> None:
        try:
            for raw in self._ws:  # type: ignore[arg-type]
                if isinstance(raw, bytes):
                    continue
                try:
                    frame = json.loads(raw)
                except Exception:
                    logger.debug('local_ws non-JSON frame: %r', raw[:200] if isinstance(raw, str) else raw)
                    continue
                ftype = frame.get('type')
                if ftype == 'Results':
                    self._emit_result(frame)
                elif ftype in ('Error', 'Warning'):
                    try:
                        self._on_error(self, frame)
                    except Exception as e:
                        logger.error('local_ws on_error handler raised: %s', e)
                else:
                    logger.debug('local_ws unhandled frame type %r', ftype)
        except Exception as e:
            self._death_reason = self._death_reason or f'reader {type(e).__name__}: {e}'
            self._dead = True
            logger.info('local_ws reader ended: %s', self._death_reason)
        finally:
            try:
                self._on_close(self, None)
            except Exception as e:
                logger.debug('local_ws on_close handler raised: %s', e)

    def _emit_result(self, frame: dict) -> None:
        """Normalize a Results frame to the SimpleNamespace shape Deepgram handlers expect."""
        alt_data = (frame.get('channel') or {}).get('alternatives') or [{}]
        alt = alt_data[0] if alt_data else {}
        # The local server doesn't provide per-word breakdowns; callers that rely
        # on result.channel.alternatives[0].words must handle empty lists.
        alternative = SimpleNamespace(
            transcript=alt.get('transcript') or '',
            words=[],
            confidence=alt.get('confidence') or 1.0,
        )
        channel = SimpleNamespace(alternatives=[alternative])
        result = SimpleNamespace(
            channel=channel,
            start=float(frame.get('start') or 0.0),
            duration=float(frame.get('duration') or 0.0),
            is_final=bool(frame.get('is_final', True)),
            speech_final=bool(frame.get('speech_final', True)),
            # raw frame attached for debugging / future enhancements
            _raw=frame,
        )
        try:
            self._on_message(self, result)
        except Exception as e:
            logger.error('local_ws on_message handler raised: %s', e)


def connect_local_whisper(
    on_message: Callable,
    on_error: Callable,
    on_close: Optional[Callable],
    language: str,
    sample_rate: int,
    model: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Optional[LocalWhisperSocket]:
    """Open a LocalWhisperSocket. Returns None on connect failure."""
    url = build_ws_url(language=language, sample_rate=sample_rate, model=model, base_url=base_url)
    sock = LocalWhisperSocket(url=url, on_message=on_message, on_error=on_error, on_close=on_close)
    ok = sock.start()
    if not ok:
        return None
    logger.info('local_ws connected: %s', url)
    return sock
