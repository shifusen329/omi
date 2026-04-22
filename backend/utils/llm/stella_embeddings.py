import logging
import os
from typing import List, Optional

import httpx

logger = logging.getLogger(__name__)


class StellaEmbeddings:
    """
    Embeddings adapter for Ollama's /api/embed endpoint.

    Exposes the langchain Embeddings contract actually used in this codebase:
    `embed_query(str) -> list[float]` and `embed_documents(list[str]) -> list[list[float]]`.
    Drop-in replacement for the `embeddings` module-level object in utils.llm.clients.

    Ollama /api/embed payload: {"model": str, "input": str | list[str]}
    Response shape: {"embeddings": [[...], [...]]}
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: float = 60.0,
    ):
        self._base_url = (base_url or os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434')).rstrip('/')
        self._model = model or os.environ.get('EMBEDDINGS_MODEL', 'stella_en_1.5B_v5:latest')
        self._url = f'{self._base_url}/api/embed'
        self._sync = httpx.Client(timeout=timeout)
        self._async: Optional[httpx.AsyncClient] = None
        self._async_timeout = timeout

    def _post_sync(self, payload: dict) -> List[List[float]]:
        r = self._sync.post(self._url, json=payload)
        r.raise_for_status()
        return r.json()['embeddings']

    async def _post_async(self, payload: dict) -> List[List[float]]:
        if self._async is None:
            self._async = httpx.AsyncClient(timeout=self._async_timeout)
        r = await self._async.post(self._url, json=payload)
        r.raise_for_status()
        return r.json()['embeddings']

    def embed_query(self, text: str) -> List[float]:
        return self._post_sync({'model': self._model, 'input': text})[0]

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        return self._post_sync({'model': self._model, 'input': texts})

    async def aembed_query(self, text: str) -> List[float]:
        return (await self._post_async({'model': self._model, 'input': text}))[0]

    async def aembed_documents(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        return await self._post_async({'model': self._model, 'input': texts})

    def close(self):
        try:
            self._sync.close()
        except Exception:
            pass
        if self._async is not None:
            try:
                import asyncio

                asyncio.get_event_loop().create_task(self._async.aclose())
            except Exception:
                pass
