"""
pgvector-backed replacement for database/vector_db.py's Pinecone integration.

Exposes the *exact* public surface of vector_db.py so that every existing
caller works unchanged — same function names, same arguments, same return
shapes. The only setup differences are:

* POSTGRES_URL env var pointing at a pgvector-enabled Postgres.
* VECTOR_DB=pgvector in env (read by vector_db.py to dispatch here).
* Schema created from migrations/007_pgvector_init.sql.

Embedding dimension is 1536 (Stella on Ollama); HNSW indexes on all three
tables use vector_cosine_ops.
"""
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector

from utils.llm.clients import embeddings

logger = logging.getLogger(__name__)

_POSTGRES_URL = os.environ.get('POSTGRES_URL')


def _configure_connection(conn) -> None:
    register_vector(conn)


_pool: Optional[ConnectionPool] = None


def _get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        if not _POSTGRES_URL:
            raise RuntimeError('POSTGRES_URL is not set; set VECTOR_DB back to pinecone or configure Postgres.')
        _pool = ConnectionPool(
            _POSTGRES_URL,
            min_size=1,
            max_size=10,
            configure=_configure_connection,
            kwargs={'row_factory': dict_row},
        )
    return _pool


# ==========================================
# Conversation vectors — namespace ns1
# ==========================================


def _conv_row(uid: str, conversation_id: str, vector: List[float], metadata: Optional[Dict[str, Any]] = None):
    meta = metadata or {}
    return {
        'id': f'{uid}-{conversation_id}',
        'uid': uid,
        'memory_id': conversation_id,
        'embedding': vector,
        'created_at': meta.get('created_at') or int(datetime.now(timezone.utc).timestamp()),
        'people': meta.get('people') or meta.get('people_mentioned') or [],
        'topics': meta.get('topics') or [],
        'entities': meta.get('entities') or [],
        'dates': meta.get('dates') or meta.get('dates_mentioned') or [],
        'metadata': Jsonb({k: v for k, v in meta.items() if k not in {'people', 'people_mentioned', 'topics', 'entities', 'dates', 'dates_mentioned', 'created_at'}}),
    }


_CONV_UPSERT = """
INSERT INTO conversation_vectors (id, uid, memory_id, embedding, created_at, people, topics, entities, dates, metadata)
VALUES (%(id)s, %(uid)s, %(memory_id)s, %(embedding)s, %(created_at)s, %(people)s, %(topics)s, %(entities)s, %(dates)s, %(metadata)s)
ON CONFLICT (id) DO UPDATE SET
    embedding  = EXCLUDED.embedding,
    created_at = EXCLUDED.created_at,
    people     = EXCLUDED.people,
    topics     = EXCLUDED.topics,
    entities   = EXCLUDED.entities,
    dates      = EXCLUDED.dates,
    metadata   = EXCLUDED.metadata
"""


def upsert_vector(uid: str, conversation_id: str, vector: List[float]):
    row = _conv_row(uid, conversation_id, vector)
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_CONV_UPSERT, row)
    logger.info('upsert_vector %s', row['id'])


def upsert_vector2(uid: str, conversation_id: str, vector: List[float], metadata: Dict[str, Any]):
    row = _conv_row(uid, conversation_id, vector, metadata)
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_CONV_UPSERT, row)
    logger.info('upsert_vector2 %s', row['id'])


def upsert_vectors(uid: str, vectors: List[List[float]], conversation_ids: List[str]):
    rows = [_conv_row(uid, cid, v) for cid, v in zip(conversation_ids, vectors)]
    if not rows:
        return
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_CONV_UPSERT, rows)
    logger.info('upsert_vectors count=%d', len(rows))


def update_vector_metadata(uid: str, conversation_id: str, metadata: Dict[str, Any]):
    vec_id = f'{uid}-{conversation_id}'
    people = metadata.get('people') or metadata.get('people_mentioned') or []
    topics = metadata.get('topics') or []
    entities = metadata.get('entities') or []
    dates = metadata.get('dates') or metadata.get('dates_mentioned') or []
    residual = {k: v for k, v in metadata.items() if k not in {'people', 'people_mentioned', 'topics', 'entities', 'dates', 'dates_mentioned', 'uid', 'memory_id'}}
    q = """
    UPDATE conversation_vectors
       SET people=%s, topics=%s, entities=%s, dates=%s, metadata = metadata || %s
     WHERE id = %s
    """
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (people, topics, entities, dates, Jsonb(residual), vec_id))


def query_vectors(query: str, uid: str, starts_at: int = None, ends_at: int = None, k: int = 5) -> List[str]:
    """Semantic search by string; returns top-k conversation_ids."""
    xq = embeddings.embed_query(query)
    where = ['uid = %s']
    params: List[Any] = [uid]
    if starts_at is not None and ends_at is not None:
        where.append('created_at BETWEEN %s AND %s')
        params.extend([starts_at, ends_at])
    q = f"""
    SELECT memory_id
      FROM conversation_vectors
     WHERE {' AND '.join(where)}
     ORDER BY embedding <=> %s::vector
     LIMIT %s
    """
    params.extend([xq, k])
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            return [r['memory_id'] for r in cur.fetchall()]


def query_vectors_by_metadata(
    uid: str,
    vector: List[float],
    dates_filter: List[datetime],
    people: List[str],
    topics: List[str],
    entities: List[str],
    dates: List[str],
    limit: int = 5,
):
    """
    Semantic + metadata-filtered search.

    Mirrors Pinecone's $and + $or behaviour: uid AND (people && OR topics && OR entities &&),
    optionally intersected with a created_at range. Falls back to no-metadata search if
    the metadata filter returns nothing.
    """

    def run(with_meta: bool, with_dates: bool, top_k: int) -> List[dict]:
        where = ['uid = %s']
        params: List[Any] = [uid]
        if with_meta and (people or topics or entities):
            where.append('(people && %s OR topics && %s OR entities && %s)')
            params.extend([people or [], topics or [], entities or []])
        if with_dates and dates_filter and len(dates_filter) == 2 and dates_filter[0] and dates_filter[1]:
            where.append('created_at BETWEEN %s AND %s')
            params.extend([int(dates_filter[0].timestamp()), int(dates_filter[1].timestamp())])
        q = f"""
        SELECT memory_id, people, topics, entities, dates
          FROM conversation_vectors
         WHERE {' AND '.join(where)}
         ORDER BY embedding <=> %s::vector
         LIMIT %s
        """
        params.extend([vector, top_k])
        with _get_pool().connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, params)
                return cur.fetchall()

    rows = run(with_meta=True, with_dates=True, top_k=1000)
    if not rows:
        # match the legacy retry that drops the metadata filter but keeps dates
        logger.warning('query_vectors_by_metadata retry without structured filters for uid=%s', uid)
        rows = run(with_meta=False, with_dates=True, top_k=20)
    if not rows:
        return []

    # re-rank by count of metadata overlaps (matches the Pinecone code path)
    scored: Dict[str, int] = defaultdict(int)
    for r in rows:
        cid = r['memory_id']
        for t in topics:
            if t in (r.get('topics') or []):
                scored[cid] += 1
        for e in entities:
            if e in (r.get('entities') or []):
                scored[cid] += 1
        for p in people:
            if p in (r.get('people') or []):
                scored[cid] += 1
    ordered = [r['memory_id'] for r in rows]
    ordered.sort(key=lambda x: scored.get(x, 0), reverse=True)
    return ordered[:limit] if len(ordered) > limit else ordered


def delete_vector(uid: str, conversation_id: str):
    vec_id = f'{uid}-{conversation_id}'
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM conversation_vectors WHERE id = %s', (vec_id,))
    logger.info('delete_vector %s', vec_id)


# ==========================================
# Memory vectors — namespace ns2
# ==========================================

MEMORIES_NAMESPACE = 'ns2'  # kept for backward-compat with any code that reads it


_MEM_UPSERT = """
INSERT INTO memory_vectors (id, uid, memory_id, embedding, category, created_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    embedding  = EXCLUDED.embedding,
    category   = EXCLUDED.category,
    created_at = EXCLUDED.created_at
"""


def upsert_memory_vector(uid: str, memory_id: str, content: str, category: str):
    vector = embeddings.embed_query(content)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_MEM_UPSERT, (f'{uid}-{memory_id}', uid, memory_id, vector, category, now_ts))
    logger.info('upsert_memory_vector %s', memory_id)
    return vector


def upsert_memory_vectors_batch(uid: str, items: List[dict]) -> int:
    if not items:
        return 0
    contents = [item['content'] for item in items]
    vectors = embeddings.embed_documents(contents)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    rows = [
        (f"{uid}-{item['memory_id']}", uid, item['memory_id'], vectors[i], item.get('category'), now_ts)
        for i, item in enumerate(items)
    ]
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_MEM_UPSERT, rows)
    logger.info('upsert_memory_vectors_batch count=%d', len(rows))
    return len(rows)


def find_similar_memories(uid: str, content: str, threshold: float = 0.85, limit: int = 5) -> List[dict]:
    vector = embeddings.embed_query(content)
    q = """
    SELECT memory_id, category, 1 - (embedding <=> %s::vector) AS similarity
      FROM memory_vectors
     WHERE uid = %s
     ORDER BY embedding <=> %s::vector
     LIMIT %s
    """
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (vector, uid, vector, limit))
            rows = cur.fetchall()
    return [
        {'memory_id': r['memory_id'], 'category': r['category'], 'score': float(r['similarity'])}
        for r in rows
        if float(r['similarity']) >= threshold
    ]


def check_memory_duplicate(uid: str, content: str, threshold: float = 0.85) -> dict | None:
    similar = find_similar_memories(uid, content, threshold=threshold, limit=1)
    if similar:
        logger.warning('Found duplicate memory: %s', similar[0])
        return similar[0]
    return None


def search_memories_by_vector(uid: str, query: str, limit: int = 10) -> List[str]:
    vector = embeddings.embed_query(query)
    q = """
    SELECT memory_id
      FROM memory_vectors
     WHERE uid = %s
     ORDER BY embedding <=> %s::vector
     LIMIT %s
    """
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (uid, vector, limit))
            return [r['memory_id'] for r in cur.fetchall()]


def delete_memory_vector(uid: str, memory_id: str):
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM memory_vectors WHERE id = %s', (f'{uid}-{memory_id}',))
    logger.info('delete_memory_vector %s', memory_id)


# ==========================================
# Screen activity vectors — namespace ns3
# ==========================================

SCREEN_ACTIVITY_NAMESPACE = 'ns3'

_SA_UPSERT = """
INSERT INTO screen_activity_vectors (id, uid, screenshot_id, embedding, timestamp, app_name)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    embedding = EXCLUDED.embedding,
    timestamp = EXCLUDED.timestamp,
    app_name  = EXCLUDED.app_name
"""


def upsert_screen_activity_vectors(uid: str, rows: List[dict]) -> int:
    prepared = []
    for row in rows:
        embedding = row.get('embedding')
        if not embedding:
            continue
        ts = row['timestamp']
        if isinstance(ts, str):
            ts_int = int(datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp())
        else:
            ts_int = int(ts)
        prepared.append(
            (
                f"{uid}-sa-{row['id']}",
                uid,
                str(row['id']),
                embedding,
                ts_int,
                row.get('appName') or row.get('app_name'),
            )
        )
    if not prepared:
        return 0
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_SA_UPSERT, prepared)
    logger.info('upsert_screen_activity_vectors uid=%s count=%d', uid, len(prepared))
    return len(prepared)


def search_screen_activity_vectors(
    uid: str,
    query_vector: List[float],
    start_date: int = None,
    end_date: int = None,
    app_filter: str = None,
    k: int = 10,
) -> List[dict]:
    where = ['uid = %s']
    params: List[Any] = [uid]
    if start_date and end_date:
        where.append('timestamp BETWEEN %s AND %s')
        params.extend([start_date, end_date])
    elif start_date:
        where.append('timestamp >= %s')
        params.append(start_date)
    elif end_date:
        where.append('timestamp <= %s')
        params.append(end_date)
    if app_filter:
        where.append('app_name = %s')
        params.append(app_filter)
    q = f"""
    SELECT screenshot_id, timestamp, app_name,
           1 - (embedding <=> %s::vector) AS score
      FROM screen_activity_vectors
     WHERE {' AND '.join(where)}
     ORDER BY embedding <=> %s::vector
     LIMIT %s
    """
    # Placeholder order: SELECT's vector, WHERE params, ORDER BY's vector, LIMIT.
    final_params = [query_vector, *params, query_vector, k]
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, final_params)
            rows = cur.fetchall()
    return [
        {
            'screenshot_id': r['screenshot_id'],
            'timestamp': r['timestamp'],
            'appName': r['app_name'],
            'score': float(r['score']),
        }
        for r in rows
    ]


def delete_screen_activity_vectors(uid: str, ids: List[int]):
    if not ids:
        return
    vec_ids = [f'{uid}-sa-{sid}' for sid in ids]
    with _get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM screen_activity_vectors WHERE id = ANY(%s)', (vec_ids,))
