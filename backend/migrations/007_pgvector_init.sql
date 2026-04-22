-- Self-host schema: replaces Pinecone with pgvector.
-- Embedding dim 1536 (Stella on Ollama) — fits pgvector HNSW's 2000-dim cap.
-- Apply with: psql "$POSTGRES_URL" -f migrations/007_pgvector_init.sql

CREATE EXTENSION IF NOT EXISTS vector;

-- ns1 in the old Pinecone layout
CREATE TABLE IF NOT EXISTS conversation_vectors (
    id          TEXT PRIMARY KEY,
    uid         TEXT NOT NULL,
    memory_id   TEXT NOT NULL,
    embedding   VECTOR(1536) NOT NULL,
    created_at  BIGINT NOT NULL,
    people      TEXT[] NOT NULL DEFAULT '{}',
    topics      TEXT[] NOT NULL DEFAULT '{}',
    entities    TEXT[] NOT NULL DEFAULT '{}',
    dates       TEXT[] NOT NULL DEFAULT '{}',
    metadata    JSONB  NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_conv_uid          ON conversation_vectors (uid);
CREATE INDEX IF NOT EXISTS idx_conv_uid_created  ON conversation_vectors (uid, created_at);
CREATE INDEX IF NOT EXISTS idx_conv_people       ON conversation_vectors USING GIN (people);
CREATE INDEX IF NOT EXISTS idx_conv_topics       ON conversation_vectors USING GIN (topics);
CREATE INDEX IF NOT EXISTS idx_conv_entities     ON conversation_vectors USING GIN (entities);
CREATE INDEX IF NOT EXISTS idx_conv_embedding_hnsw
    ON conversation_vectors USING hnsw (embedding vector_cosine_ops);

-- ns2 in the old Pinecone layout — user memories/facts
CREATE TABLE IF NOT EXISTS memory_vectors (
    id          TEXT PRIMARY KEY,
    uid         TEXT NOT NULL,
    memory_id   TEXT NOT NULL,
    embedding   VECTOR(1536) NOT NULL,
    category    TEXT,
    created_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mem_uid ON memory_vectors (uid);
CREATE INDEX IF NOT EXISTS idx_mem_embedding_hnsw
    ON memory_vectors USING hnsw (embedding vector_cosine_ops);

-- ns3 in the old Pinecone layout — desktop screen activity
CREATE TABLE IF NOT EXISTS screen_activity_vectors (
    id              TEXT PRIMARY KEY,
    uid             TEXT NOT NULL,
    screenshot_id   TEXT NOT NULL,
    embedding       VECTOR(1536) NOT NULL,
    timestamp       BIGINT NOT NULL,
    app_name        TEXT
);
CREATE INDEX IF NOT EXISTS idx_sa_uid_ts ON screen_activity_vectors (uid, timestamp);
CREATE INDEX IF NOT EXISTS idx_sa_embedding_hnsw
    ON screen_activity_vectors USING hnsw (embedding vector_cosine_ops);
