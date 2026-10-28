-- Resilient Multi-Peer Storage Schema (From Gate Specification)

CREATE TABLE IF NOT EXISTS users (
    user_id       SERIAL PRIMARY KEY,
    email         VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    file_id            VARCHAR(64) PRIMARY KEY,
    owner_id           INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    filename           VARCHAR(255) NOT NULL,
    current_version_id VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS file_versions (
    version_id     VARCHAR(64) PRIMARY KEY,
    file_id        VARCHAR(64) NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    version_number INT NOT NULL,
    size           BIGINT NOT NULL,
    file_hash      VARCHAR(64) NOT NULL,
    status         VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS peers (
    peer_id        VARCHAR(32) PRIMARY KEY,
    ip_port        VARCHAR(64) NOT NULL,
    status         VARCHAR(16) NOT NULL,
    last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id     VARCHAR(64) PRIMARY KEY,
    version_id   VARCHAR(64) NOT NULL REFERENCES file_versions(version_id) ON DELETE CASCADE,
    sequence_no  INT NOT NULL,
    size         BIGINT NOT NULL,
    sha256       VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS chunk_placement (
    placement_id  SERIAL PRIMARY KEY,
    chunk_id      VARCHAR(64) NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    peer_id       VARCHAR(32) NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    replica_state VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_events (
    event_id    SERIAL PRIMARY KEY,
    version_id  VARCHAR(64),
    chunk_id    VARCHAR(64),
    peer_id     VARCHAR(32),
    event_type  VARCHAR(64) NOT NULL,
    event_time  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    result      TEXT NOT NULL
);

-- Insert initial peer records
INSERT INTO peers (peer_id, ip_port, status) VALUES
  ('A', '127.0.0.1:5001', 'ONLINE'),
  ('B', '127.0.0.1:5002', 'ONLINE'),
  ('C', '127.0.0.1:5003', 'ONLINE')
ON CONFLICT (peer_id) DO NOTHING;

-- Insert default admin user
INSERT INTO users (user_id, email, password_hash) VALUES
  (1, 'admin@resilient.local', 'scrypt_placeholder_hash')
ON CONFLICT (user_id) DO NOTHING;
