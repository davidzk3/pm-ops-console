BEGIN;

CREATE TABLE IF NOT EXISTS core.users (
  user_id      text PRIMARY KEY,
  email        text UNIQUE NOT NULL,
  password_hash text NOT NULL,
  role         text NOT NULL DEFAULT 'operator',
  is_active    boolean NOT NULL DEFAULT true,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_core_users_email ON core.users (email);

COMMIT;