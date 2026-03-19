import os
import uuid
import psycopg
from apps.api.auth import hash_password

def get_db_dsn():
    return os.getenv("DATABASE_URL") or "postgresql://pmops:pmops@localhost:5432/pmops"

def main():
    email = os.getenv("OPERATOR_EMAIL", "operator@local")
    password = os.getenv("OPERATOR_PASSWORD", "operator123")

    user_id = str(uuid.uuid4())
    pw_hash = hash_password(password)

    q = """
    INSERT INTO core.users (user_id, email, password_hash, role)
    VALUES (%s, %s, %s, 'operator')
    ON CONFLICT (email) DO UPDATE
    SET password_hash = EXCLUDED.password_hash,
        updated_at = now(),
        is_active = true;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (user_id, email.lower().strip(), pw_hash))
        conn.commit()

    print("OK created or updated operator:", email)

if __name__ == "__main__":
    main()