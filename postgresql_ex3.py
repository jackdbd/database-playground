"""PostgreSQL Exercise 3

Connect to a DB instance on Amazon RDS.
"""
import os
import logging
import sqlalchemy as sa
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)
DB_URL = f"postgres://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_INSTANCE_ENDPOINT']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sqlalchemy.engine.base")
logger.setLevel(logging.DEBUG)


def main():
    engine = sa.create_engine(DB_URL)
    with engine.connect() as conn:
        result = conn.execute("SELECT 1+2")
        print(result.scalar())


if __name__ == "__main__":
    main()
