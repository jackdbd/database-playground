"""SQLite Exercise 1

Connect to a SQLite in-memory database and execute a simple query.
"""
import logging
import sqlalchemy as sa

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sqlalchemy.engine.base")
logger.setLevel(logging.INFO)


def main():
    engine = sa.create_engine("sqlite:///")
    conn = engine.connect()
    result = conn.execute("SELECT 1+2")
    # return one entire row
    # print(result.fetchone())
    # return only the numeric result
    logger.info(result.scalar())


if __name__ == "__main__":
    main()
