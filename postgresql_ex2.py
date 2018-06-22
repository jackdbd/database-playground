"""PostgreSQL Exercise 2

Parse a file containing PL/SQL statements (PostgreSQL) and execute them on a
test database.

The testing.postgresql python module creates a test database and destroys it
when exiting the context manager.
"""
import os
import sqlparse
import logging
import testing.postgresql
import sqlalchemy as sa

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sqlalchemy.engine.base")
logger.setLevel(logging.DEBUG)


def main():
    with testing.postgresql.Postgresql() as postgresql:
        engine = sa.create_engine(postgresql.url())
        conn = engine.connect()
        # we need this otherwise the CREATE DATABASE statement fails
        conn.execution_options(isolation_level="AUTOCOMMIT")

        file_path = os.path.abspath(os.path.join("sql", "clubdata.sql"))
        with open(file_path, "r") as f:
            sql_file = f.read()
            statements = sqlparse.parse(sql_file)
            for statement in statements:
                sql = statement.value
                if sql == "\n":
                    logger.debug("Skip empty line")
                else:
                    sql_clause = sa.text(sql)
                    conn.execute(sql_clause)

            results = conn.execute("SELECT * from members")
            rows = results.fetchall()
            print(rows)
            assert len(rows) == 31


if __name__ == "__main__":
    main()
