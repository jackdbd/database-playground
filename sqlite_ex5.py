"""SQLite Exercise 5

Parse a file containing SQL statements for the SQLite dialect and execute them.
"""
import os
import logging
import sqlparse
import sqlalchemy as sa

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sqlalchemy.engine.base")
logger.setLevel(logging.DEBUG)


def main():
    file_path = os.path.abspath(os.path.join("sql", "rating.sql"))
    with open(file_path, "r") as f:
        sql_file = f.read()
        statements = sqlparse.parse(sql_file)

    engine = sa.create_engine("sqlite:///")
    with engine.connect() as conn:
        for statement in statements:
            sql = statement.value
            sql_clause = sa.text(sql)
            conn.execute(sql_clause)

    # We can execute a raw query defined here
    result = engine.execute("SELECT * from Rating")
    print(result.fetchall())

    # We can execute a raw query defined in a SQL file
    file_path = os.path.abspath(os.path.join("sql", "directors-query.sql"))
    with open(file_path, "r") as f, engine.connect() as conn:
        sql_file = f.read()
        statements = sqlparse.parse(sql_file)
        assert len(statements) == 1
        sql = statements[0].value
        sql_clause = sa.text(sql)
        result = conn.execute(sql_clause)
        print(result.fetchall())


if __name__ == "__main__":
    main()
