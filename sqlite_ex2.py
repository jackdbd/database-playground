"""SQLite Exercise 2

Connect to a SQLite in-memory database, create a table with SQLAlchemy Core SQL
Expression Language, insert some records and fetch all results.
"""
import logging
import sqlalchemy as sa

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sqlalchemy.engine.base")
logger.setLevel(logging.DEBUG)


def main():
    engine = sa.create_engine("sqlite:///")
    conn = engine.connect()

    # MetaData is a container object that keeps together many different
    # features (tables, columns, constraints, types) of the database being
    # described.
    # It's an idea that Mike Bayer took from Martin Fowler's Patterns of
    # Enterprise Architecture
    # http://docs.sqlalchemy.org/en/latest/core/metadata.html
    # https://www.martinfowler.com/eaaCatalog/metadataMapping.html
    metadata = sa.MetaData()

    # Creating a table with SQLAlchemy Core's SQL Expression Language resembles
    # very much crreating a table schema with the database's DDL.
    table = sa.Table(
        "people",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(25), nullable=False),
        sa.Column("age", sa.Integer, nullable=False),
        sa.Column("pythonista", sa.Boolean, default=False),
    )
    logger.debug(table.columns)
    logger.debug(metadata.tables)

    # Store in the DB the schema we have just defined
    metadata.create_all(engine)

    # We can use SQLAlchemy as a query builder: python objects represent SQL
    # clauses
    clause = table.insert()

    # We execute the clause by sending it through the connection pool
    conn.execute(clause, name="Guido", age=42, pythonista=True)

    # We can reuse the SQL clause we defined previously and insert a new person
    # When we use clauses built with SQLAlchemy we can use default parameters.
    conn.execute(clause, name="John", age=20)

    # We can also use raw SQL.
    # When we use raw SQL we cannot use default parameters (i.e. here we need
    # to specify 'pythonista').
    data = (
        {"name": "Alex", "age": 25, "pythonista": False},
        {"name": "Sarah", "age": 28, "pythonista": True},
        {"name": "Bob", "age": 35, "pythonista": True},
    )
    for line in data:
        clause = sa.text(
            """
            INSERT INTO people(name, age, pythonista) 
            VALUES(:name, :age, :pythonista)
            """
        )
        conn.execute(clause, **line)

    clause = table.select()
    # We can print/log the clause to see the generated SQL
    logger.debug(clause)

    # if we manipulate the clause we can see that the generated SQL changes.
    # This is very handy when building complex queries.
    clause = clause.order_by(table.columns.age)
    logger.debug(clause)

    # Execute the SELECT clause and fetch all the results.
    result = conn.execute(clause)
    print(f"By age: {result.fetchall()}")

    # Of course we can also use raw SQL to fetch all the results
    result = conn.execute("SELECT * FROM people ORDER BY name")
    print(f"By name: {result.fetchall()}")


if __name__ == "__main__":
    main()
