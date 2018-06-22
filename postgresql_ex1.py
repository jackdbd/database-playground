"""PostgreSQL Exercise 1

Connect to a PostgreSQL test database (it's NOT an in-memory database), create
2 tables with SQLAlchemy Core SQL Expression Language, establish a relationship
between the 2 tables, populate the tables and fetch all results.

The testing.postgresql python module creates a test database and destroys it
when exiting the context manager.
"""
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
        conn.execution_options(isolation_level="AUTOCOMMIT")

        metadata = sa.MetaData()

        table_people = sa.Table(
            "people",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.String(25), nullable=False),
            sa.Column("age", sa.Integer, nullable=False),
            sa.Column("pythonista", sa.Boolean, default=False),
        )

        table_addresses = sa.Table(
            "addresses",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("street", sa.String(25), nullable=False),
            # Use a ForeignKey constraint to establish a relationship
            sa.Column(
                "person_id", sa.Integer, sa.ForeignKey("people.id"), nullable=False
            ),
        )

        metadata.create_all(engine)

        clause = table_people.insert()
        conn.execute(clause, name="Guido", age=42, pythonista=True)

        clause = table_addresses.insert()
        conn.execute(clause, street="Python Road 123", person_id=1)

        clause = table_addresses.select()
        result = conn.execute(clause)
        print(result.fetchall())


if __name__ == "__main__":
    main()
