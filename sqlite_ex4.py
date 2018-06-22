"""SQLite Exercise 4

Connect to a SQLite in-memory database, create tables with SQLAlchemy ORM,
insert some records, establish a relationship and fetch all results.
"""
import logging
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sqlalchemy.engine.base")
logger.setLevel(logging.DEBUG)


def main():
    engine = sa.create_engine("sqlite:///")
    # conn = engine.connect()

    metadata = sa.MetaData()

    # Construct a base class for declarative class definitions.
    Base = declarative_base(metadata=metadata)

    class Person(Base):
        __tablename__ = "people"
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String(25), nullable=False)
        age = sa.Column(sa.Integer, nullable=False)
        pythonista = sa.Column(sa.Boolean, default=False)

    logger.debug(Person.__table__.columns)
    logger.debug(Person.__mapper__)

    # Store in the DB the schema we have just defined
    metadata.create_all(engine)

    Session = orm.sessionmaker(bind=engine)
    session = Session()

    person = Person(name="Guido", age=42, pythonista=True)
    session.add(person)
    person = Person(name="Alex", age=20)
    session.add(person)

    # The 'people' table has been added to the metadata.
    logger.debug(metadata.tables)

    # The person objects (Guido and Alex) have been added to the session, but
    # not yet stored in the database.
    logger.info(session.new._members)

    # Write the objects to the database (look at the generated SQL which
    # inserts the Person object in the database).
    session.flush()

    query = session.query(Person)

    # retrieve all Person objects from the database
    for person in query.all():
        print(person)

    # Let's create a new table and establish a relationship between the two.

    class Address(Base):
        __tablename__ = "addresses"
        id = sa.Column(sa.Integer(), primary_key=True)
        street = sa.Column(sa.String(25), nullable=False)
        person_id = sa.Column(sa.Integer, sa.ForeignKey(Person.id))

    metadata.create_all(engine)

    # Now we have created the two tables 'people' and 'addresses' in the
    # database, we have defined a relationship between 'people' and 'addresses'
    # at the DATABASE level.

    # We need to define this relationship at the OBJECT level as well.
    # This command works like this: SQLAlchemy looks at our tables 'people' and
    # 'addresses', sees that there is a foreign key constraint in 'addresses'
    # towards 'people', and it figures out how to establish a relation between
    # Address and Person at the OBJECT level.
    Address.person = orm.relation(Person, backref="addresses")

    person = Person(name="Mike", age=33)
    address = Address(person=person, street="Via Roma")
    session.add(person)

    # Write this new person and his address to the database
    session.flush()

    # person.addresses is a pointer to the object stored in addresses.street
    print(person.name, person.addresses)
    print(address.street)


if __name__ == "__main__":
    main()
