"""Functions and classes for flask/SQLAlchemy models."""
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, BigInteger


class FlaskPumpWoodBaseModel(DeclarativeBase):
    """Flask Sqlalchemy Database Connection.

    - adds a id column for all models
    """

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    '''All tables must have primary id'''
