from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Pe1T1(Base):
    __tablename__ = 'p1e_t1'

    time = Column(Integer(), primary_key=True)
    imported_t1 = Column('imported T1', Float(), nullable=False, default=0)
    exported_t1 = Column('exported T1', Float(), nullable=False, default=0)


class Pe1T2(Base):
    __tablename__ = 'p1e_t2'

    time = Column(Integer(), primary_key=True)
    imported_t1 = Column('imported T2', Float(), nullable=False, default=0)
    exported_t1 = Column('exported T2', Float(), nullable=False, default=0)


class SmartThingsGround(Base):
    __tablename__ = 'smartthings_ground'
    loc = Column('loc', String(), nullable=True)
    level = Column('level', String(), nullable=True)
    name = Column('name', String(), nullable=True)
    time = Column('time', Integer(), nullable=True, primary_key=True)
    capability = Column('capability', String(), nullable=True)
    attribute = Column('attribute', String(), nullable=True)
    value = Column('value', String(), nullable=True)
    unit = Column('unit', String(), nullable=True)
