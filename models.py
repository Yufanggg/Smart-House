from sqlalchemy import Column, Integer, Float
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
