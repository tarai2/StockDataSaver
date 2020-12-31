import sys,os
import datetime,time
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Column
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, FLOAT, INTEGER, TEXT, JSON, VARCHAR, BIGINT
from ..conf.mysqlconf import URL


BASE = declarative_base()
DBNAME = "FinancialData"


class Daily(BASE):
    __tablename__ = "Daily"
    timestamp = Column(DATETIME(fsp=3), nullable=False, primary_key=True)
    ticker = Column(VARCHAR(50), nullable=False, primary_key=True)
    open = Column(FLOAT())
    high = Column(FLOAT())
    low = Column(FLOAT())
    close = Column(FLOAT())
    volume = Column(FLOAT())

class Intraday(BASE):
    __tablename__ = "Intraday"
    timestamp = Column(DATETIME(fsp=3), nullable=False, primary_key=True)
    ticker = Column(VARCHAR(50), nullable=False, primary_key=True)
    open = Column(FLOAT())
    high = Column(FLOAT())
    low = Column(FLOAT())
    close = Column(FLOAT())
    volume = Column(FLOAT())

class TickerID(BASE):
    __tablename__ = "TickerID"
    ticker = Column(VARCHAR(50), nullable=False, primary_key=True)
    category1 = Column(VARCHAR(15))
    category2 = Column(VARCHAR(15))
    country = Column(VARCHAR(15))
    description = Column(TEXT())


def create():
    # Databaseの作成
    try:
        engine = create_engine(URL, encoding='utf-8')
        con = engine.connect()
        con.execute(f"create database {DBNAME}")
    except:
        pass
    finally:
        try:
            con.close()
            engine.dispose()
        except:
            pass

    # tableの作成    
    metadata = MetaData()
    engine = create_engine(URL+DBNAME, encoding='utf-8')
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=True,
                                          expire_on_commit=False,
                                          bind=engine))
    try:
        BASE.metadata.create_all(bind=engine)
    except Exception as e:
        print(e)
    finally:
        session.close()
        engine.dispose()


