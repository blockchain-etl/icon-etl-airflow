from sqlalchemy import Column, String, Integer
from resources.stages.raw.schemas_postgres.base import Base


class Log(Base):
    __tablename__ = "logs"

    log_index = Column(Integer, primary_key=True)
    transaction_hash = Column(String, primary_key=True)
    transaction_index = Column(Integer, primary_key=True)
    block_hash = Column(String)
    block_number = Column(Integer)
    address = Column(String)
    data = Column(String)
    indexed = Column(String)