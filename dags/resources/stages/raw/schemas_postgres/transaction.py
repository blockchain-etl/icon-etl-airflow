from sqlalchemy import Column, String, Integer, Numeric, BigInteger
from resources.stages.raw.schemas_postgres.base import Base


class Transaction(Base):
    __tablename__ = "transactions"

    version = Column(String)
    from_address = Column(String)
    to_address = Column(String)
    value = Column(Numeric(38, 0, asdecimal=True))
    step_limit = Column(Numeric(38, 0, asdecimal=True))
    timestamp = Column(BigInteger)
    nid = Column(Integer)
    nonce = Column(Numeric(100, 0, asdecimal=False))
    hash = Column(String, primary_key=True)
    transaction_index = Column(BigInteger)
    block_hash = Column(String)
    block_number = Column(BigInteger)
    fee = Column(Numeric(38, 0, asdecimal=True))
    signature = Column(String)
    data_type = Column(String)
    data = Column(String)
