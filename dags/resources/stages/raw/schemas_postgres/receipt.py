from sqlalchemy import Column, String, Integer, Numeric
from resources.stages.raw.schemas_postgres.base import Base


class Receipt(Base):
    __tablename__ = "receipts"

    transaction_hash = Column(String, primary_key=True)
    transaction_index = Column(Integer, primary_key=True)
    block_hash = Column(String)
    block_number = Column(Integer)
    cumulative_step_used = Column(Numeric(38, 0, asdecimal=True))
    step_used = Column(Numeric(38, 0, asdecimal=True))
    step_price = Column(Numeric(38, 0, asdecimal=True))
    score_address = Column(String)
    status = Column(String)