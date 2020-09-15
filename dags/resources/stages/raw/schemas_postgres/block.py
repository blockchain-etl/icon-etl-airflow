from sqlalchemy import Column, String, BigInteger
from resources.stages.raw.schemas_postgres.base import Base


class Block(Base):
    __tablename__ = "blocks"

    number = Column(BigInteger, primary_key=True)
    hash = Column(String)
    parent_hash = Column(String)
    merkle_root_hash = Column(String)
    timestamp = Column(BigInteger)
    version = Column(String)
    peer_id = Column(String)
    signature = Column(String)
    next_leader = Column(String)
