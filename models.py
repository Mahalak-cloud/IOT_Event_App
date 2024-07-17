from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine, QueuePool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    device_id = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    event_type = Column(String, nullable=False)
    event_data = Column(Float, nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'device_id': self.device_id,
            'timestamp': self.timestamp,
            'event_type': self.event_type,
            'event_data': self.event_data
        }

DATABASE_URL = "postgresql://event__user:1234@http://127.0.0.1:62128/browser/eventdb"
engine = create_engine(DATABASE_URL, poolclass=QueuePool, pool_size=20, max_overflow=0)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
