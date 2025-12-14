from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# fetcher.pyが書き込むDBファイルを指定
# Dockerコンテナ内でのパスになる
DATABASE_URL = "sqlite:///./data/cmma.db"

# SQLALCHEMY_DATABASE_URL = "postgresql://user:password@postgresserver/db"

engine = create_engine(
    DATABASE_URL, 
    # SQLiteは単一ファイルなので、複数のスレッドからの書き込み要求を捌くために必要
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
