from sqlalchemy import create_engine, MetaData
import os

# Dotenv inline import because env's might be provided by k8s in future.
env_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), ".env")
if os.path.isfile(env_path):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)

class DatabaseConnection:
    """
    SQL Alchemy DatabaseConnection class that uses context management.
    """
    def __init__(self):
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    def __enter__(self):
        self.conn = self.engine.connect()
        self.transaction = self.conn.begin()
        return {
            'conn': self.conn,
            'metadata': self.metadata
        }

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                self.transaction.rollback()
            else:
                self.transaction.commit()
        finally:
            self.conn.close()
            self.engine.dispose()
        if exc_type:
            raise exc_val

    def _create_engine(self):
        """Creates and returns the SQLAlchemy engine."""
        uri = self._get_postgres_uri()
        return create_engine(uri)

    @staticmethod
    def _get_postgres_uri():
        """Constructs the PostgreSQL connection URI from environment variables."""
        required_env_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
        host = os.environ["DB_HOST"]
        port = os.environ["DB_PORT"]
        db_name = os.environ["DB_NAME"]
        user = os.environ["DB_USER"]
        password = os.environ["DB_PASSWORD"]
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}"