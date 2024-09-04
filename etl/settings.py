from datetime import datetime, timezone
from pydantic_settings import BaseSettings



class Settings(BaseSettings):
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str

    elasticsearch_host: str
    elasticsearch_port: int

    redis_host: str
    redis_port: int

    filmwork_index_name: str = "movies"
    genres_index_name: str = "genres"
    persons_index_name: str = "persons"
    state_file_path: str = "sync_state.json"
    default_sync_time: str = datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat()
    default_sleep_time: int = 5

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()