from sqlalchemy import create_engine


def create_in_memory_engine():
    return create_engine('sqlite:///:memory:')
