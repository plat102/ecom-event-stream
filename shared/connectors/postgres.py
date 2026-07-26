"""JDBC connection helpers for Spark → PostgreSQL analytics warehouse."""


def jdbc_url(settings) -> str:
    return (
        f"jdbc:postgresql://{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        "?stringtype=unspecified"  # required for JSONB write, avoids payload VARCHAR vs jsonb error
    )


def jdbc_properties(settings) -> dict:
    return {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
