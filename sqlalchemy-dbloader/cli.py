import click
import os
from sqlalchemy import text, MetaData, Connection
from src.db_conn import DatabaseConnection
from src.db_schema import DatabaseSchema
from src.data_loader import DataLoader


def create_schema(conn: Connection, metadata: MetaData) -> None:
    """
    Creates schema and adds new columns to an existing tables
    """
    schema = DatabaseSchema(conn,metadata)
    schema.create_or_update_tables()

def load_csv(path: str, table_name: str, conn: Connection, metadata: MetaData) -> None:
    loader = DataLoader(conn, metadata)
    loader.load_csv(path,table_name)


@click.command()
@click.option(
    "--operation",
    type=click.Choice(["create-schema", "load-csv"]),
    required=True,
    help="pick what operation you need: create schema or load-csv"
)
@click.option(
    "--table_name",
    required=False,
    help="required in load-csv, name of table to load data",
)
@click.option(
    "--file",
    required=False,
    help="rquired in load-csv, name of file with data",
)
def main(operation, table_name, file) -> None:
     with DatabaseConnection() as db_conn:
        """
        Dirty hack to get connection object and metadata.
        Need to find out better way to do it.
        Maybe version with engine and metadata as class variables was better idea.
        """
        conn = db_conn['conn']
        metadata = db_conn['metadata']

        match operation:
            case "create-schema":
                create_schema(conn,metadata)
            case "load-csv":
                filepath = os.path.join(os.environ['STORAGE_PATH'], file)
                load_csv(filepath,table_name,conn,metadata)
            case _:
                print("Operation not picked.")

if __name__ == "__main__":
    main()


