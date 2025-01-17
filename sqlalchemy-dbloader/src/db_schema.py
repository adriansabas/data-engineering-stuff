from sqlalchemy import Table, Column, Integer, Text, MetaData, Connection, text

class DatabaseSchema:
    """
    Manages database schema creation and maintenance.
    Disclaimer: altering existing tables should be done via sqlalchemy migrations done in alembic, 
        what you see here is not really a best practice

    Known missing functionalities:
    - alter dtatypes in existing columns
    - delete columns and tables

    This class is prone to error and probably needs bulletproffing.
    """
    
    def __init__(self, db_connection: Connection, db_metadata: MetaData):
        self.db_connection = db_connection
        self.new_metadata = MetaData()
        self.db_metadata = db_metadata
        self.tables = self._define_tables()

    def _define_tables(self) -> dict:
        """Defines the tables in the schema."""
        sample_table1 = Table(
            'sample_table1',
            self.new_metadata,
            Column('id', Integer, primary_key=True),
            Column('name', Text),
            Column('number', Integer),
            Column('other',Text)
        )
        sample_table2 = Table(
            'sample_table2',
            self.new_metadata,
            Column('id', Integer, primary_key=True),
            Column('name', Text),
            Column('number', Integer),
            Column('other',Text)
        )

        return {'sample_table1': sample_table1,'sample_table2': sample_table2 }


    def create_or_update_tables(self) -> None:
        for table_name, new_table in self.tables.items():
            self._sync_table(table_name, new_table)

    def _sync_table(self, table_name: str, new_table: Table):
        """Synchronizes schema from _define_tables method with the database."""
        if table_name in self.db_metadata.tables.keys():
            existing_table = self.db_metadata.tables[table_name]
            self._update_table(existing_table, new_table)
        else:
            new_table.create(self.db_connection)
            print(f"Created table '{table_name}'.")

    def _update_table(self, existing_table: Table, new_table: Table):
        """Adds new columns to an existing table"""
        existing_columns = {col.name for col in existing_table.columns}
        new_columns = {col.name: col for col in new_table.columns}

        for column_name, column in new_columns.items():
            if column_name not in existing_columns:
                self._add_column(existing_table, column)

        #TODO: handle datatype changes

    def _add_column(self, table: Table, column: Column):    
        alter_statement = f'ALTER TABLE {table.name} ADD COLUMN {column.name} {column.type}'
        self.db_connection.execute(text(alter_statement))
        print(f"Added column '{column.name}' to table '{table.name}'.")
