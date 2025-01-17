import pandas as pd

class DataLoader:
    """
        Validates and loads data to database table
        TODO:
        - upsert
        - batching
        - parquet handling
    """
    
    def __init__(self, db_connection: 'DatabaseConnection.conn', schema: 'DatabaseSchema.schema'):
        self.db_connection = db_connection
        self.schema = schema

    def load_csv(self, csv_path: str, table_name: str) -> None:
        """Loads a CSV file into the specified database table."""
        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' does not exist in the schema.")
        
        # Load CSV and check schema
        csv_data = pd.read_csv(csv_path)
        table = self.schema.tables[table_name]
        self._validate_csv_schema(csv_data, table)

        # Insert data into the table
        self._insert_data(csv_data, table)

    def _validate_csv_schema(self, csv_data: pd.DataFrame, table):
        """Validates that the CSV schema matches the database table schema."""
        table_columns = [col.name for col in table.columns]
        csv_columns = [csv_data.columns]
        if csv_columns in (table_columns):
            raise ValueError(f"CSV columns {csv_columns} do not match table columns {table_columns}.")

    def _insert_data(self, csv_data: pd.DataFrame, table):
        #here needs to be upsert not insert
        csv_data.to_sql(table.name,self.db_connection, if_exists='append', index=False)