import argparse
import mysql.connector
import time

class MigrateService:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        table: str,
        alter: list[str],
        chunk_size: int,
        swap_tables: bool,
        drop_old_table: bool,
        drop_triggers: bool,
        drop_audit_table: bool,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table = table
        self.alter = alter
        self.chunk_size = chunk_size
        self.swap_tables = swap_tables
        self.drop_old_table = drop_old_table
        self.drop_triggers = drop_triggers
        self.drop_audit_table = drop_audit_table
        self.cnx = mysql.connector.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            autocommit=True,
        )
        self.cursor = self.cnx.cursor()
        self.audit_table_name = f'_{self.table}_audit'
        self.shadow_table_name = f"_{self.table}_new"

    def execute(self) -> None:
        try:
            self._create_audit_table()
            self._add_triggers()
            self._create_shadow_table()
            self._copy_data()
            self._replay_audit_logs()
            if self.swap_tables:
                self._swap_tables()
            if self.drop_triggers:
                self._drop_triggers()
            if self.swap_tables and self.drop_old_table:
                self._drop_old_table()
            if self.audit_table_name:
                self._drop_audit_table()
            print("Migration completed successfully.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.cursor.close()
            self.cnx.close()

    def _create_audit_table(self):
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.audit_table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                action VARCHAR(10),
                original_id INT,
                row_data JSON,
                action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print(f"Audit table {self.audit_table_name} created.")

    def _add_triggers(self):
        self.cursor.execute(f"""
            CREATE TRIGGER {self.table}_insert AFTER INSERT ON {self.table}
            FOR EACH ROW
            INSERT INTO {self.audit_table_name} (action, original_id, row_data) 
            VALUES ('INSERT', NEW.id, JSON_OBJECT('new', ROW_TO_JSON(NEW)));
        """)
        
        self.cursor.execute(f"""
            CREATE TRIGGER {self.table}_update AFTER UPDATE ON {self.table}
            FOR EACH ROW
            INSERT INTO {self.audit_table_name} (action, original_id, row_data) 
            VALUES ('UPDATE', OLD.id, JSON_OBJECT('old', ROW_TO_JSON(OLD), 'new', ROW_TO_JSON(NEW)));
        """)
        
        self.cursor.execute(f"""
            CREATE TRIGGER {self.table}_delete AFTER DELETE ON {self.table}
            FOR EACH ROW
            INSERT INTO {self.audit_table_name} (action, original_id, row_data) 
            VALUES ('DELETE', OLD.id, JSON_OBJECT('old', ROW_TO_JSON(OLD)));
        """)
        
        print(f"Triggers for table {self.table} created.")

    def _create_shadow_table(self):
        self.cursor.execute(f"CREATE TABLE {self.shadow_table_name} LIKE {self.table}")
        for alter_command in self.alter:
            self.cursor.execute(f"ALTER TABLE {self.shadow_table_name} {alter_command}")
        print(f"Shadow table {self.shadow_table_name} created and altered.")

    def _copy_data(self):
        offset = 0
        while True:
            self.cursor.execute(f"INSERT INTO {self.shadow_table_name} SELECT * FROM {self.table} LIMIT {self.chunk_size} OFFSET {offset}")
            rows = self.cursor.rowcount
            if rows == 0:
                break
            offset += self.chunk_size
            print(f"Copied {offset} rows...")
            # time.sleep()

    def _replay_audit_logs(self):
        self.cursor.execute(f"SELECT * FROM {self.audit_table_name}")
        audit_logs = self.cursor.fetchall()
        
        for log in audit_logs:
            action, original_id, row_data = log[1], log[2], log[3]
            if action == 'INSERT':
                self.cursor.execute(f"INSERT INTO {self.shadow_table_name} SELECT * FROM {self.table} WHERE id = {original_id}")
            elif action == 'UPDATE':
                old_row, new_row = row_data['old'], row_data['new']
                self.cursor.execute(f"UPDATE {self.shadow_table_name} SET ... WHERE id = {original_id}")
            elif action == 'DELETE':
                self.cursor.execute(f"DELETE FROM {self.shadow_table_name} WHERE id = {original_id}")
            self.cursor.execute(f"DELETE FROM {self.audit_table_name} WHERE id = {log[0]}")
        
        print(f"Audit logs replayed and applied to {self.shadow_table_name}.")

    def _swap_tables(self):
        self.cursor.execute(f"RENAME TABLE {self.table} TO {self.table}_old, {self.shadow_table_name} TO {self.table}")
        print(f"Tables swapped: {self.table} with {self.shadow_table_name}.")

    def _drop_old_table(self):
        self.cursor.execute(f"DROP TABLE {self.table}_old")
        print(f"Old table {self.table}_old dropped.")

    def _drop_triggers(self):
        self.cursor.execute(f"DROP TRIGGER IF EXISTS {self.table}_insert")
        self.cursor.execute(f"DROP TRIGGER IF EXISTS {self.table}_update")
        self.cursor.execute(f"DROP TRIGGER IF EXISTS {self.table}_delete")
        print(f"Triggers for table {self.table} dropped.")
    
    def _drop_audit_table(self):
        self.cursor.execute(f"DROP TABLE {self.audit_table_name}")
        print(f"Audit table {self.audit_table_name} dropped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Admitad Online Schema Change Tool')
    parser.add_argument('--host', required=True, help='MySQL host')
    parser.add_argument('--port', type=int, help='MySQL port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--alter', required=True, help='Alter command to apply to the new table')
    parser.add_argument('--user', required=True, help='MySQL user')
    parser.add_argument('--password', required=True, help='MySQL password')
    parser.add_argument('--chunk-size', required=True, dest='chunk_size', help='Copy chunk size')
    parser.add_argument('--swap-tables', action='store_true', help='Swap tables after migration')
    parser.add_argument('--drop-old-table', action='store_true', help='Drop old table after swapping')
    parser.add_argument('--drop-triggers', action='store_true', help='Drop triggers after migration')
    parser.add_argument('--drop-audit-table', action='store_true', help='Drop audit table after migration')
    
    args = parser.parse_args()
    
    service = MigrateService(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database=args.database,
        table=args.table,
        alter=args.alter.split(';'),
        chunk_size=int(args.chunk_size),
        swap_tables=args.swap_tables,
        drop_old_table=args.drop_old_table,
        drop_triggers=args.drop_triggers,
        drop_audit_table=args.drop_audit_table,
    )
    
    service.execute()
