import argparse
import json
import logging
import time

import mysql.connector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()


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
        self.temp_table = 'temp_copied_ids'
        self.s_time = time.perf_counter()

    def _get_table_columns(self) -> list[str]:
        self.cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{self.table}' AND TABLE_SCHEMA = '{self.database}'
        """)
        return [col[0] for col in self.cursor.fetchall()]

    def _create_audit_table(self):
        logger.info('Creating audit table...')
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.audit_table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                action VARCHAR(10),
                original_id INT,
                row_data JSON,
                action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info(f"Audit table {self.audit_table_name} created.")

    def _add_triggers(self):
        logger.info('Creating triggers...')
        
        columns = self._get_table_columns()
        set_columns = ', '.join([f"'{col}', NEW.{col}" for col in columns])
        old_set_columns = ', '.join([f"'{col}', OLD.{col}" for col in columns]) 

        self.cursor.execute(f"""
            CREATE TRIGGER {self.table}_insert AFTER INSERT ON {self.table}
            FOR EACH ROW
            INSERT INTO {self.audit_table_name} (action, original_id, row_data) 
            VALUES (
                'INSERT',
                NEW.id,
                JSON_OBJECT({set_columns})
            );
        """)

        self.cursor.execute(f"""
            CREATE TRIGGER {self.table}_update AFTER UPDATE ON {self.table}
            FOR EACH ROW
            INSERT INTO {self.audit_table_name} (action, original_id, row_data) 
            VALUES (
                'UPDATE',
                OLD.id,
                JSON_OBJECT({set_columns})
            );
        """)

        self.cursor.execute(f"""
            CREATE TRIGGER {self.table}_delete AFTER DELETE ON {self.table}
            FOR EACH ROW
            INSERT INTO {self.audit_table_name} (action, original_id, row_data) 
            VALUES (
                'DELETE',
                OLD.id,
                JSON_OBJECT('old', JSON_OBJECT({old_set_columns}))
            );
        """)
        
        logger.info(f"Triggers for table {self.table} created.")

    def _create_shadow_table(self):
        logger.info('Creating shadow table...')
        self.cursor.execute(f"CREATE TABLE {self.shadow_table_name} LIKE {self.table}")
        for alter_command in self.alter:
            self.cursor.execute(f"ALTER TABLE {self.shadow_table_name} {alter_command}")
        logger.info(f"Shadow table {self.shadow_table_name} created and altered.")

    def _copy_data(self):
        logger.info('Copying data to shadow table...')
        
        self.cursor.execute(f"CREATE TEMPORARY TABLE {self.temp_table} (id INT unsigned PRIMARY KEY)")
        
        self.cursor.execute(f"SELECT MIN(id), MAX(id) FROM {self.table}")
        min_id, max_id = self.cursor.fetchone()
        
        if min_id is None or max_id is None:
            logger.info("No data found in the source table.")
            return

        logger.info(f"Copying data from ID {min_id} to {max_id}")
        
        current_id = min_id
        while current_id <= max_id:
            end_id = min(current_id + self.chunk_size - 1, max_id)
            self.cursor.execute(f"""
                INSERT INTO {self.shadow_table_name} 
                SELECT * FROM {self.table}
                WHERE id BETWEEN {current_id} AND {end_id}
            """)
            self.cursor.execute(f"""
                INSERT IGNORE INTO {self.temp_table} 
                SELECT id FROM {self.table}
                WHERE id BETWEEN {current_id} AND {end_id}
            """)
            
            current_id = end_id + 1
            
            logger.info(f"Copied rows from ID {current_id - self.chunk_size} to {end_id}...")

        logger.info('Data copied successfully to the shadow table.')

    def _replay_audit_logs(self):
        logger.info('Replaying audit logs...')

        self.cursor.execute(f"SELECT * FROM {self.audit_table_name}")
        audit_logs = self.cursor.fetchall()
        columns = self._get_table_columns()
        
        
        for log in audit_logs:
            action, original_id, row_data = log[1], log[2], json.loads(log[3])
            if action == 'INSERT':
                self.cursor.execute(f"INSERT INTO {self.shadow_table_name} SELECT * FROM {self.table} WHERE id = {original_id}")
            elif action == 'UPDATE':
                columns_str = ', '.join(columns)
                new_values_str = ', '.join([f"'{row_data.get(col, '')}'" for col in columns])
                update_str = ', '.join([f"{col} = VALUES({col})" for col in columns if col != 'id'])
                
                insert_query = f"""
                    INSERT INTO {self.shadow_table_name} ({columns_str})
                    VALUES ({new_values_str})
                    ON DUPLICATE KEY UPDATE {update_str}
                """
                self.cursor.execute(insert_query)
            elif action == 'DELETE':
                self.cursor.execute(f"DELETE FROM {self.shadow_table_name} WHERE id = {original_id}")
            self.cursor.execute(f"DELETE FROM {self.audit_table_name} WHERE id = {log[0]}")
        
        logger.info(f"Audit logs replayed and applied to {self.shadow_table_name}.")

    def _swap_tables(self):
        logger.info('Swapping tables...')
        self.cursor.execute(f"RENAME TABLE {self.table} TO {self.table}_old, {self.shadow_table_name} TO {self.table}")
        logger.info(f"Tables swapped: {self.table} with {self.shadow_table_name}.")

    def _drop_old_table(self):
        logger.info('Dropping old table...')
        self.cursor.execute(f"DROP TABLE {self.table}_old")
        logger.info(f"Old table {self.table}_old dropped.")

    def _drop_triggers(self):
        logger.info('Dropping triggers...')
        self.cursor.execute(f"DROP TRIGGER IF EXISTS {self.table}_insert")
        self.cursor.execute(f"DROP TRIGGER IF EXISTS {self.table}_update")
        self.cursor.execute(f"DROP TRIGGER IF EXISTS {self.table}_delete")
        logger.info(f"Triggers for table {self.table} dropped.")
    
    def _drop_audit_table(self):
        logger.info('Dropping audit table...')
        self.cursor.execute(f"DROP TABLE {self.audit_table_name}")
        logger.info(f"Audit table {self.audit_table_name} dropped.")
    
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
                logger.info('dropped triggers')
                self._drop_triggers()
            if self.swap_tables and self.drop_old_table:
                self._drop_old_table()
            if self.audit_table_name:
                logger.info('dropped audit')
                self._drop_audit_table()
            # TODO: is new table is similar to old
            # TODO: index?
            # TODO: drop temp table
            # TODO: resume by temp table data
            logger.info("Migration completed successfully.")
        except mysql.connector.Error as err:
            logger.error(f"Error: {err}", exc_info=True)
        finally:
            self.cursor.close()
            self.cnx.close()
            logger.info('Database connection closed.')
            logger.info(f'Spend {round(time.perf_counter() - self.s_time, 3)}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Admitad Online Schema Change Tool')
    parser.add_argument('--host', required=True, help='MySQL host')
    parser.add_argument('--port', type=int, help='MySQL port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--alter', required=True, help='Alter command to apply to the new table')
    parser.add_argument('--user', required=True, help='MySQL user')
    parser.add_argument('--password', required=True, help='MySQL password')
    parser.add_argument('--chunk-size', type=int, required=True, dest='chunk_size', default=1000, help='Copy chunk size')
    parser.add_argument('--swap-tables', action='store_true', default=False, help='Swap tables after migration')
    parser.add_argument('--drop-old-table', action='store_true', default=False, help='Drop old table after swapping')
    parser.add_argument('--drop-triggers', action='store_true', default= False, help='Drop triggers after migration')
    parser.add_argument('--drop-audit-table', action='store_true', default=False, help='Drop audit table after migration')
    
    args = parser.parse_args()
    logger.info(args)
    
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
