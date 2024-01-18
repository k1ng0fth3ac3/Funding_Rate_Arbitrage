from dbManager import Connection
from datetime import date,time,datetime,timezone
from dbTables import Tables_info
import sys

class DBlogger:
    log_table_name = 'action_log_detailed'

    def __init__(self, remote_server: bool = False, terminal_print: bool = False):
        self.conn = Connection(remote_server=remote_server)
        self.terminal_print: bool = terminal_print

        self.counter: float = 0     # For any misc counting of data
        self.counter2: float = 0    # For any misc counting of data

        table_info = Tables_info()
        self.columns = list(table_info.action_log_detailed().keys())

    def add(self, action: str, sub_action: str,type: str,details: str):
        dateNow = datetime.now(timezone.utc).date()
        timeNow = datetime.now(timezone.utc).time().strftime("%H:%M:%S")

        data = []
        data_row = (
            f'{dateNow}',
            f'{timeNow}',
            f'{action[:100]}',
            f'{sub_action[:100]}',
            f'{type[:20]}',
            f'{details[:255]}'
        )
        data.append(data_row)

        if self.terminal_print:
            print(f'{timeNow} - {action} - {sub_action} - {type} - {details}')

        self.conn.insert_to_table(table_name=self.log_table_name,columns=self.columns,data=data)

    def close_connection(self):
        self.conn.close_connection()

    def exit_code_run_due_to_error(self, details: str = None):
        print(f'An error occured and run was terminated! {details}')
        sys.exit(1)
