from datetime import datetime,time,date,timezone
import psycopg2
from keys import Postgres_keys


# Somehow ENSURE that the connection is always closed. Otherwise there can be data leakage and


class Connection:


    def __init__(self):

        self.conn = psycopg2.connect(host='67.211.45.183', dbname='Funding_Rate_Data', user=Postgres_keys.userName,
                            password=Postgres_keys.pw, port=5433)

        #self.conn = psycopg2.connect(host='localhost', dbname='Funding_Rate_Data', user=Postgres_keys.userName,
        #                    password=Postgres_keys.pw, port=5432)


        self.cur = self.conn.cursor()


    def close_connection(self):
        self.cur.close()
        self.conn.close()

    def create_table(self, table_name: str, dicCols: dict):
        col_def = ''
        for col, col_type in dicCols.items():
            if col_def != '':
                col_def += ',\n'
            col_def += f'{col} {col_type}'

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {col_def}
        );
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'{table_name} created')

    def insert_to_table(self, table_name: str, columns: list, data: list):

        columns.remove('id')        # We don't need the ID col

        columns_str = ''
        values_str = ''
        for column in columns:
            if columns_str != '':
                columns_str = columns_str + ', '
                values_str = values_str + ', '
            columns_str = columns_str + column
            values_str = values_str + '%s'

        query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({values_str});
        """

        self.cur.executemany(query, data)
        self.conn.commit()


    def update_records(self, table_name: str, columns: list, values: list, where_clause_sql: str):
        pass

    def delete_records(self, table_name, where_clause_sql: str):
        pass

    def truncate_table(self, table_name: str):
        pass


    def select_first_row(self, table_name: str):
        pass

    def select_last_row(self, table_name: str):
        pass

    def select_custom(self, sql_statement: str):
        pass

    def get_table_info(self, table_name: str):

        query = """
            SELECT * FROM action_log
            WHERE table_name = %s;
        """

        self.cur.execute(query, (table_name,))
        results = self.cur.fetchall()

        dicTableInfo = {
            'created_on' : None,                        # Table created on
            'total_log_entries': 0,                     # Total number of log entries
            'upload_count': 0,                          # Number of times we have uploaded data
            'upload_rows': 0,                           # Number of rows uploaded in total
            'upload_first_date': None,                  # First date of upload
            'upload_first_time': None,                  # First time of upload
            'upload_last_date': None,                   # Last date of upload
            'upload_last_time': None,                   # Last time of upload
            'total_rows': 0,                            # Total actual rows (might differ quite a bit from the uploaded rows
            'total_size_gb': 0                          # Total table size in GigaBits
        }

        for row in results:
            dicTableInfo['total_log_entries'] += 1

            if row[3] == 'Create table':
                dicTableInfo['created_on'] = row[1]

            if row[3] == 'Data upload':
                dicTableInfo['upload_count'] += 1
                dicTableInfo['upload_rows'] += row[5]

                if dicTableInfo['upload_first_date'] is None or dicTableInfo['upload_first_date'] >= row[1]:
                    dicTableInfo['upload_first_date'] = row[1]
                    dicTableInfo['upload_first_time'] = row[2]

                if dicTableInfo['upload_last_date'] is None or dicTableInfo['upload_last_date'] <= row[1]:
                    dicTableInfo['upload_last_date'] = row[1]
                    dicTableInfo['upload_last_time'] = row[2]


        # ----- Columns
        query2 = f"""
        SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';
        """

        self.cur.execute(query2)
        dicTableInfo['columns'] = [row[0] for row in self.cur.fetchall()]
        # -----/

        # ----- Total rows
        query3 = f"""
        SELECT COUNT(id) FROM {table_name};
        """

        self.cur.execute(query3)
        result = self.cur.fetchone()
        if result is not None:
            dicTableInfo['total_rows'] = result[0]
        else:
            dicTableInfo['total_rows'] = 0
        # -----/

        # ----- Total Table size in GigaBits
        query4 = f"""
            SELECT pg_total_relation_size('{table_name}')
            """

        self.cur.execute(query4)
        result = self.cur.fetchone()
        if result is not None:
            dicTableInfo['total_size_gb'] = result[0] / 1000000
        else:
            dicTableInfo['total_size_gb'] = 0
        # -----/

        return dicTableInfo

    def add_to_action_log(self, table_name: str, action: str, rows: float, note: str, customTime = None):
        now = datetime.now(timezone.utc)
        date = now.date()
        if customTime is None:
            time = now.time().replace(microsecond=0)
        else:
            time = customTime


        query = f"""
        INSERT INTO action_log (utc_date, utc_time, action, table_name, rows, note)
        VALUES ('{date}', '{time}', '{action}', '{table_name}', {rows}, '{note}');
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'Added to log: {table_name} -- {action} -- {date} -- {rows}')

    def delete_log_entry(self, table_name: str, action: str, date = datetime.now(timezone.utc).date()):
        query = f"""
        DELETE FROM action_log WHERE
        table_name = '{table_name}' AND action = '{action}' AND date = '{date}';
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'Deleted entry {table_name} -- {action} -- {date}')

    def delete_day_from_table(self, table_name: str, dateCol: str = 'date', date = datetime.now(timezone.utc).date()):
        query = f"""
        DELETE FROM {table_name} WHERE {dateCol} = '{date}';
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'Deleted entries from table {table_name} for date {date}')

    def delete_date_time_from_table(self, table_name: str, dateCol: str = 'date', timeCol: str = 'time',params: tuple = ()):
        query = f"""
        DELETE FROM {table_name} WHERE {dateCol} = %s AND {timeCol} = %s;
        """

        self.cur.execute(query,params)
        self.conn.commit()

        print(f'Deleted entries from table {table_name} for datetime {params}')


    def delete_older_than_x_days_from_table(self,table_name: str, dateCol: str = 'date', params: tuple = ()):

        query = f"""
                DELETE FROM {table_name} WHERE {dateCol} <= CURRENT_DATE - %s;
                """

        self.cur.execute(query,params)
        self.conn.commit()

        print(f'Deleted entries from table {table_name}')


    def clear_whole_table(self, table_name: str):
        query = f"""
        DELETE FROM {table_name};
        """
        self.cur.execute(query)
        self.conn.commit()

        print(f'Cleared table {table_name}')


    def get_uniq_values_from_col(self, table_name: str, col_name: str, col_2_name: str = None):

        # If we have only 1 column, we get count of occurances
        # If we have 2 columns, the Column 1 will be the dic Key and the Col 2 will be a value.

        dicData = {}

        if col_2_name is None:
            query = f"""
                  SELECT  {col_name},
                          COUNT({col_name}) AS count
                    FROM  {table_name}
                GROUP BY  {col_name}
                ORDER BY  2 DESC;
            """
        else:
            query = f"""
                  SELECT  {col_name},
                          {col_2_name}
                    FROM  {table_name}
                ORDER BY  {col_name} DESC;
            """

        self.cur.execute(query)
        results = self.cur.fetchall()

        if col_2_name is None:
            for row in results:
                value = row[0]
                count = row[1]
                dicData[value] = count
        else:
            for row in results:
                dicData[row[0]] = row[1]

        return dicData

    def insert_to_table_with_sql(self, to_table_name: str, to_columns: str, from_table_name: str, columns: str = '*',
                        where_clause: str = None, group_by:str = None, order_by: str = None, params: tuple = ()):

        query = f"""
            INSERT INTO {to_table_name} ({to_columns})
            SELECT  {columns}
            FROM  {from_table_name}
            {f'WHERE {where_clause}' if where_clause else ''}
            {f'GROUP BY {group_by}' if group_by else ''}
            {f'ORDER BY {order_by}' if order_by else ''}
             ;
        """

        self.cur.execute(query, params)
        self.conn.commit()

    def select_table_data(self, table_name: str, columns: str = '*', where_clause: str = None, group_by:str = None,
                          order_by: str = None, params: tuple = ()):
        # if we need to have joins, just write it in the table_name
        # Write %s in place of all the parameters we wish to pass and then define them in the same order in the tuple
        # If we need to use the LIKE keyword, always have parameter for it and don't hardcode it.

        query = f"""
            SELECT  {columns}
            FROM  {table_name}
            {f'WHERE {where_clause}' if where_clause else ''}
            {f'GROUP BY {group_by}' if group_by else ''}
            {f'ORDER BY {order_by}' if order_by else ''}
             ;
        """

        self.cur.execute(query, params)
        results = self.cur.fetchall()

        return results