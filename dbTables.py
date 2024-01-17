from dbManager import Connection



class Create_table:

    def __init__(self, connObj: Connection):
        self.connection = connObj
        self.action = 'Create table'
        self.table_info = Tables_info()

    def action_log(self):
        table_name = 'action_log'

        dicCols = self.table_info.action_log()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def update_log(self):
        table_name = 'update_log'

        dicCols = self.table_info.update_log()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def exchange_info(self):
        table_name = 'exchange_info'

        dicCols = self.table_info.exchange_info()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


    def funding_rates(self):
        table_name = 'funding_rates'

        dicCols = self.table_info.funding_rates()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


class Tables_info:


    def action_log(self):
        # For each time we run a code
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['utc_date'] = 'DATE'
        dicCols['utc_time'] = 'TIME'
        dicCols['action'] = 'VARCHAR(20)'
        dicCols['table_name'] = 'VARCHAR(50)'
        dicCols['rows'] = 'DECIMAL(8,0)'
        dicCols['note'] = 'VARCHAR(255)'

        return dicCols

    def update_log(self):
        # For each run and individual exchange we have 1 row of data
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['utc_date'] = 'DATE'
        dicCols['utc_time'] = 'TIME'
        dicCols['funding_cycle'] = 'INT'                    # 1 = 04:00 UTC, 2 = 12:00 UTC, 3 = 20:00 UTC
        dicCols['exchange_id'] = 'VARCHAR(30)'
        dicCols['currencies'] = 'INT'                       # Individual currencies (ETH, BTC, SOL, etc.)
        dicCols['pairs'] = 'INT'                            # Individual pairs (ETH-USDT, ETH-USDC, etc.)
        dicCols['volume'] = 'DECIMAL(16,0)'                 # 24h Volume (only the pairs that pass our filters)

        return dicCols

    def exchange_info(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['exchange_id'] = 'VARCHAR(30)'
        dicCols['exchange_name'] = 'VARCHAR(30)'
        dicCols['is_dex'] = 'BOOL'
        dicCols['first_update_date'] = 'DATE'
        dicCols['first_update_time'] = 'TIME'
        dicCols['last_update_date'] = 'DATE'
        dicCols['last_update_time'] = 'TIME'
        dicCols['update_count'] = 'INT'

        return dicCols


    def funding_rates(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['utc_date'] = 'DATE'
        dicCols['utc_time'] = 'TIME'
        dicCols['funding_cycle'] = 'INT'                            # 1 = 04:00 UTC, 2 = 12:00 UTC, 3 = 20:00 UTC
        dicCols['exchange_id'] = 'VARCHAR(30)'
        dicCols['symbol'] = 'VARCHAR(100)'
        dicCols['base'] = 'VARCHAR(20)'
        dicCols['target'] = 'VARCHAR(10)'
        dicCols['funding_rate'] = 'DECIMAL(8,6)'
        dicCols['open_interest'] = 'DECIMAL(16,0)'
        dicCols['volume'] = 'DECIMAL(16,0)'
        dicCols['spread'] = 'DECIMAL(10,9)'

        return dicCols

