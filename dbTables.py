from dbManager import Connection



class Create_table:

    def __init__(self, connObj: Connection):
        self.connection = connObj
        self.action = 'Create table'
        self.table_info = Tables_info()

    def action_log(self, delete_existing: bool = False):
        table_name = 'action_log'

        dicCols = self.table_info.action_log()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def update_log(self, delete_existing: bool = False):
        table_name = 'update_log'

        dicCols = self.table_info.update_log()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def action_log_detailed(self, delete_existing: bool = False):
        table_name = 'action_log_detailed'

        dicCols = self.table_info.action_log_detailed()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


    def active_pairs(self, delete_existing: bool = False):
        table_name = 'active_pairs'

        dicCols = self.table_info.active_pairs()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')

    def exchange_info(self, delete_existing: bool = False):
        table_name = 'exchange_info'

        dicCols = self.table_info.exchange_info()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


    def funding_rates_2h(self, delete_existing: bool = False):
        table_name = 'funding_rates_2h'

        dicCols = self.table_info.funding_rates()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


    def funding_rates(self, delete_existing: bool = False):
        table_name = 'funding_rates'

        dicCols = self.table_info.funding_rates()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def calc_table(self, delete_existing: bool = False):
        table_name = 'calc_table'

        dicCols = self.table_info.calc_table()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')

    def result_table(self, delete_existing: bool = False):
        table_name = 'result_table'

        dicCols = self.table_info.result_table()

        self.connection.create_table(table_name=table_name,dicCols=dicCols,delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')

    def price_history(self,delete_existing: bool = False):
        table_name = 'price_history'

        dicCols = self.table_info.price_history()

        self.connection.create_table(table_name=table_name, dicCols=dicCols, delete_existing=delete_existing)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')


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

    def action_log_detailed(self):
        # Detailed log which is updated on each individual event
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['utc_date'] = 'DATE'
        dicCols['utc_time'] = 'TIME'
        dicCols['action'] = 'VARCHAR(100)'              # Data upload, etc.
        dicCols['sub_action'] = 'VARCHAR(100)'          # Initialization, upload of x, fetching from api, etc.
        dicCols['type'] = 'VARCHAR(20)'                 # Success / Warning / Error
        dicCols['details'] = 'VARCHAR(255)'             # Any details

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

    def active_pairs(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['utc_date_added'] = 'DATE'
        dicCols['utc_last_active'] = 'DATE'                 # Last active date (delete items that are too old)
        dicCols['exchange_id'] = 'VARCHAR(30)'
        dicCols['base'] = 'VARCHAR(100)'
        dicCols['exchange_id_symbol'] = 'VARCHAR(130)'      # Exchange id + _ + symbol

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

    def calc_table(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['exchange_id'] = 'VARCHAR(30)'
        dicCols['symbol'] = 'VARCHAR(100)'
        dicCols['base'] = 'VARCHAR(20)'
        dicCols['target'] = 'VARCHAR(10)'
        dicCols['cycle_count'] = 'INT'
        dicCols['cycle_nr'] = 'INT'                     # Not really used, always 1
        dicCols['volume'] = 'DECIMAL(16,0)'
        dicCols['spread'] = 'DECIMAL(10,9)'
        dicCols['open_interest'] = 'DECIMAL(16,0)'
        dicCols['funding_rate'] = 'DECIMAL(8,6)'
        dicCols['avg_cycle_3'] = 'DECIMAL(8,6)'         # Funding rate avg from past 3 Cycles
        dicCols['avg_cycle_6'] = 'DECIMAL(8,6)'
        dicCols['avg_cycle_9'] = 'DECIMAL(8,6)'
        dicCols['avg_cycle_12'] = 'DECIMAL(8,6)'
        dicCols['avg_cycle_15'] = 'DECIMAL(8,6)'
        dicCols['avg_cycle_18'] = 'DECIMAL(8,6)'
        dicCols['avg_cycle_21'] = 'DECIMAL(8,6)'

        return dicCols

    def result_table(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['delta'] = 'DECIMAL(8,6)'
        dicCols['apr'] = 'DECIMAL(10,3)'

        dicCols['base'] = 'VARCHAR(20)'
        dicCols['exchange_id_1'] = 'VARCHAR(30)'
        dicCols['funding_rate_1'] = 'DECIMAL(8,6)'
        dicCols['exchange_id_2'] = 'VARCHAR(30)'
        dicCols['funding_rate_2'] = 'DECIMAL(8,6)'

        dicCols['target_1'] = 'VARCHAR(10)'
        dicCols['target_2'] = 'VARCHAR(10)'

        dicCols['volume_1'] = 'DECIMAL(16,0)'
        dicCols['volume_2'] = 'DECIMAL(16,0)'

        dicCols['spread_1'] = 'DECIMAL(10,9)'
        dicCols['spread_2'] = 'DECIMAL(10,9)'

        dicCols['open_interest_1'] = 'DECIMAL(16,0)'
        dicCols['open_interest_2'] = 'DECIMAL(16,0)'

        dicCols['avg_delta_3'] = 'DECIMAL(8,6)'
        dicCols['avg_delta_6'] = 'DECIMAL(8,6)'
        dicCols['avg_delta_9'] = 'DECIMAL(8,6)'
        dicCols['avg_delta_12'] = 'DECIMAL(8,6)'
        dicCols['avg_delta_15'] = 'DECIMAL(8,6)'
        dicCols['avg_delta_18'] = 'DECIMAL(8,6)'
        dicCols['avg_delta_21'] = 'DECIMAL(8,6)'

        return dicCols


    def price_history(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['utc_date'] = 'DATE'
        dicCols['base'] = 'VARCHAR(20)'
        dicCols['price'] = 'DECIMAL(16,10)'
        dicCols['price_change_24h'] = 'DECIMAL(8,3)'

        return dicCols


