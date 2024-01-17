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

    def funding_rates(self):
        table_name = 'funding_rates'

        dicCols = self.table_info.funding_rates()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


class Tables_info:


    def action_log(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['date'] = 'DATE'
        dicCols['time'] = 'TIME'
        dicCols['action'] = 'VARCHAR(20)'
        dicCols['table_name'] = 'VARCHAR(50)'
        dicCols['rows'] = 'DECIMAL(8,0)'
        dicCols['note'] = 'VARCHAR(255)'

        return dicCols

    def funding_rates(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['date'] = 'DATE'
        dicCols['time'] = 'TIME'
        dicCols['exchange_id'] = 'VARCHAR(30)'
        dicCols['symbol'] = 'VARCHAR(100)'
        dicCols['base'] = 'VARCHAR(20)'
        dicCols['target'] = 'VARCHAR(10)'
        dicCols['funding_rate'] = 'DECIMAL(8,6)'
        dicCols['open_interest'] = 'DECIMAL(16,0)'
        dicCols['volume'] = 'DECIMAL(16,0)'

        return dicCols

