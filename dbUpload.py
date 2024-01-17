from datetime import datetime,timezone,date,time
from dbTables import Tables_info
from dbManager import Connection
from geckoFutures import Gecko,Exchange_pair

class Upload:

    def __init__(self):
        self.action = 'Data upload'
        self.tables_info = Tables_info()
        self.conn = Connection()
        self.gecko = Gecko()

    def funding_rates(self):

        # ---------------------------------------- SETUP
        # ----- Get Table info
        table_name = 'funding_rates'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        # ----- Check Time difference between last upload and now
        dateNow = datetime.now(timezone.utc).date()
        timeNow = datetime.now(timezone.utc).time().strftime("%H:%M:%S")
        if time(20, 0) <= datetime.now(timezone.utc).time() < time(4, 0):
            fundingCycle = 1
        elif time(4, 0) <= datetime.now(timezone.utc).time() < time(8, 0):
            fundingCycle = 2
        elif time(8, 0) <= datetime.now(timezone.utc).time() < time(20, 0):
            fundingCycle = 3

        lastUpdDate = dicTableInfo['upload_last_date']
        lastUpdTime = dicTableInfo['upload_last_time']
        if lastUpdDate is not None:
            if time(20, 0) <= lastUpdTime < time(4, 0):
                lastFundingCycle = 1
            elif time(4, 0) <= lastUpdTime < time(8, 0):
                lastFundingCycle = 2
            elif time(8, 0) <= lastUpdTime < time(20, 0):
                lastFundingCycle = 3
        # -----/

        # ----- In case we have data for the same funding cycle, delete the previous data and re-upload
        if lastUpdDate is not None and fundingCycle == lastFundingCycle and lastUpdDate == dateNow:
            connection.delete_date_time_from_table(table_name=table_name,dateCol='utc_date',timeCol='utc_time',params=(lastUpdDate,lastUpdTime))
        # -----/

        # ----- Get column Names
        fr_columns = dicTableInfo['columns']                            # funding_rates columns
        dicTableInfo = connection.get_table_info('update_log')
        ul_columns = dicTableInfo['columns']                            # update_log columns
        dicTableInfo = connection.get_table_info('exchange_info')
        ei_columns = dicTableInfo['columns']                            # exchange_info columns
        # -----/
        # ----------------------------------------/

        # ---------------------------------------- UPLOAD DATA
        self.gecko.get_exchanges(40)
        self.gecko.get_futures_data()

        dicExchanges = {}           # Key: exchange_id, Value: dic with currency- and pairs counts


        data = []
        for ep in self.gecko.exchange_pairs.values():


            # ----- Update log data (for each exchange)
            if ep.exchange_id not in dicExchanges:
                dicExchanges[ep.exchange_id] = {}
                dicExchanges[ep.exchange_id]['currencies_count'] = 0
                dicExchanges[ep.exchange_id]['pairs_count'] = 0
                dicExchanges[ep.exchange_id]['volume'] = 0
                ul_data_row = ()
                currencies = []  # Currency list for the active exchange

            if ep.base not in currencies:
                dicExchanges[ep.exchange_id]['currencies_count'] +=1            # Currencies count
                currencies.append(ep.base)
            dicExchanges[ep.exchange_id]['pairs_count'] +=1                     # Pairs count
            dicExchanges[ep.exchange_id]['volume'] += ep.volume_usd             # Volume
            # -----/


            # ----- Funding Rate data
            data_row = ()
            data_row = (f'{dateNow}',
                        f'{timeNow}',
                           fundingCycle,
                        f'{ep.exchange_id}',
                        f'{ep.symbol}',
                        f'{ep.base}',
                        f'{ep.target}',
                        ep.funding_rate,
                        ep.open_interest,
                        ep.volume_usd,
                        ep.spread
                        )
            data.append(data_row)
            # -----/


        connection.insert_to_table(table_name,fr_columns,data)
        connection.add_to_action_log(table_name,self.action,len(data),f'Data fetched from {len(dicExchanges)} exchanges',customTime=timeNow)


        # ----- Update_log data
        data = []       # Reset
        for exchange_id, dicData in dicExchanges.items():

            data_row = ()
            data_row = (f'{dateNow}',
                        f'{timeNow}',
                           fundingCycle,
                        f'{exchange_id}',
                        dicData['currencies_count'],
                        dicData['pairs_count'],
                        dicData['volume']
                        )
            data.append(data_row)

        connection.insert_to_table('update_log',ul_columns,data)
        connection.add_to_action_log('update_log',self.action,len(data),'-',customTime=timeNow)
        # -----/


        connection.close_connection()
        # ----------------------------------------/