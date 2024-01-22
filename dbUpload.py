from datetime import datetime,timezone,date,time
from dbTables import Tables_info
from dbManager import Connection
from geckoFutures import Gecko,Exchange_pair
from logToDb import DBlogger
from psycopg2 import Error as PsycopgError  # Import the specific exception type


class Upload:

    def __init__(self, logger: DBlogger, remote_server: bool = False):
        self.action = 'Data upload'
        self.tables_info = Tables_info()
        self.remote_server = remote_server
        self.gecko = Gecko(logger=logger)

        self.logger = logger

        self.logger.add('Data upload','Initialize','Success','Created Tables_info and gecko objects')

    def funding_rates(self, exchange_count: int = 40):

        # ---------------------------------------- SETUP
        # ----- Connect to Database and Get Table info
        table_name = 'funding_rates'

        try:
            connection = Connection(remote_server=self.remote_server)
            self.logger.add('Data upload', 'Establishing connection to database', 'Success', 'Connected')
        except PsycopgError as e:
            # Specific database-related error
            error_message = str(e)[-255:]
            self.logger.add('Data upload', 'Establishing connection to database', 'Error', error_message)
            self.logger.exit_code_run_due_to_error(error_message)
        except Exception as e:
            # Other unexpected exceptions
            error_message = str(e)[-255:]
            self.logger.add('Data upload', 'Establishing connection to database', 'Error', error_message)
            self.logger.exit_code_run_due_to_error(error_message)

        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        # ----- Check Time difference between last upload and now
        try:
            dateNow = datetime.now(timezone.utc).date()
            timeNow = datetime.now(timezone.utc).time().strftime("%H:%M:%S")
            if datetime.now(timezone.utc).time() >= time(20, 0) or datetime.now(timezone.utc).time() < time(4, 0):
                fundingCycle = 1
            elif datetime.now(timezone.utc).time() < time(12, 0):
                fundingCycle = 2
            elif datetime.now(timezone.utc).time() < time(20, 0):
                fundingCycle = 3

            lastUpdDate = dicTableInfo['upload_last_date']
            lastUpdTime = dicTableInfo['upload_last_time']
            if lastUpdDate is not None:
                if lastUpdTime >= time(20, 0) or lastUpdTime < time(4, 0):
                    lastFundingCycle = 1
                elif lastUpdTime < time(12, 0):
                    lastFundingCycle = 2
                elif lastUpdTime < time(20, 0):
                    lastFundingCycle = 3
            # -----/

            self.logger.add('Data upload', 'Calculating funding Cycle', 'Success', 'Calculated')
        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', 'Calculating funding Cycle', 'Error', f'{error_message}')
            self.logger.exit_code_run_due_to_error(f'{error_message}')


        # ----- In case we have data for the same funding cycle, delete the previous data and re-upload
        try:
            if lastUpdDate is not None and fundingCycle == lastFundingCycle and lastUpdDate == dateNow:
                connection.delete_date_time_from_table(table_name=table_name,dateCol='utc_date',timeCol='utc_time',params=(lastUpdDate,lastUpdTime))
                self.logger.add('Data upload', 'Delete previous data for the day and Cycle', 'Success', 'Deleted')
        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', 'Delete previous data for the day and Cycle', 'Error', f'{error_message}')
            self.logger.exit_code_run_due_to_error(f'{error_message}')
        # -----/

        # ----- Get column Names
        fr_columns = list(self.tables_info.funding_rates().keys())      # Funding Rate columns
        ul_columns = list(self.tables_info.update_log().keys())         # Upload log columns
        # -----/
        # ----------------------------------------/

        # ---------------------------------------- GET DATA FROM API
        self.gecko.get_exchanges(exchange_count)
        self.gecko.get_futures_data()

        self.logger.add('Data upload', f'All Funding rate data Fetched', 'Success',
                        f'Ready to upload the data to DB')
        # ----------------------------------------/


        # ---------------------------------------- FUNDING RATE DATA
        try:
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

            self.logger.add('Data upload', f'Object Funding Rate Data processing into rows', 'Success',
                            f'All data converted into rows')
        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Object Funding Rate Data processing into rows', 'Error',
                            f'{error_message}')

        try:
            connection.insert_to_table(table_name,fr_columns,data)

            self.logger.add('Data upload', f'Upload data to {table_name} table', 'Success',
                            f'{len(data)} rows uploaded')
        except PsycopgError as e:
            # Specific database-related error
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to {table_name} table', 'Error', error_message)
            self.logger.exit_code_run_due_to_error(error_message)
        except Exception as e:
            # Other unexpected exceptions
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to {table_name} table', 'Error', error_message)
            self.logger.exit_code_run_due_to_error(error_message)

        connection.add_to_action_log(table_name,self.action,len(data),
                                     f'Data fetched from {len(dicExchanges)} exchanges',customTime=timeNow)

        # ----------------------------------------/

        # ---------------------------------------- UPDATE LOG
        # ----- Update_log data
        try:
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


            self.logger.add('Data upload', f'Object Log Data processing into rows', 'Success',
                            f'All data converted into rows')
        except:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Object Log Data processing into rows', 'Error',
                            f'{error_message}')
        # -----/

        # ----- Upload Update log datA
        try:
            connection.insert_to_table('update_log',ul_columns,data)
            self.logger.add('Data upload', f'Upload data to upload_log table', 'Success',
                        f'{len(data)} rows uploaded')
        except PsycopgError as e:
            # Specific database-related error
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to update:log table', 'Error', error_message)
            self.logger.exit_code_run_due_to_error(error_message)
        except Exception as e:
            # Other unexpected exceptions
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to update_log table', 'Error', error_message)
            self.logger.exit_code_run_due_to_error(error_message)

        connection.add_to_action_log('update_log',self.action,len(data),'-',customTime=timeNow)
        # -----/

        connection.close_connection()
        # ----------------------------------------/