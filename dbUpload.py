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

    def funding_rates(self, exchange_count: int = 40, min_fr: float = 0.015, min_vol: float = 15000, min_oi: float = 15000):

        self.logger.add('Data upload', 'Initialize', 'Success', 'Created Tables_info and gecko objects')

        # ---------------------------------------- SETUP
        # ----- Connect to Database and Get Table info
        table_name = 'funding_rates_2h'

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
            fundingCycle = self.get_funding_cycle(refTime=datetime.now(timezone.utc).time(),is2hourCycle=True)

            lastUpdDate = dicTableInfo['upload_last_date']
            lastUpdTime = dicTableInfo['upload_last_time']
            if lastUpdDate is not None:
                lastFundingCycle = self.get_funding_cycle(refTime=lastUpdTime, is2hourCycle=True)
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

        # Get active pairs
        dicActivePairs = connection.get_uniq_values_from_col('active_pairs','exchange_id_symbol')
        dicToActivePairs = {}   # New pairs to be added


        # ---------------------------------------- FUNDING RATE DATA
        try:
            dicExchanges = {}           # Key: exchange_id, Value: dic with currency- and pairs counts
            data = []
            for ep in self.gecko.exchange_pairs.values():

                # ----- Either our FR + Vol meets criteria this Cycle, or it met criteria in the past (is in active_pairs)
                exchange_symbol = f'{ep.exchange_id}_{ep.symbol}'
                criteriaPass = False
                if abs(ep.funding_rate) >= min_fr and ep.volume_usd >= min_vol and ep.open_interest >= min_oi:
                    criteriaPass = True
                    if not exchange_symbol in dicActivePairs:
                        dicToActivePairs[exchange_symbol] = ep
                else:
                    if exchange_symbol in dicActivePairs:
                        criteriaPass = True
                # -----/


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

        # ---------------------------------------- ACTIVE PAIRS
        try:
            data = []  # Reset
            for ex_pair, ep in dicToActivePairs.items():
                data_row = ()
                data_row = (f'{dateNow}',
                            f'{ep.exchange_id}',
                            f'{ep.base}',
                            f'{ex_pair}'
                            )
                data.append(data_row)

            self.logger.add('Data upload', f'Active pairs data processing into rows', 'Success',
                            f'All data converted into rows')

        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Active pairs data processing into rows', 'Error',
                            f'{error_message}')

        # ----- Upload data to active_pairs
        try:
            ap_columns = list(self.tables_info.active_pairs().keys())
            connection.insert_to_table('active_pairs', ap_columns, data)
            self.logger.add('Data upload', f'Upload data to active_pairs table', 'Success',
                            f'{len(data)} rows uploaded')
        except PsycopgError as e:
            # Specific database-related error
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to active_pairs table', 'Error', error_message)
        except Exception as e:
            # Other unexpected exceptions
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to active_pairs table', 'Error', error_message)
        # -----/


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
        except Exception as e:
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


    def get_funding_cycle(self, refTime, is2hourCycle: bool = True):

        # 2-hour Cycle: 1 = 20:00-22:00, 2 = 22:00-24:00, 3 = 00:00-02:00, 02:00-04:00, etc. etc.
        # 8-hour Cycle: 1 = 20:00-04:00, 2 = 04:00-12:00, 3 = 12:00-20:00

        if is2hourCycle:    # 2 Hour cycle
            fundingCycle = ((refTime.hour - 20) % 24) // 2 + 1
        else:               # 8 Hour cycle
            fundingCycle = ((refTime.hour - 20) % 24) // 8 + 1

        return fundingCycle

    def calc_table(self):
        table_name = 'calc_table'
        connection = Connection()
        connection.clear_whole_table(table_name)

        to_columns = 'exchange_id, symbol, base, target, cycle_count, cycle_nr, volume, spread, open_interest, funding_rate, avg_cycle_3, avg_cycle_6, avg_cycle_9, avg_cycle_12, avg_cycle_15, avg_cycle_18, avg_cycle_21'
        from_table = f"""
                    (
                ------	CALC AVERAGES
                SELECT	exchange_id,
                        symbol,
                        base,
                        target,
                        cycle_count,
                        cycle_nr,
                        volume,
                        spread,
                        open_interest,
                        funding_rate,
                        
                        CASE WHEN cycle_count >= 3 THEN AVG(funding_rate) OVER ( PARTITION BY exchange_id, symbol
                            ORDER BY utc_date DESC, utc_time DESC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
                        ) ELSE NULL END AS avg_cycle_3,								-- AVG FR 3 Cycles
                        
                        CASE WHEN cycle_count >= 6 THEN AVG(funding_rate) OVER ( PARTITION BY exchange_id, symbol
                            ORDER BY utc_date DESC, utc_time DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING
                        ) ELSE NULL END AS avg_cycle_6,								-- AVG FR 6 Cycles
                        
                        CASE WHEN cycle_count >= 9 THEN AVG(funding_rate) OVER ( PARTITION BY exchange_id, symbol
                            ORDER BY utc_date DESC, utc_time DESC ROWS BETWEEN CURRENT ROW AND 8 FOLLOWING
                        ) ELSE NULL END AS avg_cycle_9,								-- AVG FR 9 Cycles
            
                        CASE WHEN cycle_count >= 12 THEN AVG(funding_rate) OVER ( PARTITION BY exchange_id, symbol
                            ORDER BY utc_date DESC, utc_time DESC ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING
                        ) ELSE NULL END AS avg_cycle_12,							-- AVG FR 12 Cycles
                        
                        CASE WHEN cycle_count >= 15 THEN AVG(funding_rate) OVER ( PARTITION BY exchange_id, symbol
                            ORDER BY utc_date DESC, utc_time DESC ROWS BETWEEN CURRENT ROW AND 14 FOLLOWING
                        ) ELSE NULL END AS avg_cycle_15,								-- AVG FR 15 Cycles
                        
                        CASE WHEN cycle_count >= 18 THEN AVG(funding_rate) OVER ( PARTITION BY exchange_id, symbol
                            ORDER BY utc_date DESC, utc_time DESC ROWS BETWEEN CURRENT ROW AND 17 FOLLOWING
                        ) ELSE NULL END AS avg_cycle_18,								-- AVG FR 18 Cycles
                        
                        CASE WHEN cycle_count >= 21 THEN AVG(funding_rate) OVER ( PARTITION BY exchange_id, symbol
                            ORDER BY utc_date DESC, utc_time DESC ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING
                        ) ELSE NULL END AS avg_cycle_21									-- AVG FR 21 Cycles
                 FROM	(
                
                
                ------	CYCLE COUNT + RANK
                SELECT	DENSE_RANK() OVER (PARTITION BY all_cycles.exchange_id, all_cycles.symbol ORDER BY all_cycles.utc_date DESC,all_cycles.utc_time DESC) AS cycle_nr,
                        COUNT(all_cycles.*) OVER (PARTITION BY all_cycles.exchange_id, all_cycles.symbol) AS cycle_count,
                        all_cycles.*
                  FROM	(
                
                ------	LATEST CYCLE
                SELECT	exchange_id,
                        symbol
                  FROM	funding_rates
                 WHERE	utc_date = (SELECT utc_date FROM action_log WHERE table_name = 'funding_rates' ORDER BY	id DESC LIMIT 1)
                   AND	utc_time = (SELECT utc_time FROM action_log WHERE table_name = 'funding_rates' ORDER BY	id DESC LIMIT 1)
                   AND	volume > 15000
                   AND	open_interest > 15000
                   AND	ABS(funding_rate) > 0.01
                ------/ LATEST CYCLE
                      
                      ) AS act_cycle
                  LEFT OUTER JOIN funding_rates AS all_cycles
                    ON	act_cycle.exchange_id = all_cycles.exchange_id AND act_cycle.symbol = all_cycles.symbol
                ------/ CYCLE COUNT + RANK
                     
                     )
              GROUP BY	exchange_id,
                        symbol,
                        funding_rate,
                        utc_date,
                        utc_time,
                        cycle_count,
                        cycle_nr,
                        open_interest,
                        volume,
                        spread,
                        base,
                        target
                ------/	CALC AVERAGES
                      )
                    """
        columns = f"""
                    *
                    """
        where_clause = f"""cycle_nr = %s"""
        order_by = f"""ABS(funding_rate) DESC"""


        try:
            connection.insert_to_table_with_sql(table_name, to_columns=to_columns, from_table_name=from_table,
                                                columns=columns,where_clause=where_clause, order_by=order_by,
                                                params=(1,))

            dicInfo = connection.get_table_info(table_name)
            rowC = dicInfo['total_rows']

            self.logger.add('Data upload', f'Uploading data calculations', 'Success',
                            f'{rowC} added to the table')

        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Uploading data calculations', 'Error',
                            f'{error_message}')


        dicInfo = connection.get_table_info(table_name)
        connection.add_to_action_log(table_name,'data_calculations',dicInfo['total_rows'],'Calculated')
        connection.close_connection()