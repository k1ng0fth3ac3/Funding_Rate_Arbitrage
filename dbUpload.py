from datetime import datetime,timezone,date,time
from dbTables import Tables_info
from dbManager import Connection
from geckoFutures import Gecko,Exchange_pair
from logToDb import DBlogger
from psycopg2 import Error as PsycopgError  # Import the specific exception type
from analytics import Analyze


class Upload:

    def __init__(self, logger: DBlogger, remote_server: bool = False):
        self.action = 'Data upload'
        self.tables_info = Tables_info()
        self.remote_server = remote_server
        self.gecko = Gecko(logger=logger)
        self.dicActivePairs = {}

        self.dateNow = datetime.now(timezone.utc).date()
        self.timeNow = datetime.now(timezone.utc).time().strftime("%H:%M:%S")

        self.logger = logger



    def get_funding_rate_data(self,exchange_count, min_fr: float = 0.015, min_vol: float = 15000, min_oi: float = 15000):
        self.logger.add('Data upload', 'Initialize', 'Success', 'Created Tables_info and gecko objects')

        self.active_pairs(exchange_count, min_fr,min_vol,min_oi)
        self.funding_rates_2h()


        if self.get_funding_cycle(refTime=datetime.now(timezone.utc).time(),is2hourCycle=True) in (4, 8, 12):
            self.convert_2h_to_8h_data()
            self.calc_table()
            self.result_table()


    def active_pairs(self,exchange_count: int = 50, min_fr: float = 0.015, min_vol: float = 15000, min_oi: float = 15000):

        # ----- Connect to Database
        table_name = 'active_pairs'

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
        # -----/

        # ---------------------------------------- GET DATA FROM API
        self.gecko.get_exchanges(exchange_count)
        self.gecko.get_futures_data()

        self.logger.add('Data upload', f'All Funding rate data Fetched', 'Success',
                        f'Ready to upload the data to DB')
        # ----------------------------------------/

        # ----- Get active pairs
        self.dicActivePairs = connection.get_uniq_values_from_col('active_pairs','exchange_id_symbol')
        dicToActivePairs = {}   # New pairs to be added
        stillActive = []        # IDs that still passed the filters
        # -----/

        # ----- Get new pairs that meet our criteria
        for ep in self.gecko.exchange_pairs.values():
            exchange_symbol = f'{ep.exchange_id}_{ep.symbol}'
            if abs(ep.funding_rate) >= min_fr and ep.volume_usd >= min_vol and ep.open_interest >= min_oi:
                if not exchange_symbol in self.dicActivePairs:
                    self.dicActivePairs[exchange_symbol] = 1
                    dicToActivePairs[exchange_symbol] = ep
                else:
                    stillActive.append(f'{exchange_symbol}')
        # -----/

        # ---------------------------------------- ACTIVE PAIRS
        try:
            data = []  # Reset
            for ex_pair, ep in dicToActivePairs.items():
                data_row = ()
                data_row = (f'{self.dateNow}',
                            f'{self.dateNow}',
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

        # ---------------------------------------- UPDATE last_active date for the rest
        try:
            if len(stillActive) > 0:
                connection.update_column_by_list_of_values(table_name=table_name,column='utc_last_active', newValue=f'{self.dateNow}',
                                              where_col='exchange_id_symbol',params=(*stillActive,))

            self.logger.add('Data upload', f'Update last_active date for each pair', 'Success',
                            f'{len(stillActive)} pairs updated')

        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Update last_active date for each pair', 'Error', error_message)

        # ----------------------------------------/

        # ---------------------------------------- DELETE pairs that have not been above our threshold for over a week
        try:
            connection.delete_records(table_name=table_name,
                                  where_clause_sql="utc_last_active < NOW() - INTERVAL '%s days'",params=(7,))

            self.logger.add('Data upload', f'Delete old pairs from active_pairs', 'Success', 'Deleted')
        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Delete old pairs from active_pairs', 'Error', error_message)
        # ----------------------------------------/


        connection.close_connection()

    def funding_rates_2h(self):

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
            if lastUpdDate is not None and fundingCycle == lastFundingCycle and lastUpdDate == self.dateNow:
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


        # ---------------------------------------- FUNDING RATE DATA
        try:
            dicExchanges = {}           # Key: exchange_id, Value: dic with currency- and pairs counts
            data = []
            for ep in self.gecko.exchange_pairs.values():

                # ----- Either our FR + Vol meets criteria this Cycle, or it met criteria in the past (is in active_pairs)
                exchange_symbol = f'{ep.exchange_id}_{ep.symbol}'
                if exchange_symbol in self.dicActivePairs:

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
                    data_row = (f'{self.dateNow}',
                                f'{self.timeNow}',
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
                                     f'Data fetched from {len(dicExchanges)} exchanges',customTime=self.timeNow)

        # ----------------------------------------/


        # ----------------------------------------/

        # ---------------------------------------- UPDATE LOG
        # ----- Update_log data
        try:
            data = []       # Reset
            for exchange_id, dicData in dicExchanges.items():

                data_row = ()
                data_row = (f'{self.dateNow}',
                            f'{self.timeNow}',
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

        connection.add_to_action_log('update_log',self.action,len(data),'-',customTime=self.timeNow)
        # -----/

        connection.close_connection()
        # ----------------------------------------/


    def get_funding_cycle(self, refTime, is2hourCycle: bool = True):

        # 2-hour Cycle: 1 = 20:00-22:00, 2 = 22:00-24:00, 3 = 00:00-02:00, 4 = 02:00-04:00, etc. etc.
        # 8-hour Cycle: 1 = 20:00-04:00, 2 = 04:00-12:00, 3 = 12:00-20:00

        if is2hourCycle:    # 2 Hour cycle
            fundingCycle = ((refTime.hour - 20) % 24) // 2 + 1
        else:               # 8 Hour cycle
            fundingCycle = ((refTime.hour - 20) % 24) // 8 + 1

        return fundingCycle


    def convert_2h_to_8h_data(self):
        table_name = 'funding_rates'
        connection = Connection()
        connection.clear_whole_table(table_name)

        to_columns = 'utc_date, utc_time, funding_cycle, exchange_id, symbol, base, target, funding_rate, open_interest, volume, spread'
        from_table = f"""funding_rates_2h"""
        columns = f"""
                    (TO_CHAR(utc_date - INTERVAL '4 HOURS' + INTERVAL '1 DAY', 'YYYY-MM-DD')::date) AS utc_date,
                    MAX(utc_time) AS utc_time,
                    CASE
                        WHEN (funding_cycle BETWEEN 1 AND 4) OR (funding_cycle = 12) THEN 1
                        WHEN funding_cycle BETWEEN 5 AND 8 THEN 2
                        WHEN funding_cycle BETWEEN 9 AND 12 THEN 3
                    END AS funding_cycle,
                    exchange_id,
                    symbol,
                    base,
                    target,
                    AVG(funding_rate) AS funding_rate,
                    AVG(open_interest) AS open_interest,
                    AVG(volume) AS volume,
                    AVG(spread) AS spread
                    """
        group_by = f"""
                    TO_CHAR(utc_date - INTERVAL '4 HOURS' + INTERVAL '1 DAY', 'YYYY-MM-DD'),	-- Shift backwards 4 hours as thats when the cycle starts
                    funding_cycle,
                    exchange_id,
                    symbol,
                    base,
                    target
                    """

        try:
            dicInfo = connection.get_table_info(table_name)
            existing_rows = dicInfo['total_rows']

            connection.insert_to_table_with_sql(table_name, to_columns=to_columns, from_table_name=from_table,
                                                columns=columns, group_by=group_by)

            dicInfo = connection.get_table_info(table_name)
            rowC = dicInfo['total_rows']

            self.logger.add('Data upload', f'Convert 2h data to 8h data', 'Success',
                            f'{rowC} added to the funding_rates table')

            connection.clear_whole_table(table_name='funding_rates_2h')

        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Convert 2h data to 8h data', 'Error',
                            f'{error_message}')


        dicInfo = connection.get_table_info(table_name)
        connection.add_to_action_log(table_name, 'Data conversion', dicInfo['total_rows'] - existing_rows, 'Converted')
        connection.close_connection()



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
                     
                     ) AS cycle_counts
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
                      ) AS calc_averages
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


    def result_table(self):

        # ----- Connect to Database
        table_name = 'result_table'

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
        # -----/


        # ----- Analyze the data
        try:
            analyze = Analyze()
            analyze.get_data()

            self.logger.add('Data upload', 'Analyze data', 'Success', 'Data analyzed')
        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Analyze data', 'Error', error_message)
        # -----/

        # -----  Process data
        try:
            data = []
            for arb in analyze.arbitrage:
                data_row = ()
                data_row = (arb.delta,
                            arb.apr,

                            f'{arb.coin}',
                            f'{arb.exchange_1}',
                            arb.exchange_1_data["funding_rate"],
                            f'{arb.exchange_2}',
                            arb.exchange_2_data["funding_rate"] if arb.exchange_2_data is not None else None,

                            arb.exchange_1_data["target"],
                            arb.exchange_2_data["target"] if arb.exchange_2_data is not None else None,
                            arb.exchange_1_data["volume"],
                            arb.exchange_2_data["volume"] if arb.exchange_2_data is not None else None,
                            arb.exchange_1_data["spread"],
                            arb.exchange_2_data["spread"] if arb.exchange_2_data is not None else None,
                            arb.exchange_1_data["open_interest"],
                            arb.exchange_2_data["open_interest"] if arb.exchange_2_data is not None else None,

                            arb.delta_avg[3],
                            arb.delta_avg[6],
                            arb.delta_avg[9],
                            arb.delta_avg[12],
                            arb.delta_avg[15],
                            arb.delta_avg[18],
                            arb.delta_avg[21]
                            )
                data.append(data_row)

            self.logger.add('Data upload', 'Process data into rows', 'Success', 'Data processed')
        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Process data into rows', 'Error', error_message)
            self.logger.exit_code_run_due_to_error(error_message)
        # -----/

        # ----- Upload data
        try:
            rt_columns = list(self.tables_info.result_table().keys())
            connection.insert_to_table(table_name, rt_columns, data)
            self.logger.add('Data upload', f'Upload data to result_table table', 'Success',
                            f'{len(data)} rows uploaded')
        except PsycopgError as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to result_table table', 'Error', error_message)
        except Exception as e:
            error_message = str(e)[-255:]
            self.logger.add('Data upload', f'Upload data to result_table table', 'Error', error_message)
        # -----/


        dicInfo = connection.get_table_info(table_name)
        connection.add_to_action_log(table_name, 'data_calculations', dicInfo['total_rows'], 'Calculated')
        connection.close_connection()