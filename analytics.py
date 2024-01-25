from dbManager import Connection
from dbTables import Tables_info
import pandas as pd


class Analyze:


    def __init__(self,incl_spot: bool = True, min_fr_delta: float = 0.05, min_avg_3: float = 0.01,
                 min_avg_6: float = None, min_avg_9: float = None):
        self.tables_info = Tables_info()
        self.arbitrage = []

        self.incl_spot = incl_spot
        self.min_fr_delta = min_fr_delta
        self.min_avg_3 = min_avg_3
        self.min_avg_6 = min_avg_6
        self.min_avg_9 = min_avg_9

    def get_data(self):
        connection = Connection(remote_server=False)
        data = connection.select_table_data(table_name='calc_table',columns='*')

        columns = list(self.tables_info.calc_table().keys())
        df = pd.DataFrame(data, columns=columns)

        dicExchangesSymbolsFound = {}           # Keep track of the exchanges and symbols already matched


        for index, row in df.iterrows():
            avg_cycle_3 = row['avg_cycle_3']
            avg_cycle_6 = row['avg_cycle_6']
            avg_cycle_9 = row['avg_cycle_9']

            if ((avg_cycle_3 is not None and abs(avg_cycle_3) >= self.min_avg_3) and
                (avg_cycle_6 is None or self.min_avg_6 is None or abs(avg_cycle_6) >= self.min_avg_6) and
                (avg_cycle_9 is None or self.min_avg_9 is None or abs(avg_cycle_9) >= self.min_avg_9)):

                    # Check if the funding rate is positive and above theshold --> Spot
                    if row['funding_rate'] >= self.min_fr_delta:
                        arbitrage = Arbitrage(abs(row['funding_rate']),exchange_1_data=row)
                        self.arbitrage.append(arbitrage)

                    # Find exchange pair
                    for index2, row2 in df.iterrows():

                        if row2['base'] == row['base']:

                            if f'{row2["base"]}_{row["exchange_id"]}_{row2["exchange_id"]}' not in dicExchangesSymbolsFound\
                                    and row2['exchange_id'] != row['exchange_id']:

                                # ---- Calculate Delta
                                fr_1 = row["funding_rate"]
                                fr_2 = row2["funding_rate"]

                                if fr_1 > 0 and fr_2 < 0:
                                    fr_delta = abs(fr_1) + abs(fr_2)
                                elif fr_1 < 0 and fr_2 > 0:
                                    fr_delta = abs(fr_1) + abs(fr_2)
                                else:
                                    fr_delta = 0    # Lets ignore this, better to have just Spot than two positives or negatives
                                # -----/

                                # ----- Check if passes min delta and add to the arbitrage obj list
                                if fr_delta > self.min_fr_delta:
                                    dicExchangesSymbolsFound[f'{row2["base"]}_{row["exchange_id"]}_{row2["exchange_id"]}'] = 1
                                    dicExchangesSymbolsFound[f'{row2["base"]}_{row2["exchange_id"]}_{row["exchange_id"]}'] = 1

                                    arbitrage = Arbitrage(fr_delta, exchange_1_data=row,exchange_2_data=row2)
                                    self.arbitrage.append(arbitrage)
                                # -----/

            if len(self.arbitrage) > 0:
                self.arbitrage = sorted(self.arbitrage, key=lambda arb: arb.delta, reverse=True)


class Arbitrage:

    # Maybe also important to calculate the AVG APR and delta for different periods

    def __init__(self, fr_delta: float, exchange_1_data, exchange_2_data = None):
        self.delta = fr_delta
        self.apr = fr_delta * 3 * 365
        self.coin = exchange_1_data['base']

        self.exchange_1: str = exchange_1_data['exchange_id']
        self.exchange_1_data = exchange_1_data

        if exchange_2_data is None:
            self.exchange_2: str = 'SPOT'
            self.exchange_2_data = None
        else:
            self.exchange_2: str = exchange_2_data['exchange_id']
            self.exchange_2_data = exchange_2_data

        self.delta_averages = {}
        self.apr_averages = {}
