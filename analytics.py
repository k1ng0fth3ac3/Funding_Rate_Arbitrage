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


        self.ranked = {}            # Key: ID from result_table, Value: Dictionary

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


    def rank(self):
        # Volume of both exchanges above 500K = full points (for both exchanges)
        # OI of both exchanges above 500K = full points (fot both exchanges)
        # Compare spread to delta and give points based on how many cycles does it take to break even on that loss.
        # Max act_delta capped at 2 = full points, otherwise take the highest act_delta and that's full points.

        # Invent a way to get the price data through APIs, perhaps Gecko...or then need to get it through multiple exchanges

        # ----- SETTINGS
        vol_weight = 0.075
        oi_weight = 0.075
        spread_weight = 0.15
        act_fr = 0.3
        stability_fr = 0.3
        price_stability = 0.1
        # -----/


        connection = Connection(remote_server=False)
        data = connection.select_table_data(table_name='result_table', columns='*')

        columns = list(self.tables_info.result_table().keys())
        df = pd.DataFrame(data, columns=columns)


        for index, row in df.iterrows():
            ID = row['id']
            self.ranked[ID] = {}
            self.ranked[ID]['score'] = 0

            vol_score = 0
            oi_score = 0
            spread_score = 0
            act_fr_score = 0
            stability_fr_score = 0
            price_score = 0


            ex_2 = row['exchange_id_2']

            volume_1 = row['volume_1']
            oi_1 = row['open_interest_1']
            spread_1 = row['spread_1']
            volume_2 = row['volume_2']
            oi_2 = row['open_interest_2']
            spread_2 = row['spread_2']

            delta = row['delta']
            fr_1 = row['funding_rate_1']
            fr_2 = row['funding_rate_2']

            # -------------------- VOLUME
            vol_score = 5 if volume_1 >= 500000 else (volume_1 / 500000) * 5
            if ex_2 == 'SPOT':
                vol_score *= 2
            else:
                vol_score += 5 if volume_2 >= 500000 else (volume_2 / 500000) * 5
            # --------------------/
            # -------------------- OI
            oi_score = 5 if oi_1 >= 500000 else (oi_1 / 500000) * 5
            if ex_2 == 'SPOT':
                oi_score *= 2
            else:
                oi_score += 5 if oi_2 >= 500000 else (oi_2 / 500000) * 5
            # --------------------/
            # -------------------- SPREAD
            # How many cycles does it take break even against lost funds on spread?
            if ex_2 == 'SPOT':
                total_spread = (spread_1 * 100) / abs(fr_1)
            else:
                total_spread = (spread_1 * 100) / abs(fr_1) + (spread_2 * 100) / abs(fr_2)

            delta_to_spread = total_spread / delta
            spread_score = 10 - delta_to_spread     # Score can be severely negative
            # --------------------/

            # -------------------- ACT FR
            act_fr_score = delta * 10 if delta <= 1 else 10
            # --------------------/

            # -------------------- FR STABILITY
            # Get funding_rate data and iterate through it (max 30 days) and get stability score
            # Need to ipmort Numpy (and don't forget to install it on the Linux server as well)

            where_clause = """
                            exchange_id = %s
                        AND  
                            """
            fr_data = connection.select_table_data(table_name='funding_rates',columns='funding_rate',
                                                   where_clause=where_clause,params=())
            # --------------------/

class Arbitrage:

    # Maybe also important to calculate the AVG APR and delta for different periods

    def __init__(self, fr_delta: float, exchange_1_data, exchange_2_data = None):
        self.delta = fr_delta
        self.apr = fr_delta * 3 * 365
        self.coin = exchange_1_data['base']

        self.exchange_1: str = exchange_1_data['exchange_id']
        self.exchange_1_data = exchange_1_data

        self.delta_avg = {}

        if exchange_2_data is None:
            self.exchange_2: str = 'SPOT'
            self.exchange_2_data = None

            for i in range(3,24,3):
                if exchange_1_data[f'avg_cycle_{i}'] is not None:
                    self.delta_avg[i] = abs(exchange_1_data[f'avg_cycle_{i}'])
                else:
                    self.delta_avg[i] = None
        else:
            self.exchange_2: str = exchange_2_data['exchange_id']
            self.exchange_2_data = exchange_2_data

            for i in range(3,24,3):
                if exchange_1_data[f'avg_cycle_{i}'] is not None and exchange_2_data[f'avg_cycle_{i}'] is not None:
                    self.delta_avg[i] = abs(exchange_1_data[f'avg_cycle_{i}']) + abs(exchange_2_data[f'avg_cycle_{i}'])
                else:
                    self.delta_avg[i] = None


        self.delta_averages = {}
        self.apr_averages = {}

