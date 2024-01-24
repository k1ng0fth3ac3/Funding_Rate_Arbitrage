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
        connection = Connection()
        data = connection.select_table_data(table_name='calc_table',columns='*')

        columns = list(self.tables_info.calc_table().keys())
        df = pd.DataFrame(data, columns=columns)

        for index, row in df.iterrows():
            if row['avg_cycle_3'] is not None and abs(row['avg_cycle_3']) >= self.min_avg_3:

                if abs(row['funding_rate']) >= self.min_fr_delta:
                    arbitrage = Arbitrage(abs(row['funding_rate']),exchange_1_data=row)          # Good enough for spot
                    self.arbitrage.append(arbitrage)


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
            self.exchange_2_data = exchange_1_data




