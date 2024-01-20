import requests
import time
from logToDb import DBlogger

class Gecko:
    url_futures = 'https://api.coingecko.com/api/v3/derivatives/exchanges/'
    url_exchanges = 'https://api.coingecko.com/api/v3/derivatives/exchanges/?include_tickers=all'

    def __init__(self, logger: DBlogger):
        self.exchanges = {}             # Key: Exchange name, Value: futures_id
        self.exchange_pairs = {}        # Key: Exchange_id + symbol, Value: exchange_pair obj
        self.successful_fetch: bool

        self.logger = logger

    def get_exchanges(self, top: int = 20):
        response = requests.get(self.url_exchanges)

        if response.status_code == 200:

            data = response.json()

            i = 0   # Reset
            for exchange_data in data:
                i += 1
                if i > top:
                    break

                name = exchange_data['name']
                futures_id = exchange_data['id']
                self.exchanges[name] = futures_id

            self.logger.add('Data upload', 'Get Exchanges list from Gecko API', 'Success',
                            f'{len(self.exchanges)} fetched with limit set to {top}')

        else:
            print(f"Could not access the API point for Exchanges, sucks!")
            self.logger.add('Data upload', 'Get Exchanges list from Gecko API', 'Error',
            'Could not access the API point')
            self.logger.exit_code_run_due_to_error('Could not access the Gecko API point for Exchanges')

    def get_futures_data(self):
        # We exclude anything with multiplier (like 1000PEPE)
        # We take data only for items that have non-zero funding rate
        # We take data only for items that have volume above 10K
        # We take only items that have USD denomination
        # Take out extreme outliers (more than 10%)

        for exchange_id in self.exchanges.values():

            url = f'{self.url_futures}{exchange_id}?include_tickers=all'
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()

                tickers = data['tickers']

                try:
                    for ticker in tickers:
                        if ticker['base'][:2] != '10' and ticker['funding_rate'] != 0 and 'USD' in ticker['target']\
                                and abs(ticker['funding_rate']) < 10 and ticker['contract_type'] == 'perpetual':

                            exchange_pair = Exchange_pair(exchange_id,ticker)
                            self.exchange_pairs[f'{exchange_id}_{ticker["symbol"]}'] = exchange_pair

                            self.logger.counter +=1

                    if len(self.exchanges) > 3:
                        time.sleep(20)

                    self.logger.add('Data upload', f'Fetching data for {exchange_id}', 'Success',
                                    f'{self.logger.counter} pairs')
                    self.logger.counter = 0 # Reset
                except Exception as e:
                    error_message = str(e)[-255:]
                    self.logger.add('Data upload', f'Fetching data for {exchange_id}', 'Error',
                                    f'{error_message}')

            else:
                self.logger.add('Data upload', f'Fetching data for {exchange_id}', 'Error',
                                f'Could not access API point')
                return  # Not fatal error, just missing some data


class Exchange_pair:

    def __init__(self, exchange_id, ticker):
        self.exchange_id: str = exchange_id
        self.base: str = ticker['base']
        self.symbol: str = ticker['symbol']
        self.target: str = ticker['target']
        self.funding_rate: float = ticker['funding_rate']
        self.open_interest: float = round(ticker['open_interest_usd'],0)
        self.volume_usd: float = round(float(ticker['converted_volume']['usd']),0)
        self.spread: float = ticker['bid_ask_spread']
