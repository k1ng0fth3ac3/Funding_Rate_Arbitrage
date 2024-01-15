from keys import Kucoin_keys,Bybit_keys
import ccxt
import requests




class Funding_Rates:

    def __init__(self):
        self.exchange_pairs = {}                 # Key: exchange_pair / exchange_pair Obj


    def get_pair_data_from_cex(self, cex_list: list):

        for cex in cex_list:
            if cex.lower() == 'kucoin':
                tickers = self.data_kucoin()

            elif cex.lower() == 'bybit':
                tickers = self.data_bybit()

            self.process_cex_tickers(cex_name=cex.lower(), tickers=tickers)


    def data_kucoin(self):
        base_url = "https://api-futures.kucoin.com/api/v1/contracts/"
        endpoint = base_url + 'active'
        response = requests.get(endpoint)
        data = response.json()

        tickers = {}
        pairs_data = data['data']
        for pair_data in pairs_data:
            symbol = pair_data['symbol']

            tickers[symbol] = pair_data

        return tickers

    def data_bybit(self):
        bybit = ccxt.bybit({
            'enableRateLimit': True,
            'apiKey': Bybit_keys.api,
            'secret': Bybit_keys.secret
        })

        return bybit.fetch_tickers()


    def process_cex_tickers(self, cex_name, tickers):

        for symbol, ticker in tickers.items():
            if 'USD' in symbol:
                quote_coin = 'USD'      # Convert all stables to common 'USD' instead of (USDC, USDT, TUSD, HUSD,etc.)

            if cex_name == 'kucoin':
                base_coin = ticker['baseCurrency']

            elif cex_name == 'bybit':
                base_coin = symbol.split('/')[0]
                if quote_coin != 'USD':
                    quote_coin = symbol.split('/')[1].split(':')[0]


            pair_name = base_coin + '/' + quote_coin

            exchange_pair = Exchange_pair(cex_name=cex_name,ticker=ticker)
            self.exchange_pairs[f'{cex_name}_{pair_name}'] = exchange_pair




class Exchange_pair:

    def __init__(self, cex_name, ticker):
        self.name = cex_name
        self.symbol: str

        self.price: float
        self.low: float
        self.high: float
        self.bid: float
        self.ask: float
        self.volume: float

        self.mark_price: float

        self.funding_rate: float
        self.open_interest: float
        self.next_funding_unix: float


        self.update_pair_data(ticker)

    def update_pair_data(self, ticker):

        if self.name == 'bybit':
            self.symbol = ticker['symbol']
            self.price = float(ticker['info']['lastPrice'])
            self.low = float(ticker['low'])
            self.high = float(ticker['high'])
            self.bid = float(ticker['bid'])
            self.ask = float(ticker['ask'])
            self.volume = float(ticker['baseVolume'])
            self.mark_price = float(ticker['info']['markPrice'])
            self.funding_rate = float(ticker['info']['fundingRate'])
            self.open_interest = float(ticker['info']['openInterest'])
            self.next_funding_unix = float(ticker['info']['nextFundingTime'])
        elif self.name == 'kucoin':
            self.symbol = ticker['symbol']
            self.price = float(ticker['markPrice'])
            self.volume = float(ticker['volumeOf24h'])
            self.mark_price = float(ticker['markPrice'])
            self.funding_rate = float(ticker['fundingFeeRate']) if ticker['fundingFeeRate'] is not None else None
            self.open_interest = float(ticker['openInterest']) if ticker['openInterest'] is not None else None
            self.next_funding_unix = float(ticker['nextFundingRateTime']) if ticker['nextFundingRateTime'] is not None else None
