from keys import Kucoin_keys,Bybit_keys
import ccxt
import requests



class Funding_Arbitrage:

    def __init__(self):
        self.pairs = {}                 # Key: pair / pair Obj
        self.exchanges = {}             # Key: Exchange / Exchange Obj


    def get_pair_data_from_cex(self, cex_list: list, min_volume=10000, min_abs_fund_rate=0.0005):

        for cex in cex_list:
            if cex.lower() == 'kucoin':
                kucoin = ccxt.kucoin({
                    'enableRateLimit': True,
                    'apiKey': Kucoin_keys.api,
                    'secret': Kucoin_keys.secret,
                    'password': Kucoin_keys.password
                })
                tickers = kucoin.fetch_tickers()
            elif cex.lower() == 'bybit':
                bybit = ccxt.bybit({
                    'enableRateLimit': True,
                    'apiKey': Bybit_keys.api,
                    'secret': Bybit_keys.secret
                })

                tickers = bybit.fetch_tickers()

            self.process_cex_tickers(cex_name=cex.lower(), tickers=tickers,
                                     min_volume=min_volume,min_abs_fund_rate=min_abs_fund_rate)

    def process_cex_tickers(self, cex_name, tickers, min_volume, min_abs_fund_rate):

        for symbol, ticker in tickers.items():
            if 'USD' in symbol:
                quote_coin = 'USD'      # Convert all stables to common 'USD' instead of (USDC, USDT, TUSD, HUSD,etc.)

            if cex_name == 'kucoin':
                pass
            elif cex_name == 'bybit':
                base_coin = symbol.split('/')[0]
                volume = ticker['quoteVolume']
                abs_fund_rate = abs(float(ticker['info']['fundingRate']))
                if quote_coin != 'USD':
                    quote_coin = symbol.split('/')[1].split(':')[0]


            pair_name = base_coin + '/' + quote_coin

            if (volume is None or volume >= min_volume) and abs_fund_rate >= min_abs_fund_rate:
                if not pair_name in self.pairs:
                    pair = Pair(pair_name)
                    pair.update_cex_data(cex_name, ticker)
                    self.pairs[pair_name] = pair
                else:
                    pair = self.pairs[pair_name]
                    pair.update_cex_data(cex_name, ticker)


class Pair:

    def __init__(self, pair_name):
        self.name = pair_name
        self.exchange_count = 0                 # Count of exchanges
        self.exchanges = {}                     # Key: exchange name / exchange object

        self.lowest: Exchange = None            # Lowest funding rate exchange object
        self.highest: Exchange = None           # Highest funding rate exchange object
        self.delta: float                       # Delta between highest and lowest rates


    def update_cex_data(self, cex_name, pair_ticker):
        if cex_name not in self.exchanges:
            self.exchange_count += 1
            cex = Exchange(cex_name)
            self.exchanges[cex_name] = cex
        else:
            cex = self.exchanges[cex_name]
        cex.update_pair_data(pair_ticker)

        if self.lowest is None or self.lowest.funding_rate > cex.funding_rate:
            self.lowest = cex
        if self.highest is None or self.highest.funding_rate < cex.funding_rate:
            self.highest = cex
        if self.lowest.funding_rate < 0:
            self.delta = self.highest.funding_rate + abs(self.lowest.funding_rate)
        else:
            self.delta = self.highest.funding_rate - self.lowest.funding_rate

class Exchange:

    def __init__(self, cex_name):
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