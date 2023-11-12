from funding_arbitrage import Funding_Arbitrage, Pair,Exchange



cex_list = ['bybit']


FA = Funding_Arbitrage()
FA.get_pair_data_from_cex(cex_list)


for pair in FA.pairs.values():
    print(f'{pair.lowest.name} -- {pair.lowest.funding_rate} ')
