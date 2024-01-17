from fundingRateData import Funding_Rates, Exchange_pair

# Kucoin should be last as we need to take data one coin at a time. Should filter by existing pairs in pairs list.
cex_list = ['kucoin']


FRD = Funding_Rates()
FRD.get_pair_data_from_cex(cex_list)


for ep in FRD.exchange_pairs.values():
    print(f'{ep.name} -- {ep.symbol} -- {ep.funding_rate} -- {ep.volume} -- {ep.next_funding_unix}')