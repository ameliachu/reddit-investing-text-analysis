# -*- coding: utf -*-
"""
This script retreives the GME stock prices
for the past day (15-min intervals) using Alpha Vantage.

$ python3 get_daily_prices.py api_key

"""

import sys
import json
import requests


def main(api_key, symbol="GME"):
    """Obtain the GME prices in 15 min intervals from Alpha Vantage."""
    url = "https://alpha-vantage.p.rapidapi.com/query"

    querystring = {
        "interval": "15min",
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "datatype": "json",
        "output_size": "compact"
        }

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "alpha-vantage.p.rapidapi.com"
        }

    response = requests.request("GET", url,
                                headers=headers,
                                params=querystring)

    # print(response.text)

    gme_prices = response.json()

    symbol = gme_prices["Meta Data"]["2. Symbol"]
    date_info = gme_prices["Meta Data"]["3. Last Refreshed"]\
        .replace(" ", "_")\
        .replace(":", "-")

    file_name = f"data/alpha_vantage/{symbol}_{date_info}.json"
    with open(file_name, "w+") as f:
        json.dump(gme_prices, f)


if __name__ == "__main__":
    API_KEY = sys.argv[1]
    if len(sys.argv) > 2:
        symbol = sys.argv[2]
        main(API_KEY, symbol)
    else:
        main(API_KEY)
