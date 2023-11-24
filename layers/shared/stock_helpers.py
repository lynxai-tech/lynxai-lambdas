import requests

async def get_stock_value(symbol):
    try:
        response = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}")
        data = response.json()
        price = data['chart']['result'][0]['meta']['regularMarketPrice']
        print(price)
        return price
    except requests.HTTPError as error:
        if error.response.status_code == 404:
            return None
        print('Error retrieving stock price:', error)
        raise error


async def convert_amount_from_usd_to_eur(amount):
    try:
        response = requests.get(f"http://api.exchangerate.host/latest?access_key={process.env.EXCHANGE_RATE_ACCESS_KEY}")
        print(response)
        exchange_rate_response = response.json()
        list_of_rates = exchange_rate_response.get('rates', {})
        
        if 'USD' in list_of_rates:
            rate = list_of_rates['USD']
            return amount / rate
    except requests.HTTPError as error:
        raise ValueError(f"Currency conversion failed: {error}")
    
    
async def convert_stock_to_eur(asset):
    if 'ticker' in asset:
        stock_value = await get_stock_value(asset['ticker'])
        return await convert_amount_from_usd_to_eur(stock_value * asset['share_amount'])
    return asset.get('nominal_amount')