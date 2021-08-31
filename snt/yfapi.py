import requests
import os
import psycopg2
from psycopg2.extensions import AsIs

yahoo_token = os.environ.get("YAHOO_TOKEN")

def get_current_tickers():
    sql = '''
    with sent_group as (
    select distinct sent_time from snt.rules order by sent_time desc limit 1
    )
    select r.match_value from snt.rules r 
        inner join sent_group sg on sg.sent_time = r.sent_time;
    '''

    with psycopg2.connect("host=100.100.100.42 dbname=datascience user=roman") as pg_con:
        with pg_con.cursor() as cursor:
            cursor.execute(sql)
            r = cursor.fetchall()
            # Convert a list of tuples into a list of strings
            symbols = [x[0] for x in r]

    return symbols


def get_yf_tickers(symbols, token):
    payload = {
        'region': 'US',
        'lang': 'en',
        'symbols': symbols
    }
    get_headers = {
        'X-API-KEY': token,
        'accept': 'application\json'
    }
    yahoo_api_url = 'https://yfapi.net/v6/finance/quote'
    response = requests.get(yahoo_api_url, params=payload, headers=get_headers)

    return response


def write_to_db(yf_response):
    insert_statement = '''
    insert into snt.quotes (%s) values %s
    '''

    with psycopg2.connect("host=100.100.100.42 dbname=datascience user=roman") as pg_con:
        with pg_con.cursor() as cursor:
            for quote in yf_response.json()['quoteResponse']['result']:
                columns = quote.keys()
                values = [quote[column] for column in columns]

                #c = cursor.mogrify(insert_statement, (AsIs(','.join(columns)), tuple(values)))
                cursor.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))


def main():
    tickers = get_current_tickers()
    yf_response = get_yf_tickers(symbols=tickers, token=yahoo_token)
    write_to_db(yf_response)



if __name__ == "__main__":
    main()
