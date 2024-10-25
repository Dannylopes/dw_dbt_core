# import
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

commodities = ['AAPL34', 'MSFT34', 'TADAWUL', 'RACE', 'GC=F', 'CL=F']

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USERPROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://postgres123:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)


def get_commodities_data(codigo, periodo='5d', intervalo='1d'):
    # response = request.get('')
    ticker = yf.Ticker(codigo)
    dados = ticker.history(period = periodo, interval = intervalo)[['Close']] # Close é a cotação no fechamento
    dados['simbolo'] = codigo
    return dados

def get_all_commodities(commodities):
    all_data = []
    for codigo in commodities:
        dados = get_commodities_data(codigo)
        all_data.append(dados)
    return pd.concat(all_data)

def save_postgres(df, schema='public'):
    df.to_sql('tb_commodities', engine, if_exists='replace', index = True, index_label= 'Date', schema = schema)

if __name__ == "__main__":
    df_commodities = get_all_commodities(commodities)
    save_postgres(df_commodities, schema='public')




parei em 01h30