import time

from config import *
import pandas as pd
import datetime as dt
import sys
sys.path.append(r'C:\Users\nbrock.HAYDEN-HOMES\Hayden Homes\Back Office - Database\ETL Script')
import SnowflakeConnect as connect


def clean(min_date, min_sales):
    # --------------- target
    df = pd.read_csv(r'JobCostSummary.csv', encoding_errors='ignore')
    # status filters
    df = df[df['Status'] != 'Cancelled']
    df = df[df['Type'].str.contains('Residential|Regular', na=False)]
    df['Closed Date'] = pd.to_datetime(df['Closed Date'])
    # date filters
    mindate, maxdate = dt.datetime.strptime(min_date, '%Y-%m-%d'), max(df['Closed Date']).date()
    df = df[df['Closed Date'] >= mindate]
    print('sales from', mindate, 'to', maxdate)
    # cleanup
    df.columns = [c.lower() for c in df.columns]
    df['city'] = df['city'].str.strip()
    # cleaning and aggregating names to nearby areas
    df = pd.merge(df, pd.read_csv('lu_NearbyAreas.csv'), on=['city', 'state'], how='left')
    print(len(df['city_alt'].dropna()), 'records updated city < 40min commute')
    df['city_alt'] = df['city_alt'].fillna(df['city'])
    df.drop('city', axis=1, inplace=True)
    df.rename(columns={'city_alt': 'city'}, inplace=True)

    df = df.groupby(['city', 'state']).size().reset_index(name='sales')

    # regression target
    df = df[df['sales'] >= min_sales]
    # classification label
    # df['performer'] = (df['sales'] >= 20).astype(int)

    # --------------- features - snowflake internal + marketplace (enrich)
    conn = connect.connect()
    with open('query.sql') as f:
        q = f.read()
    with conn, conn.cursor() as curs:
        curs.execute(q)
        data = curs.fetchall()
        headers = [desc[0].lower() for desc in curs.description]
    X = pd.DataFrame(data, columns=headers)

    # --------------- merge
    df = pd.merge(df, X, on=['city', 'state'], how='outer')

    # --------------- permits
    # assume if no permit data is available, no permits pulled
    for permit_col in ['totalpermits', 'percprodbuilder']:
        df[permit_col] = df[permit_col].fillna(0)

    # --------------- hayden homes presence
    f = pd.read_csv(r'hhpresence.csv')
    f.columns = ['state', 'county', 'city', 'hhpresence']
    df = pd.merge(df, f, on=['city', 'county', 'state'], how='left')
    # if not in HHPresence Table, no HH presence
    df['hhpresence'] = df['hhpresence'].fillna(0)

    # bend - capture market competition history/reputation
    df['bend'] = (df['city'].str.lower() == 'bend').astype(int)

    # --------------- DROP NULL & DUPES
    before = len(df.index)
    df = df.dropna()
    print(f'dropNA {before / len(df.index):.4f}%')

    before = (len(df.index))
    df = df.drop_duplicates(subset=['geoid'])
    print(f'dedupe {before / len(df.index):.4f}%')

    print('totalcities', len(df.index))

    # --------------- DATAFRAME PREP
    for c in [d for d in df.columns if d not in BASE_COLS]:
        df[c] = df[c].astype(float)
    df = df.drop(columns=['city', 'county', 'state'])
    df.to_csv('clean_data.csv', index=False)
    df.set_index('geoid', drop=True, inplace=True)
    return df


def read(enriched=False):
    if enriched:
        df = pd.read_csv('enriched_data.csv', index_col='geoid')
    else:
        df = pd.read_csv('clean_data.csv', index_col='geoid')
    df.index.astype(str)
    return df


def enrich(base_data):
    start = time.time()
    print('processing data enrichment...')

    # conn = connect.connect()
    # with open('enrich.sql') as f:
    #     q = f.read()
    # with conn, conn.cursor() as curs:
    #     curs.execute(q)
    #     querydata = curs.fetchall()
    #     headers = [desc[0].lower() for desc in curs.description]
    # addition = pd.DataFrame(querydata, columns=headers)
    # addition.set_index('geoid', drop=True, inplace=True)

    addition = pd.read_csv('enrich_query.csv')
    addition.columns = [c.lower() for c in addition.columns[:]]
    addition.drop(columns=['city', 'county', 'state'], inplace=True)

    before = len(addition.index)
    addition = addition.dropna()
    print(f'dropNA {(before / len(addition.index)):.4}%')

    onehot = pd.get_dummies(addition)
    addition = onehot.groupby('geoid').mean()

    addition.index = addition.index.astype(str)
    base_data.index = base_data.index.astype(str)
    enriched = pd.merge(base_data, addition, left_index=True, right_index=True, how='inner')

    enriched.to_csv('enriched_data.csv')
    print(f'complete! {((time.time() - start)/60):.4f}min')
    return enriched


if __name__ == '__main__':
    # df = clean(MINDATE, MINSALES)
    # print(df.head(n=50).to_string())

    # print(enrich(read()).head(10))
    print(read())