import pandas_datareader.data as web
import pandas as pd
import datetime as dt
import json
import pytz

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

firebase_admin.initialize_app()
db = firestore.client()

indices = [
    {'col_name': 'Japan',    'doc_id': 'Nikkei_225',     'symbol': '^NKX',     'country': 'Japan',         'display_name': '日経平均',      'source': 'stooq'},
    {'col_name': 'Japan',    'doc_id': 'TOPIX',          'symbol': '^TPX',     'country': 'Japan',         'display_name': 'TOPIX',         'source': 'stooq'},
    {'col_name': 'NY',       'doc_id': 'S&P_500',        'symbol': '^SPX',     'country': 'United States', 'display_name': 'S&P 500',       'source': 'stooq'},
    {'col_name': 'NY',       'doc_id': 'Nasdaq',         'symbol': '^NDQ',     'country': 'United States', 'display_name': 'NASDAQ',        'source': 'stooq'},
    {'col_name': 'NY',       'doc_id': 'Dow_Jones',      'symbol': '^DJC',     'country': 'United States', 'display_name': 'NY DOW',        'source': 'stooq'},
    {'col_name': 'Asia',     'doc_id': 'Shanghai',       'symbol': '^SHC',     'country': 'China',         'display_name': '上海',          'source': 'stooq'},
    {'col_name': 'Asia',     'doc_id': 'Hangseng',       'symbol': '^HSI',     'country': 'China',         'display_name': '香港ハンセン',  'source': 'stooq'},
    {'col_name': 'Asia',     'doc_id': 'TAIEX',          'symbol': '^TWSE',    'country': 'Taiwan',        'display_name': '加権指数',      'source': 'stooq'},
    {'col_name': 'Asia',     'doc_id': 'KOSPI',          'symbol': '^KOSPI',   'country': 'South Korea',   'display_name': '韓国総合',      'source': 'stooq'},
    {'col_name': 'Asia',     'doc_id': 'Straits_Times',  'symbol': '^STI',     'country': 'Singapore',     'display_name': 'Straits Times', 'source': 'stooq'},
    {'col_name': 'Exchange', 'doc_id': 'EUR_USD',        'symbol': 'EURUSD=X', 'country': 'Exchange',      'display_name': 'ユーロ_ドル',   'source': 'yahoo'},
    {'col_name': 'Exchange', 'doc_id': 'USD_JPY',        'symbol': 'USDJPY=X', 'country': 'Exchange',      'display_name': 'ドル_円',       'source': 'yahoo'},
    {'col_name': 'Exchange', 'doc_id': 'EUR_JPY',        'symbol': 'EURJPY=X', 'country': 'Exchange',      'display_name': 'ユーロ_円',     'source': 'yahoo'},
    {'col_name': 'Exchange', 'doc_id': 'USD_SGD',        'symbol': 'USDSGD=X', 'country': 'Exchange',      'display_name': 'ドル_シンガポールドル',   'source': 'yahoo'},
    {'col_name': 'Exchange', 'doc_id': 'USD_CNY',        'symbol': 'USDCNY=X', 'country': 'Exchange',      'display_name': 'ドル_人民元',   'source': 'yahoo'},
    {'col_name': 'Exchange', 'doc_id': 'USD_HKD',        'symbol': 'USDHKD=X', 'country': 'Exchange',      'display_name': 'ドル_香港ドル', 'source': 'yahoo'},
    {'col_name': 'Exchange', 'doc_id': 'USD_TWD',        'symbol': 'USDTWD=X', 'country': 'Exchange',      'display_name': 'ドル_台湾ドル', 'source': 'yahoo'},
    {'col_name': 'Exchange', 'doc_id': 'USD_KRW',        'symbol': 'USDKRW=X', 'country': 'Exchange',      'display_name': 'ドル_ウォン',   'source': 'yahoo'},
]

st = dt.date.today() - dt.timedelta(days=2)
ed = dt.date.today() + dt.timedelta(days=1)

def get_filtered_indices(source):
    return [index for index in indices if index['source'] == source]

def get_symbol_list(source):
    return [index['symbol'] for index in indices if index['source'] == source]

def gen_columns(source):
    if source == 'stooq':
        return ['date', 'open', 'high', 'low', 'price', 'volume']
    if source == 'yahoo':
        return ['date', 'adj_close', 'open', 'high', 'low', 'price', 'volume']

def get_indices_historical_data(source):

    df = web.DataReader(get_symbol_list(source), source, st, ed)
    df = df.sort_index()
    df = df.swaplevel(0,1,axis=1)
    now = dt.datetime.now(pytz.timezone('Asia/Tokyo'))

    l = []
    for index in get_filtered_indices(source):
        symbol = index['symbol']
        df_one = df[symbol]
        df_one = df_one.dropna()
        df_one = df_one.rename_axis('Date').reset_index().astype(str)
        
        df_one.columns   = gen_columns(source)
        df_one['change'] = '0'
        df_one['year']   = now.year
        df_one['month']  = now.month
        df_one['day']    = now.day
        df_one['hour']   = now.hour
        df_one['minute'] = now.minute
        df_one['registered_at'] = now.strftime('%Y-%m-%d %H:%M')
        j = json.loads(df_one.to_json(orient='records'))

        obj = {
            'status': 'success',
            'code': index['symbol'],
            'name': index['doc_id'],
            'country': index['country'].replace(' ', '_'),
            'col_name': index['col_name'],
            'display_name': index['display_name'],
            'data': j
        }

        l.append(obj)
        print("get data:", obj['status'], obj['display_name'])
    return l


# firestoreのstocksコレクションに株価情報を保存
def set_firestore(obj):
    print('start set forestore')
    if obj['status'] == 'error': 
        return
    print(obj['name'], ': adding ...')

    # sampleにdata保存
    set_historical(obj)
    
    # dailyにdata保存
    # set_realtime(obj)


# stooqはリアルタイムデータないから使えないけど一応
def set_realtime(obj):
    col_name = 'indices'
    doc_id   = 'daily'

    doc_ref = db.collection(col_name).document(doc_id).collection(obj['col_name']).document(obj['name'])
    doc = doc_ref.get()

    if doc.exists:
        doc_ref.update({ u'data': firestore.ArrayUnion([obj['data']]) })
        print('set firestore indices-daily: ', obj['data'])
    else:
        doc_ref.set({
            u'data': obj['data'],
            u'name': obj['name'],
            u'code': obj['code'],
        }, merge=True)
        print('error : document does not exist', col_name,'/', doc_id,'/', obj['col_name'],'/', obj['name'])


def set_historical(obj):
    print('start set_historical', obj['name'])
    col_name = 'indices'
    doc_id = 'sample'
    data = list(map(lambda x: {
        u'date': x['date'],
        u'open': x['open'],
        u'high': x['high'],
        u'low': x['low'],
        u'price': x['price'],
        u'volume': x['volume'],
        u'change': x['change'],
        u'year': int(x['date'].split('-')[0]),
        u'month':  int(x['date'].split('-')[1]),
        u'day':  int(x['date'].split('-')[2]),
    } , obj['data']))

    doc_ref = db.collection(col_name).document(doc_id).collection(obj['col_name']).document(obj['name'])
    doc = doc_ref.get()

    if doc.exists:
        original = doc.to_dict()['data']

        if original[-1]['date'] == data[0]['date']:
            original.pop(-1)

        data = original + data
        doc_ref.set({u'data': data}, merge=True)
        print('historical data updated', obj['name'], obj['code'])
    else:
        doc_ref.set({
            u'code': obj['code'],
            u'name': obj['name'],
            u'country': obj['country'],
            u'display_name': obj['display_name'],
            u'data': data,
        }, merge=True)
        print('we do not have historical data so add: ', obj['name'], obj['code'], col_name,'/', doc_id,'/', obj['col_name'],'/', obj['name'])


# if __name__ == '__main__':
def collect_indices_data(event, context):
    l = get_indices_historical_data('stooq')
    m = get_indices_historical_data('yahoo')
    print('finish getting data')
    l.extend(m)
    print('extended')
    print(l)
    print(l[0])
    for obj in l:
        print('start for sentence',obj['name'])
        start = dt.datetime.now()
        set_firestore(obj)
        end = dt.datetime.now()
        print('time: ', end - start)
        

