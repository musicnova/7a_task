import sys
from datetime import timedelta, datetime


from pyspark import HiveContext
from pyspark.sql import functions as f, SparkSession


def algo(src, from_dt, to_dt):
    res = steps(src, from_dt, to_dt)
    return res


def steps(src, from_dt, to_dt):
    #    Новые
    #    фичи
    #    Цифры
    #    по
    #    mcc
    #    Погода
    #    по
    #    месту
    #    расстояние
    #    до
    #    дальнейшего
    #    соседа
    #    максимальная
    #    продолжительность
    #    приобретений
    #    в
    #    данной
    #    точке
    #    по
    #    дням
    #
    #    ПРОССУМИРОВАТЬ
    #    ДЕЛЬТЫ
    #    ПО
    #    РАЗНЫМ
    #    КООРДИНАТАМ
    #
    #    [Boosters]
    #    Raiffeisen
    #    Data
    #    Cup.Baseline
    #    Общий
    #    подход:
    #
    #    Добавляем
    #    к
    #    каждой
    #    транзакции
    #    столбец: is_work(если
    #    транзакция
    #    находится
    #    в
    #    пределах
    #    0.02
    #    от
    #    дома
    #    клиента)
    #    Добавляем
    #    к
    #    каждой
    #    транзакции
    #    столбец: is_home(если
    #    транзакция
    #    находится
    #    в
    #    пределах
    #    0.02
    #    от
    #    работы
    #    клиента)
    #    Обучаем
    #    классификатор
    #    предсказывающий
    #    вероятность(is_home == 1)
    #    для
    #    транзакции
    #    Обучаем
    #    классификатор
    #    предсказывающий
    #    вероятность(is_work == 1)
    #    для
    #    транзакции
    #    Точность
    #    определения
    #    местоположения:
    #
    #    для
    #    классификатора
    #    is_home: ~3
    #    x %
    #    для
    #    классификатора
    #    is_work: ~2
    #    x %
    #    общая
    #    оценка
    #    на
    #    Public
    #    Leaderboard: ???
    #    Примечание
    #
    #    Требуется
    #    Python
    #    версии
    #    3.5
    #    Требуется
    #    библиотека
    #    xgboost(для
    #    обучения
    #    использовалась
    #    xgboost
    #    версии
    #    0.7.post3)
    #    Требуются
    #    файлы: test_set.csv, train_set.csv
    #    в
    #    одном
    #    каталоге
    #    с
    #    данным
    #    скриптом
    #    Требования
    #    к
    #    памяти: должно
    #    работать
    #    с
    #    2
    #    Гб
    #    свободного
    #    RAM
    #    Время
    #    работы: ~3
    #    минуты(тестировалось
    #    на
    #    процессоре
    #    Intel
    #    Core
    #    i7 - 4770)
    #
    #    % load_ext
    #    autoreload
    #    % autoreload
    #    2
    #    ​
    #    import sys
    #    MODULES_PATH = '../code/'
    #    if MODULES_PATH not in sys.path:
    #        sys.path.append(MODULES_PATH)
    #    import mfuncs
    #
    #    import pandas as pd
    #    import numpy as np
    #    from tqdm import tqdm
    #    tqdm.pandas()
    #    pd.options.display.max_columns = 1000
    #    pd.options.display.max_colwidth = -1
    #    ​
    #    import lightgbm as lgb
    #    ​
    #    ​
    #    from sklearn.neighbors import NearestNeighbors
    #    from sklearn.cluster import KMeans, MeanShift, estimate_bandwidth, AgglomerativeClustering
    #    from sklearn.metrics import silhouette_samples, silhouette_score
    #    ​
    #    from sklearn.metrics.pairwise import pairwise_distances
    #    import gmaps
    #    API_KEY = 'AIzaSyCG_RL0_kavuEaJAqEN5xXbU4h0VJUbA9M'
    #    gmaps.configure(api_key=API_KEY)  # Your Google API key
    #    % pylab
    #    inline
    #    figsize(13, 13)
    #    Populating
    #    the
    #    interactive
    #    namespace
    #    from numpy and matplotlib
    #
    #    # Определим типы колонок для экономии памяти
    #    dtypes = pd.read_csv('../data/df_all_b11_dtypes.csv', header=None, index_col=0).to_dict()[1]
    #    dtypes.pop('transaction_date', None)
    #    df_all = pd.read_csv('../data/df_all_b11.csv', dtype=dtypes, parse_dates=['transaction_date'])
    #    Мои
    #    фичи
    #
    #    # добавим признаки после групбая
    #    df_gb = df_all[['customer_id', 'amount', 'add_lat', 'add_lon']].groupby('customer_id')
    #    coord_stat_df = df_gb.agg(['mean', 'max', 'min'])
    #    coord_stat_df['transactions_per_user'] = df_gb.agg('size')
    #    coord_stat_df.columns = ['_'.join(col).strip() for col in coord_stat_df.columns.values]
    #    coord_stat_df = coord_stat_df.astype(np.float32)
    #    coord_stat_df.reset_index(inplace=True)
    #    df_all = pd.merge(df_all, coord_stat_df, on='customer_id', how='left')
    #
    #    cols = ['add_lat', 'add_lon']
    #    types = ['min', 'max', 'mean']
    #    for c in cols:
    #        for t in types:
    #            df_all['{}_diff_{}'.format(c, t)] = np.abs(df_all[c] - df_all['{}_{}'.format(c, t)], dtype=np.float32)
    #
    #    df_all = df_all.loc[:, ~df_all.columns.duplicated()]
    #
    #    # разности
    #    df_all['lat_diff_cluster_lat'] = np.abs(df_all['add_lat'] - df_all['cl_lat'], dtype=np.float32)
    #    df_all['lon_diff_cluster_lon'] = np.abs(df_all['add_lon'] - df_all['cl_lon'], dtype=np.float32)
    #    df_all['lon_diff_cluster'] = (df_all['lat_diff_cluster_lat'] + df_all['lon_diff_cluster_lon']).astype(np.float32)
    #    Категории
    #    mcc
    #
    #    # категории
    #    df_all['mcc_str'] = df_all['mcc'].astype(str).str.rjust(4, '0')
    #    df_mcc = pd.read_csv('../data/internal/mcc.csv')
    #    df_mcc = df_mcc.iloc[1:, :3]
    #    df_mcc.columns = ['mcc_str', 'mcc_cat1', 'mcc_cat2']
    #    df_mcc.drop_duplicates(subset=['mcc_str'], inplace=True)
    #    df_mcc['mcc_cat1'] = pd.factorize(df_mcc['mcc_cat1'])[0].astype(np.int32)
    #    df_mcc['mcc_cat2'] = pd.factorize(df_mcc['mcc_cat2'])[0].astype(np.int32)
    #    df_mcc.fillna('none', inplace=True)
    #    df_all = pd.merge(df_all, df_mcc, on='mcc_str', how='left')
    #    del df_all['mcc_str']
    #    df_mcc.head()
    #    mcc_str
    #    mcc_cat1
    #    mcc_cat2
    #    1
    #    0001 - 1
    #    0
    #    2
    #    0002 - 1
    #    0
    #    3
    #    0003 - 1
    #    0
    #    4
    #    0004 - 1
    #    0
    #    5
    #    0005 - 1
    #    0
    #    Плотности
    #    населения
    #    по
    #    районам
    #    МСК
    #
    #    import geopandas as gpd
    #    from shapely.geometry import Point, Polygon
    #    mos_shp = gpd.read_file('../data/internal/demography.shp')
    #    ​
    #    _pnts = [Point(vals.T) for vals in df_all[df_all.city_name == 'Москва'][['add_lon', 'add_lat']].values]
    #    pnts = gpd.GeoDataFrame(geometry=_pnts)
    #    pnts.crs = mos_shp.crs
    #    ​
    #    mos_shp.drop(['NAME', 'ABBREV_AO'], 1, inplace=True)
    #    mos_shp['area'] = mos_shp['geometry'].area
    #    for c in mos_shp.columns:
    #        if c not in ['geometry', 'area'] and 'index' not in c:
    #            mos_shp[c + 'dens'] = mos_shp[c] / mos_shp['area']
    #
    #    % % time
    #    cities_with_country = gpd.sjoin(pnts, mos_shp, how="left", op='intersects')
    #    CPU
    #    times: user
    #    44
    #    s, sys: 260
    #    ms, total: 44.3
    #    s
    #    Wall
    #    time: 44.3
    #    s
    #
    #    cols = cities_with_country.drop(['geometry', 'index_right'], 1).columns
    #    for c in cols:
    #        df_all[c] = -1
    #    df_all.loc[df_all.city_name == 'Москва', cols] = cities_with_country
    #
    #    # частота mcc
    #    df_mcc = df_all['mcc'].value_counts(normalize=True).reset_index()
    #    df_mcc.columns = ['mcc', 'mcc_freq']
    #    df_all = pd.merge(df_all, df_mcc, on='mcc', how='left')
    #
    #    # метро
    #    mos_metro = pd.read_csv('../data/internal/moscow_metro.csv')
    #    pet_metro = pd.read_csv('../data/internal/peter_metro.csv')
    #    df_metro = pd.concat([mos_metro, pet_metro])
    #    ​
    #    vals1 = df_all[['add_lat', 'add_lon']].values
    #    vals2 = df_metro[['metro_lat', 'metro_lon']].values
    #    X = pairwise_distances(vals1, vals2)
    #    dist_to_min_metro = X.min(axis=1)
    #    ​
    #    # X[X == 0] = 10000
    #    df_all['dist_to_minmetro'] = X.min(axis=1)
    #    df_all['metro_in_01'] = (X < 0.01).sum(axis=1)
    #    df_all['metro_in_001'] = (X < 0.001).sum(axis=1)
    #    df_all['metro_in_02'] = (X < 0.02).sum(axis=1)
    #    df_all['metro_in_005'] = (X < 0.005).sum(axis=1)
    #    df_all['metro_in_03'] = (X < 0.03).sum(axis=1)
    #
    #    # расстояние до участковых комиссий
    #    df_cik = pd.read_csv('../data/internal/cik_uik.csv')
    #    df_cik.dropna(subset=['lat_ik'], inplace=True)
    #    df_cik.dropna(subset=['lon_ik'], inplace=True)
    #    ​
    #    df_cik = df_cik[df_cik['lon_ik'] < 45]
    #    vals1 = df_all[['add_lat', 'add_lon']].drop_duplicates().values.astype(np.float32)
    #    df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #    vals2 = df_cik[['lat_ik', 'lon_ik']].drop_duplicates().values.astype(np.float32)
    #    ​
    #    print(vals2.shape)
    #    X = pairwise_distances(vals1, vals2)
    #    ​
    #    df_vals['dist_to_ciktro'] = X.min(axis=1)
    #    df_vals['cik_in_01'] = (X < 0.01).sum(axis=1)
    #    df_vals['cik_in_001'] = (X < 0.001).sum(axis=1)
    #    df_vals['cik_in_02'] = (X < 0.02).sum(axis=1)
    #    df_vals['cik_in_005'] = (X < 0.005).sum(axis=1)
    #    df_vals['cik_in_03'] = (X < 0.03).sum(axis=1)
    #    (37481, 2)
    #
    #    df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #    df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #    df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #    df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #    df_vals.drop_duplicates(subset=['add_lat_', 'add_lon_'], inplace=True)
    #    del df_vals['add_lat']
    #    del df_vals['add_lon']
    #    ​
    #    df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #    del X
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #
    #    # погода в МСК и ПИТЕРЕ
    #    # буду смотреть погоду в 18-00
    #    w1 = pd.read_csv('../data/internal/weather/moscow.csv', sep=';', index_col=False)
    #    w1['city_name'] = 'Москва'
    #    w1['transaction_date'] = pd.to_datetime(w1['Local time in Moscow'], format='%d.%m.%Y %H:%M')
    #    del w1['Local time in Moscow']
    #    w1 = w1[w1.transaction_date.dt.hour == 18].reset_index(drop=True)
    #    w1['transaction_date'] = w1['transaction_date'].dt.date
    #    ​
    #    w2 = pd.read_csv('../data/internal/weather/peter.csv', sep=';', index_col=False)
    #    w2['city_name'] = 'Санкт-Петербург'
    #    w2['transaction_date'] = pd.to_datetime(w2['Local time in Moscow'], format='%d.%m.%Y %H:%M')
    #    del w2['Local time in Moscow']
    #    w2 = w2[w2.transaction_date.dt.hour == 18].reset_index(drop=True)
    #    w2['transaction_date'] = w2['transaction_date'].dt.date
    #    ​
    #    df_weather = pd.concat([w1, w2], axis=0).reset_index(drop=True)
    #    df_weather['transaction_date'] = pd.to_datetime(df_weather['transaction_date'])
    #    ​
    #    cn = df_weather['city_name']  # hardcode
    #    df_weather = df_weather.select_dtypes(exclude=['object'])
    #    df_weather['city_name'] = cn
    #    for c in df_weather:
    #        if df_weather[c].isnull().mean() > 0.9:
    #            del df_weather[c]
    #    # df_weather = df_weather.add_prefix('weather_')
    #    df_all = pd.merge(df_all, df_weather, on=['city_name', 'transaction_date'], how='left')
    #
    #    # df_all.drop(['index', 'T', 'Po', 'P', 'Pa', 'U', 'Ff', 'VV', 'Td', 'tR'], 1, inplace=True)
    #
    #    # добавляем новые MCC OHE с самыми частыми категориями
    #    df_all['mcc_rm'] = df_all['mcc']
    #    df_all.loc[~df_all['mcc_rm'].isin(df_all['mcc_rm'].value_counts().iloc[:35].index.values), 'mcc_rm'] = 99999
    #    ​
    #    df_all['mcc_rm_cat1'] = df_all['mcc_cat1']
    #    df_all.loc[~df_all['mcc_rm_cat1'].isin(df_all['mcc_rm_cat1'].value_counts().iloc[:35].index.values),
    #               'mcc_rm_cat1'] = 99999
    #
    #    # OHE урезанных MCC
    #    df_all = pd.concat([df_all,
    #                        pd.get_dummies(df_all['mcc_rm'], prefix='mcc_rm_ohe').astype(np.int8)], axis=1)
    #    del df_all['mcc_rm']
    #    df_all = pd.concat([df_all,
    #                        pd.get_dummies(df_all['mcc_rm_cat1'], prefix='mcc_rm_cat1_ohe').astype(np.int8)], axis=1)
    #    del df_all['mcc_rm_cat1']
    #    ​
    #    df_all = pd.concat([df_all,
    #                        pd.get_dummies(df_all['mcc_cat2'], prefix='mcc_cat2_ohe').astype(np.int8)], axis=1)
    #    del df_all['mcc_cat2']
    #    df_all = df_all.reset_index(drop=True)
    #
    #    mcc_cols_0 = [c for c in df_all.columns if 'mcc_rm_ohe' in c]
    #    mcc_cols_1 = [c for c in df_all.columns if 'mcc_rm_cat1_ohe' in c]
    #    mcc_cols_2 = [c for c in df_all.columns if 'mcc_cat2_ohe' in c]
    #    ​
    #    ​
    #    mcc_cols_0_ = [c + '_amount' for c in mcc_cols_0]
    #    mcc_cols_1_ = [c + '_amount' for c in mcc_cols_1]
    #    mcc_cols_2_ = [c + '_amount' for c in mcc_cols_2]
    #
    #    # сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #    df_mcc = df_all.groupby('customer_id')[mcc_cols_0].agg(['mean', 'sum'])
    #    df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    df_mcc = df_mcc.astype(np.float32).reset_index()
    #    df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #    df_mcc.head()
    #    customer_id
    #    mcc_rm_ohe_4111_mean
    #    mcc_rm_ohe_4111_sum
    #    mcc_rm_ohe_4784_mean
    #    mcc_rm_ohe_4784_sum
    #    mcc_rm_ohe_5200_mean
    #    mcc_rm_ohe_5200_sum
    #    mcc_rm_ohe_5211_mean
    #    mcc_rm_ohe_5211_sum
    #    mcc_rm_ohe_5261_mean
    #    mcc_rm_ohe_5261_sum
    #    mcc_rm_ohe_5311_mean
    #    mcc_rm_ohe_5311_sum
    #    mcc_rm_ohe_5331_mean
    #    mcc_rm_ohe_5331_sum
    #    mcc_rm_ohe_5411_mean
    #    mcc_rm_ohe_5411_sum
    #    mcc_rm_ohe_5499_mean
    #    mcc_rm_ohe_5499_sum
    #    mcc_rm_ohe_5533_mean
    #    mcc_rm_ohe_5533_sum
    #    mcc_rm_ohe_5541_mean
    #    mcc_rm_ohe_5541_sum
    #    mcc_rm_ohe_5641_mean
    #    mcc_rm_ohe_5641_sum
    #    mcc_rm_ohe_5651_mean
    #    mcc_rm_ohe_5651_sum
    #    mcc_rm_ohe_5661_mean
    #    mcc_rm_ohe_5661_sum
    #    mcc_rm_ohe_5691_mean
    #    mcc_rm_ohe_5691_sum
    #    mcc_rm_ohe_5699_mean
    #    mcc_rm_ohe_5699_sum
    #    mcc_rm_ohe_5712_mean
    #    mcc_rm_ohe_5712_sum
    #    mcc_rm_ohe_5732_mean
    #    mcc_rm_ohe_5732_sum
    #    mcc_rm_ohe_5812_mean
    #    mcc_rm_ohe_5812_sum
    #    mcc_rm_ohe_5813_mean
    #    mcc_rm_ohe_5813_sum
    #    mcc_rm_ohe_5814_mean
    #    mcc_rm_ohe_5814_sum
    #    mcc_rm_ohe_5912_mean
    #    mcc_rm_ohe_5912_sum
    #    mcc_rm_ohe_5921_mean
    #    mcc_rm_ohe_5921_sum
    #    mcc_rm_ohe_5941_mean
    #    mcc_rm_ohe_5941_sum
    #    mcc_rm_ohe_5942_mean
    #    mcc_rm_ohe_5942_sum
    #    mcc_rm_ohe_5945_mean
    #    mcc_rm_ohe_5945_sum
    #    mcc_rm_ohe_5977_mean
    #    mcc_rm_ohe_5977_sum
    #    mcc_rm_ohe_5992_mean
    #    mcc_rm_ohe_5992_sum
    #    mcc_rm_ohe_5995_mean
    #    mcc_rm_ohe_5995_sum
    #    mcc_rm_ohe_5999_mean
    #    mcc_rm_ohe_5999_sum
    #    mcc_rm_ohe_6011_mean
    #    mcc_rm_ohe_6011_sum
    #    mcc_rm_ohe_7230_mean
    #    mcc_rm_ohe_7230_sum
    #    mcc_rm_ohe_7832_mean
    #    mcc_rm_ohe_7832_sum
    #    mcc_rm_ohe_8099_mean
    #    mcc_rm_ohe_8099_sum
    #    mcc_rm_ohe_8999_mean
    #    mcc_rm_ohe_8999_sum
    #    mcc_rm_ohe_99999_mean
    #    mcc_rm_ohe_99999_sum
    #    0
    #    0001
    #    f322716470bf9bfc1708f06f00fc
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.02
    #    1.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.340000
    #    17.0
    #    0.020000
    #    1.0
    #    0.0
    #    0.0
    #    0.140000
    #    7.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.020000
    #    1.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.080000
    #    4.0
    #    0.360000
    #    18.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.020000
    #    1.0
    #    1
    #    000216
    #    83
    #    ccb416637fe9a4cd35e4606e
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00
    #    0.0
    #    0.038462
    #    3.0
    #    0.000000
    #    0.0
    #    0.012821
    #    1.0
    #    0.371795
    #    29.0
    #    0.012821
    #    1.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.012821
    #    1.0
    #    0.0
    #    0.0
    #    0.025641
    #    2.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.012821
    #    1.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.141026
    #    11.0
    #    0.064103
    #    5.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.115385
    #    9.0
    #    0.025641
    #    2.0
    #    0.064103
    #    5.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.102564
    #    8.0
    #    2
    #    0002
    #    d0f8a642272b41c292c12ab6e602
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00
    #    0.0
    #    0.000000
    #    0.0
    #    0.051948
    #    4.0
    #    0.000000
    #    0.0
    #    0.610390
    #    47.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.142857
    #    11.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.194805
    #    15.0
    #    3
    #    0004
    #    d182d9fede3ba2534b2d5e5ad27e
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00
    #    0.0
    #    0.000000
    #    0.0
    #    0.008333
    #    1.0
    #    0.000000
    #    0.0
    #    0.150000
    #    18.0
    #    0.016667
    #    2.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.016667
    #    2.0
    #    0.008333
    #    1.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.008333
    #    1.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.033333
    #    4.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.008333
    #    1.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.691667
    #    83.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.016667
    #    2.0
    #    0.000000
    #    0.0
    #    0.041667
    #    5.0
    #    4
    #    00072
    #    97
    #    d86e14bd68bd87b1dbdefe302
    #    0.008333
    #    2.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.000000
    #    0.0
    #    0.270833
    #    65.0
    #    0.008333
    #    2.0
    #    0.0
    #    0.0
    #    0.004167
    #    1.0
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.004167
    #    1.0
    #    0.004167
    #    1.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.0
    #    0.033333
    #    8.0
    #    0.004167
    #    1.0
    #    0.233333
    #    56.0
    #    0.045833
    #    11.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.008333
    #    2.0
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.020833
    #    5.0
    #    0.000000
    #    0.0
    #    0.029167
    #    7.0
    #    0.258333
    #    62.0
    #    0.008333
    #    2.0
    #    0.008333
    #    2.0
    #    0.004167
    #    1.0
    #    0.008333
    #    2.0
    #    0.037500
    #    9.0
    #
    #    # по объемам
    #    for i, c in enumerate(mcc_cols_0):
    #        df_all[mcc_cols_0_[i]] = (df_all[c] * df_all['amount']).astype(np.float32)
    #    for i, c in enumerate(mcc_cols_2):
    #        df_all[mcc_cols_2_[i]] = (df_all[c] * df_all['amount']).astype(np.float32)
    #
    #    # по объемам
    #    df_mcc = df_all.groupby('customer_id')[mcc_cols_0_].agg(['mean', 'sum'])
    #    df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    df_mcc = df_mcc.astype(np.float32).reset_index()
    #    df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #    df_mcc.head()
    #    ​
    #    # df_all['add_lat_'] = (df_all['add_lat'] * 40).astype(np.int32)
    #    # df_all['add_lon_'] = (df_all['add_lon'] * 40).astype(np.int32)
    #    ​
    #    # df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols_].agg(['mean', 'sum'])
    #    # df_mcc = df_mcc.add_suffix('_40coord')
    #    # df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    # df_mcc = df_mcc.astype(np.float32)
    #    # df_mcc.reset_index(inplace=True)
    #    # df_mcc.head()
    #    # df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #    ​
    #    # del df_all['add_lat_']
    #    # del df_all['add_lon_']
    #    customer_id
    #    mcc_rm_ohe_4111_amount_mean
    #    mcc_rm_ohe_4111_amount_sum
    #    mcc_rm_ohe_4784_amount_mean
    #    mcc_rm_ohe_4784_amount_sum
    #    mcc_rm_ohe_5200_amount_mean
    #    mcc_rm_ohe_5200_amount_sum
    #    mcc_rm_ohe_5211_amount_mean
    #    mcc_rm_ohe_5211_amount_sum
    #    mcc_rm_ohe_5261_amount_mean
    #    mcc_rm_ohe_5261_amount_sum
    #    mcc_rm_ohe_5311_amount_mean
    #    mcc_rm_ohe_5311_amount_sum
    #    mcc_rm_ohe_5331_amount_mean
    #    mcc_rm_ohe_5331_amount_sum
    #    mcc_rm_ohe_5411_amount_mean
    #    mcc_rm_ohe_5411_amount_sum
    #    mcc_rm_ohe_5499_amount_mean
    #    mcc_rm_ohe_5499_amount_sum
    #    mcc_rm_ohe_5533_amount_mean
    #    mcc_rm_ohe_5533_amount_sum
    #    mcc_rm_ohe_5541_amount_mean
    #    mcc_rm_ohe_5541_amount_sum
    #    mcc_rm_ohe_5641_amount_mean
    #    mcc_rm_ohe_5641_amount_sum
    #    mcc_rm_ohe_5651_amount_mean
    #    mcc_rm_ohe_5651_amount_sum
    #    mcc_rm_ohe_5661_amount_mean
    #    mcc_rm_ohe_5661_amount_sum
    #    mcc_rm_ohe_5691_amount_mean
    #    mcc_rm_ohe_5691_amount_sum
    #    mcc_rm_ohe_5699_amount_mean
    #    mcc_rm_ohe_5699_amount_sum
    #    mcc_rm_ohe_5712_amount_mean
    #    mcc_rm_ohe_5712_amount_sum
    #    mcc_rm_ohe_5732_amount_mean
    #    mcc_rm_ohe_5732_amount_sum
    #    mcc_rm_ohe_5812_amount_mean
    #    mcc_rm_ohe_5812_amount_sum
    #    mcc_rm_ohe_5813_amount_mean
    #    mcc_rm_ohe_5813_amount_sum
    #    mcc_rm_ohe_5814_amount_mean
    #    mcc_rm_ohe_5814_amount_sum
    #    mcc_rm_ohe_5912_amount_mean
    #    mcc_rm_ohe_5912_amount_sum
    #    mcc_rm_ohe_5921_amount_mean
    #    mcc_rm_ohe_5921_amount_sum
    #    mcc_rm_ohe_5941_amount_mean
    #    mcc_rm_ohe_5941_amount_sum
    #    mcc_rm_ohe_5942_amount_mean
    #    mcc_rm_ohe_5942_amount_sum
    #    mcc_rm_ohe_5945_amount_mean
    #    mcc_rm_ohe_5945_amount_sum
    #    mcc_rm_ohe_5977_amount_mean
    #    mcc_rm_ohe_5977_amount_sum
    #    mcc_rm_ohe_5992_amount_mean
    #    mcc_rm_ohe_5992_amount_sum
    #    mcc_rm_ohe_5995_amount_mean
    #    mcc_rm_ohe_5995_amount_sum
    #    mcc_rm_ohe_5999_amount_mean
    #    mcc_rm_ohe_5999_amount_sum
    #    mcc_rm_ohe_6011_amount_mean
    #    mcc_rm_ohe_6011_amount_sum
    #    mcc_rm_ohe_7230_amount_mean
    #    mcc_rm_ohe_7230_amount_sum
    #    mcc_rm_ohe_7832_amount_mean
    #    mcc_rm_ohe_7832_amount_sum
    #    mcc_rm_ohe_8099_amount_mean
    #    mcc_rm_ohe_8099_amount_sum
    #    mcc_rm_ohe_8999_amount_mean
    #    mcc_rm_ohe_8999_amount_sum
    #    mcc_rm_ohe_99999_amount_mean
    #    mcc_rm_ohe_99999_amount_sum
    #    0
    #    0001
    #    f322716470bf9bfc1708f06f00fc
    #    0.000000
    #    0.00000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    27.80541
    #    1390.270508
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    174.479874
    #    8723.993164
    #    3.240682
    #    162.034088
    #    0.0
    #    0.0
    #    142.566284
    #    7128.314453
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    3.753808
    #    187.690399
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.00000
    #    0.000000
    #    0.000000
    #    35.659580
    #    1782.979004
    #    6294.915039
    #    314745.750000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    20.779797
    #    1038.989868
    #    1
    #    000216
    #    83
    #    ccb416637fe9a4cd35e4606e
    #    0.000000
    #    0.00000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00000
    #    0.000000
    #    75.903763
    #    5920.493164
    #    0.000000
    #    0.000000
    #    46.309711
    #    3612.157471
    #    447.340057
    #    34892.523438
    #    2.944012
    #    229.632919
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    5.861734
    #    457.215271
    #    0.0
    #    0.0
    #    25.973988
    #    2025.971069
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    3.667529
    #    286.067230
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    32.625473
    #    2544.786865
    #    91.864258
    #    7165.412109
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.00000
    #    46.323765
    #    3613.253418
    #    88.990273
    #    6941.241211
    #    4656.711426
    #    363223.500000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    214.347534
    #    16719.107422
    #    2
    #    0002
    #    d0f8a642272b41c292c12ab6e602
    #    0.000000
    #    0.00000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00000
    #    0.000000
    #    0.000000
    #    0.000000
    #    8.041179
    #    619.170776
    #    0.000000
    #    0.000000
    #    102.385597
    #    7883.690918
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.00000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    244.772186
    #    18847.457031
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    129.416885
    #    9965.099609
    #    3
    #    0004
    #    d182d9fede3ba2534b2d5e5ad27e
    #    0.000000
    #    0.00000
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00000
    #    0.000000
    #    0.000000
    #    0.000000
    #    3.977230
    #    477.267609
    #    0.000000
    #    0.000000
    #    82.209663
    #    9865.159180
    #    3.002061
    #    360.247314
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    96.718063
    #    11606.167969
    #    11.536736
    #    1384.408325
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    15.889756
    #    1906.770752
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    23.739368
    #    2848.724121
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    10.003338
    #    1200.400513
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.000000
    #    0.00000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    1752.626221
    #    210315.156250
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    14.573447
    #    1748.813721
    #    0.000000
    #    0.000000
    #    72.051506
    #    8646.180664
    #    4
    #    00072
    #    97
    #    d86e14bd68bd87b1dbdefe302
    #    4.562878
    #    1095.09082
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    0.00000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    0.000000
    #    237.856369
    #    57085.527344
    #    0.716898
    #    172.055511
    #    0.0
    #    0.0
    #    2.150652
    #    516.156555
    #    0.000000
    #    0.000000
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    6.908348
    #    1658.003418
    #    2.476619
    #    594.388672
    #    0.0
    #    0.0
    #    0.000000
    #    0.000000
    #    26.938381
    #    6465.211426
    #    0.738737
    #    177.296783
    #    62.065884
    #    14895.812500
    #    27.020506
    #    6484.921387
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    7.383849
    #    1772.123779
    #    0.0
    #    0.0
    #    0.0
    #    0.0
    #    20.362558
    #    4887.01416
    #    0.000000
    #    0.000000
    #    17.092093
    #    4102.102051
    #    1940.178711
    #    465642.875000
    #    3.774575
    #    905.898132
    #    2.133497
    #    512.039246
    #    9.404689
    #    2257.125244
    #    0.959698
    #    230.327621
    #    275.180389
    #    66043.296875
    #
    #    # по объемам
    #    df_mcc = df_all.groupby('customer_id')[mcc_cols_2_].agg(['mean', 'sum'])
    #    df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    df_mcc = df_mcc.astype(np.float32).reset_index()
    #    df_mcc.head()
    #    df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #    ​
    #    # df_all['add_lat_'] = (df_all['add_lat'] * 40).astype(np.int32)
    #    # df_all['add_lon_'] = (df_all['add_lon'] * 40).astype(np.int32)
    #    ​
    #    # df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols_].agg(['mean', 'sum'])
    #    # df_mcc = df_mcc.add_suffix('_40coord')
    #    # df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    # df_mcc = df_mcc.astype(np.float32)
    #    # df_mcc.reset_index(inplace=True)
    #    # df_mcc.head()
    #    # df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #    ​
    #    # del df_all['add_lat_']
    #    # del df_all['add_lon_']
    #
    #    # сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #    # mcc_cols = [c for c in df_all.columns if 'mcc_cat1' in c]
    #    # df_mcc = df_all.groupby('customer_id')[mcc_cols].agg(['mean'])
    #    # df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    # df_mcc.reset_index(inplace=True)
    #    # df_mcc.head()
    #    # df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #
    #    # сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #    df_mcc = df_all.groupby('customer_id')[mcc_cols_2].agg(['mean', 'sum'])
    #    df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    df_mcc = df_mcc.astype(np.float32)
    #    df_mcc.reset_index(inplace=True)
    #    df_mcc.head()
    #    df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #
    #    # РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА
    #    df_all['add_lat_'] = (df_all['add_lat'] * 40).astype(np.int32)
    #    df_all['add_lon_'] = (df_all['add_lon'] * 40).astype(np.int32)
    #    ​
    #    df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols_0].agg(['mean', 'sum'])
    #    df_mcc = df_mcc.add_suffix('_40coord')
    #    df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    df_mcc = df_mcc.astype(np.float32)
    #    df_mcc.reset_index(inplace=True)
    #    df_mcc.head()
    #    df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #    ​
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #
    #    mcc_cols = [c for c in df_all.columns if 'mcc_rm_ohe' in c and 'mean' not in c and 'sum' not in c]
    #    # РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА
    #    df_all['add_lat_'] = (df_all['add_lat'] * 100).astype(np.int32)
    #    df_all['add_lon_'] = (df_all['add_lon'] * 100).astype(np.int32)
    #    ​
    #    df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols_0].agg(['mean', 'sum'])
    #    df_mcc = df_mcc.add_suffix('_100coord')
    #    df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    df_mcc = df_mcc.astype(np.float32)
    #    df_mcc.reset_index(inplace=True)
    #    df_mcc.head()
    #    df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #    ​
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #
    #    # РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА (ПРОВЕРИЛ-ЛУЧШЕ РАБОТАЕТ НА БОЛЬШИХ УЧАСТКАХ)
    #    df_all['add_lat_'] = (df_all['add_lat'] * 40).astype(np.int32)
    #    df_all['add_lon_'] = (df_all['add_lon'] * 40).astype(np.int32)
    #    ​
    #    df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols_2].agg(['mean', 'sum'])
    #    df_mcc = df_mcc.add_suffix('_200coord')
    #    df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    df_mcc = df_mcc.astype(np.float32)
    #    df_mcc.reset_index(inplace=True)
    #    df_mcc.head()
    #    df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #    ​
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #
    #    # РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА
    #    # df_all['add_lat_'] = (df_all['add_lat'] * 100).astype(np.int32)
    #    # df_all['add_lon_'] = (df_all['add_lon'] * 100).astype(np.int32)
    #    ​
    #    # df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols].agg(['mean', 'sum'])
    #    # df_mcc = df_mcc.add_suffix('_100coord')
    #    # df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #    # df_mcc = df_mcc.astype(np.float32)
    #    # df_mcc.reset_index(inplace=True)
    #    # df_mcc.head()
    #    # df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #    ​
    #    # del df_all['add_lat_']
    #    # del df_all['add_lon_']
    #    Игрушки
    #    с
    #    адресами
    #
    #    df_all['string'] = df_all['string'].fillna('')
    #    df_all['string'] = df_all['string'].str.lower()
    #
    #    df_all['has_street'] = df_all['string'].str.contains('улиц').astype(np.int8)
    #    df_all['has_pereul'] = df_all['string'].str.contains('переул').astype(np.int8)
    #    df_all['has_bulvar'] = df_all['string'].str.contains('бульв').astype(np.int8)
    #    df_all['has_prospekt'] = df_all['string'].str.contains('проспект').astype(np.int8)
    #    df_all['has_shosse'] = df_all['string'].str.contains('шосс').astype(np.int8)
    #    ​
    #    df_all['has_torg'] = df_all['string'].str.contains('торгов').astype(np.int8)
    #    df_all['has_bus'] = df_all['string'].str.contains('бизн').astype(np.int8)
    #    Медианы
    #    по
    #    юзеру
    #    и
    #    по
    #    без
    #    дубликатов
    #
    #    dft = df_all.groupby('terminal_id')['add_lat'].agg('std').astype(np.float32).reset_index()
    #    dft['moving_terminal'] = (dft['add_lat'] > 0).astype(np.int8)
    #    del dft['add_lat']
    #    df_all = pd.merge(df_all, dft, on='terminal_id', how='left')
    #
    #    df_med = df_all.groupby('customer_id')['add_lat', 'add_lon'].agg('median').astype(np.float32).reset_index()
    #    df_med.columns = ['customer_id', 'add_lat_median', 'add_lon_median']
    #    df_all = pd.merge(df_all, df_med, on='customer_id', how='left')
    #
    #    df_med = df_all.drop_duplicates(subset=['customer_id',
    #                                            'add_lat', 'add_lon']).groupby('customer_id')['add_lat', 'add_lon'].agg(
    #        'median').reset_index()
    #    df_med.columns = ['customer_id', 'add_lat_median_unique', 'add_lon_median_unique']
    #    df_all = pd.merge(df_all, df_med, on='customer_id', how='left')
    #
    #    df_all['lat_diff_median'] = np.abs(df_all['add_lat'] - df_all['add_lat_median'])
    #    df_all['lon_diff_median'] = np.abs(df_all['add_lon'] - df_all['add_lat_median'])
    #    df_all['lat_diff_median_unique'] = np.abs(df_all['add_lat'] - df_all['add_lat_median_unique'])
    #    df_all['lon_diff_median_unique'] = np.abs(df_all['add_lon'] - df_all['add_lon_median_unique'])
    #    ​
    #    df_all['diff_median'] = df_all['lat_diff_median'] + df_all['lon_diff_median']
    #    df_all['diff_median_unique'] = df_all['lat_diff_median_unique'] + df_all['lon_diff_median_unique']
    #
    #    del dft
    #    del df_med
    #    OSM
    #    https: // wiki.openstreetmap.org / wiki / RU: % D0 % 9
    #    E % D0 % B1 % D1 % 8
    #    A % D0 % B5 % D0 % BA % D1 % 82 % D1 % 8
    #    B_ % D0 % BA % D0 % B0 % D1 % 80 % D1 % 82 % D1 % 8
    #    B  # .D0.9A.D0.BE.D0.BC.D0.BC.D0.B5.D1.80.D1.87.D0.B5.D1.81.D0.BA.D0.B8.D0.B5
    #
    #    import ogr
    #    driver = ogr.GetDriverByName('OSM')
    #    data_msk = driver.Open('../data/internal/moscow.osm')
    #    data_peter = driver.Open('../data/internal/peter.osm')
    #
    #    features = []
    #    nlayer = data_msk.GetLayerCount()  # 5
    #    print(nlayer)
    #    for i in range(nlayer):
    #        features += [x for x in data_msk.GetLayerByIndex(i)]
    #    nlayer = data_peter.GetLayerCount()  # 5
    #    print(nlayer)
    #    for i in range(nlayer):
    #        features += [x for x in data_peter.GetLayerByIndex(i)]
    #    5
    #    5
    #    расстояние
    #    до
    #    бизнес
    #    центров
    #
    #    coords = []
    #    for f in tqdm(features):
    #        s = str(f.ExportToJson(as_object=True)).lower()
    #        if 'бизнес' in s and 'центр' in s:
    #            el = f.ExportToJson(as_object=True)['geometry']['coordinates'][0]
    #            if type(el) != float:
    #                coords.append(el)
    #    100 % |██████████ | 622083 / 622083[02:27 < 00:00, 4215.21
    #    it / s]
    #
    #    # coords = []
    #    # for f in tqdm(features):
    #    #     s = str(f.ExportToJson(as_object=True)).lower()
    #    #     if 'running' in s:
    #    #         coords.append(s)
    #    #         print(s)
    #    #         el = f.ExportToJson(as_object=True)['geometry']['coordinates'][0]
    #    #         if type(el) != float:
    #    #             coords.append(el)
    #
    #    vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #    df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #    vals2 = np.array(coords, dtype=np.float32)
    #    vals1.shape, vals2.shape
    #    ((160184, 2), (206, 2))
    #
    #    X = pairwise_distances(vals1, vals2)
    #    X[X == 0] = 10000
    #
    #    suf = 'bc'
    #    df_vals[suf + '_dist_to'] = X.min(axis=1)
    #    df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #    df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #    df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #    df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #    df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #    ​
    #    df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #    df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #    df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #    df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #    del df_vals['add_lat']
    #    del df_vals['add_lon']
    #    ​
    #    df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #    del X
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #    до
    #    фастфудов
    #    http: // andrewgaidus.com / Convert_OSM_Data /
    #
    #    driver = ogr.GetDriverByName('OSM')
    #    data_msk = driver.Open('../data/internal/moscow.osm')
    #    data_peter = driver.Open('../data/internal/peter.osm')
    #    layer_p = data_msk.GetLayer('points')  # 5
    #    features_p = [x for x in layer_p]
    #    layer_p = data_peter.GetLayer('points')  # 5
    #    features_p += [x for x in layer_p]
    #
    #    # coords = []
    #    # for f in tqdm(features_p):
    #    #     s = str(f.ExportToJson(as_object=True)).lower()
    #    #     if 'run' in s:
    #    #         print(s)
    #    #         coords.append(f.ExportToJson(as_object=True)['geometry']['coordinates'])
    #
    #    coords = []
    #    for f in tqdm(features_p):
    #        s = str(f.ExportToJson(as_object=True)).lower()
    #        if 'fast_food' in s:
    #            coords.append(f.ExportToJson(as_object=True)['geometry']['coordinates'])
    #    100 % |██████████ | 343972 / 343972[01:16 < 00:00, 4515.63
    #    it / s]
    #
    #    vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #    df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #    vals2 = np.array(coords, dtype=np.float32)
    #    vals1.shape, vals2.shape
    #    ((160184, 2), (2562, 2))
    #
    #    X = pairwise_distances(vals1, vals2)
    #    X[X == 0] = 10000
    #
    #    suf = 'fastfood'
    #    df_vals[suf + '_dist_to'] = X.min(axis=1)
    #    df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #    df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #    df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #    df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #    df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #    ​
    #    df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #    df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #    df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #    df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #    del df_vals['add_lat']
    #    del df_vals['add_lon']
    #    ​
    #    df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #    del X
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #    станции
    #
    #    coords = []
    #    for f in tqdm(features_p):
    #        s = str(f.ExportToJson(as_object=True)).lower()
    #        if 'railway' in s:
    #            coords.append(f.ExportToJson(as_object=True)['geometry']['coordinates'])
    #    100 % |██████████ | 343972 / 343972[01:16 < 00:00, 4482.17
    #    it / s]
    #
    #    vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #    df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #    vals2 = np.array(coords, dtype=np.float32)
    #    vals1.shape, vals2.shape
    #    ((160184, 2), (8159, 2))
    #
    #    X = pairwise_distances(vals1, vals2)
    #    X[X == 0] = 10000
    #
    #    suf = 'rail'
    #    df_vals[suf + '_dist_to'] = X.min(axis=1)
    #    df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #    df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #    df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #    df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #    df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #    ​
    #    df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #    df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #    df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #    df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #    del df_vals['add_lat']
    #    del df_vals['add_lon']
    #    ​
    #    df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #    del X
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #    райф
    #
    #    coords = []
    #    for f in tqdm(features_p):
    #        s = str(f.ExportToJson(as_object=True)).lower()
    #        if 'райф' in s or 'raiffeisen' in s:
    #            coords.append(f.ExportToJson(as_object=True)['geometry']['coordinates'])
    #    100 % |██████████ | 343972 / 343972[01:15 < 00:00, 4561.06
    #    it / s]
    #
    #    vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #    df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #    vals2 = np.array(coords, dtype=np.float32)
    #    vals1.shape, vals2.shape
    #    ((160184, 2), (194, 2))
    #
    #    X = pairwise_distances(vals1, vals2)
    #    X[X == 0] = 10000
    #
    #    suf = 'raif1'
    #    df_vals[suf + '_dist_to'] = X.min(axis=1)
    #    df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #    df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #    df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #    df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #    df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #    ​
    #    df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #    df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #    df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #    df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #    del df_vals['add_lat']
    #    del df_vals['add_lon']
    #    ​
    #    df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #    del X
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #
    #    coords = []
    #    for f in tqdm(features):
    #        s = str(f.ExportToJson(as_object=True)).lower()
    #        if 'райф' in s or 'raiffeisen' in s:
    #            el = f.ExportToJson(as_object=True)['geometry']['coordinates'][0]
    #            if type(el) != float:
    #                coords.append(el)
    #    100 % |██████████ | 622083 / 622083[02:27 < 00:00, 4226.28
    #    it / s]
    #
    #    vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #    df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #    vals2 = np.array(coords, dtype=np.float32)
    #    vals1.shape, vals2.shape
    #    ((160184, 2), (1, 2))
    #
    #    X = pairwise_distances(vals1, vals2)
    #    X[X == 0] = 10000
    #
    #    suf = 'raif2'
    #    df_vals[suf + '_dist_to'] = X.min(axis=1)
    #    df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #    df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #    df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #    df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #    df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #    ​
    #    df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #    df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #    df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #    df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #    del df_vals['add_lat']
    #    del df_vals['add_lon']
    #    ​
    #    df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #    del X
    #    del df_all['add_lat_']
    #    del df_all['add_lon_']
    #
    #    del vals2
    #
    #    for c in tqdm(df_all.columns):
    #        if df_all[c].dtype == np.int64:
    #            df_all[c] = df_all[c].astype(np.int32)
    #        if df_all[c].dtype == np.float64:
    #            df_all[c] = df_all[c].astype(np.float32)
    #    100 % |██████████ | 699 / 699[00:03 < 00:00, 220.50
    #    it / s]
    #
    #    df_all.dtypes.to_csv('../data/df_all_b21_dtypes.csv')
    #    df_all.to_csv('../data/df_all_b21.csv', index=None)
    #
    #    df_all.shape
    #    (2294265, 699)
    #
    #    % % time
    #    # # Определим типы колонок для экономии памяти
    #    # dtypes = pd.read_csv('../data/df_all_b2_dtypes.csv', header=None, index_col=0).to_dict()[1]
    #    # dtypes.pop('transaction_date', None)
    #    # df_all = pd.read_csv('../data/df_all_b2.csv', dtype=dtypes, parse_dates=['transaction_date'])
    #    Ранки
    #
    #    gb = df_all.groupby('customer_id')
    #
    #    df_all['rank_amount_cid'] = df_all.groupby('customer_id')['amount'].rank()
    #
    #    df_all = pd.merge(df_all,
    #                      df_all.groupby('customer_id')['amount'].agg(['size']).reset_index(), on='customer_id', how='left')
    #
    #    df_all['rank_amount_cid_percent'] = df_all['rank_amount_cid'] / df_all['size']
    #
    #    del features_p
    #    Расстояния
    #    до
    #    центров
    #
    #    am_cols = [c for c in df_all if 'amount' in c]
    #
    #    df_all['dist_to_center'] = -1
    #
    #    vals1 = df_all[df_all.city_name == 'Санкт-Петербург'][['add_lat', 'add_lon']].values
    #    vals2 = np.array([[59.935386, 30.324629]])
    #    X = pairwise_distances(vals1, vals2)
    #    df_all.loc[df_all.city_name == 'Санкт-Петербург', 'dist_to_center'] = X
    #
    #    vals1 = df_all[df_all.city_name == 'Москва'][['add_lat', 'add_lon']].values
    #    vals2 = np.array([[55.7537090, 37.6198133]])
    #    X = pairwise_distances(vals1, vals2)
    #    df_all.loc[df_all.city_name == 'Москва', 'dist_to_center'] = X
    #    LightGBM
    #
    #    df_all.shape, df_all.columns.duplicated().sum()
    #    ((2294265, 703), 0)
    #
    #    df_all = df_all.loc[:, ~df_all.columns.duplicated()]
    #
    #    from sklearn.model_selection import train_test_split
    #    ​
    #    ys = ['is_home', 'is_work']
    #    drop_cols = ['atm_address', 'customer_id', 'pos_address', 'terminal_id', 'transaction_date',
    #                 'is_home', 'has_home', 'is_work', 'has_work', 'is_train', 'city_name']
    #    drop_cols += ['work_lat', 'work_lon', 'home_lat', 'home_lon', 'string']
    #    ​
    #    drop_cols += ['pred:is_home', 'pred:is_work']
    #    # cols = [c for c in df_all.columns if 'median_dist' in c]
    #    # cols = [c for c in df_all.columns if 'lat' in c or 'lon' in c and 'diff' not in c and 'median' not in c]
    #    # cols += ['address']
    #    # drop_cols += cols
    #    ​
    #    cols = [c for c in df_all.columns if 'mcc_ohe' in c and 'mean' not in c]
    #    # cols += ['address']
    #    drop_cols += cols
    #    ​
    #    ​
    #    y_cols = ['is_home', 'is_work']
    #    usecols = df_all.drop(drop_cols, 1, errors='ignore').columns
    #
    #    params = {
    #        'objective': 'binary',
    #        'num_leaves': 511,
    #        'learning_rate': 0.01,
    #        'metric': 'binary_logloss',
    #        'feature_fraction': 0.8,
    #        'bagging_fraction': 0.8,
    #        'bagging_freq': 1,
    #        'num_threads': 12,
    #        'verbose': 0,
    #    }
    #    ​
    #    model = {}
    #
    #    y_col = 'is_home'
    #    ​
    #    cust_train = df_all[df_all['is_train'] == 1].groupby('customer_id')[y_col.replace('is_', 'has_')].max()
    #    cust_train = cust_train[cust_train > 0].index
    #    ​
    #    cust_train, cust_valid = train_test_split(cust_train, test_size=0.2, shuffle=True, random_state=111)
    #    ​
    #    df_train = pd.DataFrame(cust_train, columns=['customer_id']).merge(df_all, how='left')
    #    df_valid = pd.DataFrame(cust_valid, columns=['customer_id']).merge(df_all, how='left')
    #    ​
    #    lgb_train = lgb.Dataset(df_train[usecols], df_train[y_col])
    #    lgb_valid = lgb.Dataset(df_valid[usecols], df_valid[y_col])
    #    ​
    #    gbm_h = lgb.train(params,
    #                      lgb_train,
    #                      valid_sets=[lgb_valid],
    #                      num_boost_round=2000,
    #                      verbose_eval=30,
    #                      early_stopping_rounds=100)
    #    ​
    #    model[y_col] = gbm_h
    #    Training
    #    until
    #    validation
    #    scores
    #    don
    #    't improve for 100 rounds.
    #    [30]
    #    valid_0
    #    's binary_logloss: 0.583925
    #    [60]
    #    valid_0
    #    's binary_logloss: 0.516789
    #    [90]
    #    valid_0
    #    's binary_logloss: 0.47214
    #    [120]
    #    valid_0
    #    's binary_logloss: 0.442272
    #    [150]
    #    valid_0
    #    's binary_logloss: 0.421676
    #    [180]
    #    valid_0
    #    's binary_logloss: 0.40754
    #    [210]
    #    valid_0
    #    's binary_logloss: 0.397597
    #    [240]
    #    valid_0
    #    's binary_logloss: 0.390218
    #    [270]
    #    valid_0
    #    's binary_logloss: 0.385013
    #    [300]
    #    valid_0
    #    's binary_logloss: 0.381261
    #    [330]
    #    valid_0
    #    's binary_logloss: 0.378859
    #    [360]
    #    valid_0
    #    's binary_logloss: 0.376952
    #    [390]
    #    valid_0
    #    's binary_logloss: 0.375839
    #    [420]
    #    valid_0
    #    's binary_logloss: 0.375117
    #    [450]
    #    valid_0
    #    's binary_logloss: 0.3746
    #    [480]
    #    valid_0
    #    's binary_logloss: 0.374571
    #    [510]
    #    valid_0
    #    's binary_logloss: 0.374397
    #    [540]
    #    valid_0
    #    's binary_logloss: 0.374263
    #    [570]
    #    valid_0
    #    's binary_logloss: 0.374408
    #    [600]
    #    valid_0
    #    's binary_logloss: 0.374786
    #    [630]
    #    valid_0
    #    's binary_logloss: 0.375561
    #    Early
    #    stopping, best
    #    iteration is:
    #    [556]
    #    valid_0
    #    's binary_logloss: 0.374234
    #
    #    y_col = 'is_work'
    #    ​
    #    cust_train = df_all[df_all['is_train'] == 1].groupby('customer_id')[y_col.replace('is_', 'has_')].max()
    #    cust_train = cust_train[cust_train > 0].index
    #    ​
    #    cust_train, cust_valid = train_test_split(cust_train, test_size=0.2, shuffle=True, random_state=111)
    #    ​
    #    ​
    #    ​
    #    df_train = pd.DataFrame(cust_train, columns=['customer_id']).merge(df_all, how='left')
    #    df_valid = pd.DataFrame(cust_valid, columns=['customer_id']).merge(df_all, how='left')
    #    ​
    #    lgb_train = lgb.Dataset(df_train[usecols], df_train[y_col])
    #    lgb_valid = lgb.Dataset(df_valid[usecols], df_valid[y_col])
    #    ​
    #    gbm_w = lgb.train(params,
    #                      lgb_train,
    #                      valid_sets=[lgb_valid],
    #                      num_boost_round=2000,
    #                      verbose_eval=30,
    #                      early_stopping_rounds=100)
    #    ​
    #    model[y_col] = gbm_w
    #    Training
    #    until
    #    validation
    #    scores
    #    don
    #    't improve for 100 rounds.
    #    [30]
    #    valid_0
    #    's binary_logloss: 0.561026
    #    [60]
    #    valid_0
    #    's binary_logloss: 0.481483
    #    [90]
    #    valid_0
    #    's binary_logloss: 0.430032
    #    [120]
    #    valid_0
    #    's binary_logloss: 0.396466
    #    [150]
    #    valid_0
    #    's binary_logloss: 0.374586
    #    [180]
    #    valid_0
    #    's binary_logloss: 0.359539
    #    [210]
    #    valid_0
    #    's binary_logloss: 0.349693
    #    [240]
    #    valid_0
    #    's binary_logloss: 0.343236
    #    [270]
    #    valid_0
    #    's binary_logloss: 0.339659
    #    [300]
    #    valid_0
    #    's binary_logloss: 0.337191
    #    [330]
    #    valid_0
    #    's binary_logloss: 0.336102
    #    [360]
    #    valid_0
    #    's binary_logloss: 0.33665
    #    [390]
    #    valid_0
    #    's binary_logloss: 0.337185
    #    [420]
    #    valid_0
    #    's binary_logloss: 0.339059
    #    Early
    #    stopping, best
    #    iteration is:
    #    [336]
    #    valid_0
    #    's binary_logloss: 0.336068
    #
    #    gbm_w
    #    Полезные
    #    MCC
    #    дом
    #    6011 - финансы
    #    5411 - придомовые
    #    магазы
    #    5814 - мак
    #    5912 - аптеки
    #    5921 - пиво
    #    5499 - магазы
    #    пяторочка
    #    типа
    #    5812 - рестроанчики
    #    работа
    #
    #    figsize(14, 10)
    #    lgb.plot_importance(gbm_h, max_num_features=40)
    #
    #    def _best(x):
    #        ret = None
    #        for col in ys:
    #            pred = ('pred:%s' % col)
    #            if pred in x:
    #                i = (x[pred].idxmax())
    #                cols = [pred, 'add_lat', 'add_lon']
    #                if col in x:
    #                    cols.append(col)
    #                tmp = x.loc[i, cols]
    #                tmp.rename({
    #                    'add_lat': '%s:add_lat' % col,
    #                    'add_lon': '%s:add_lon' % col,
    #                }, inplace=True)
    #                if ret is None:
    #                    ret = tmp
    #                else:
    #                    ret = pd.concat([ret, tmp])
    #        return ret
    #
    #    ​
    #    ​
    #
    #    def predict_proba(dt, ys=['is_home', 'is_work']):
    #        for col in ys:
    #            pred = ('pred:%s' % col)
    #            dt[pred] = model[col].predict(dt[usecols])
    #        return dt.groupby('customer_id').apply(_best).reset_index()
    #
    #    ​
    #
    #    def score(dt, ys=['is_home', 'is_work'], return_df=False):
    #        dt_ret = predict_proba(dt, ys)
    #        if return_df:
    #            return dt_ret
    #        mean = 0.0
    #        for col in ys:
    #            col_mean = dt_ret[col].mean()
    #            mean += col_mean
    #        if len(ys) == 2:
    #            mean = mean / len(ys)
    #        return mean
    #
    #    print("Train accuracy:", score(df_train, ys=['is_home']))
    #    print("Test accuracy:", score(df_valid, ys=['is_home']))
    #    ​
    #    print("Train accuracy:", score(df_train, ys=['is_work']))
    #    print("Test accuracy:", score(df_valid, ys=['is_work']))
    #    Train
    #    accuracy: 0.640571982549685
    #    Test
    #    accuracy: 0.6443798449612403
    #    Train
    #    accuracy: 0.5368395540475036
    #    Test
    #    accuracy: 0.3536821705426357
    #    Train
    #    accuracy: 0.5458070770722249
    #    Test
    #    accuracy: 0.5494186046511628
    #    Train
    #    accuracy: 0.4301987396994668
    #    Test
    #    accuracy: 0.3536821705426357
    #
    #    Анализ
    #    False - Negative
    #
    #    # сколько вообще людей имеют хорошую точку
    #    df_all[(df_all.is_train == 1)].groupby('customer_id')['is_work'].agg('max').mean()
    #
    #    df_pred = score(df_valid, ys=['is_home'], return_df=True)
    #
    #    df_pred.sample(5)
    #
    #    cid = 'bf66305d0ec05abb6e6a6358acb8c2a1'
    #    cid = df_pred[df_pred.is_home == 0].sample(1)['customer_id'].values[0]
    #    ​
    #    df_an = df_all[df_all.customer_id == cid]
    #    center_home = df_an[['home_lat', 'home_lon']].drop_duplicates().values
    #    center_work = df_an[['work_lat', 'work_lon']].drop_duplicates().values
    #    ​
    #    ​
    #    predicted_home = df_pred[df_pred.customer_id == cid][
    #        ['is_home:add_lat', 'is_home:add_lon']].drop_duplicates().values
    #    predicted_work = df_pred[df_pred.customer_id == cid][
    #        ['is_work:add_lat', 'is_work:add_lon']].drop_duplicates().values
    #    ​
    #    points_pos = df_an[df_an.is_pos == 1][['add_lat', 'add_lon']].dropna().values
    #    points_atm = df_an[df_an.is_pos == 0][['add_lat', 'add_lon']].dropna().values
    #    print(center_home.shape, center_work.shape, points_pos.shape, points_atm.shape)
    #    ​
    #    # синие - покупки
    #    # красные - банкоматы
    #    gmap = gmaps.Map()
    #    if len(points_pos) > 0:
    #        gmap.add_layer(gmaps.symbol_layer(points_pos, hover_text='pos',
    #                                          fill_color="blue", stroke_color="blue", scale=3))
    #    if len(points_atm) > 0:
    #        gmap.add_layer(gmaps.symbol_layer(points_atm, hover_text='atm',
    #                                          fill_color="red", stroke_color="red", scale=3))
    #    ​
    #    if not np.isnan(center_home)[0][0]:
    #        gmap.add_layer(gmaps.marker_layer(center_home, label='home'))
    #    if not np.isnan(center_work)[0][0]:
    #        gmap.add_layer(gmaps.marker_layer(center_work, label='work'))
    #    ​
    #    gmap.add_layer(gmaps.marker_layer(predicted_home, label='predicted_home'))
    #    gmap.add_layer(gmaps.marker_layer(predicted_work, label='predicted_work'))
    #
    #    gmap
    #
    #    df_all.to_csv('../data/dfpredict1903.csv', index=None)
    #    Predict
    #
    #    del cust_test
    #
    #    cust_test = df_all.loc[df_all['is_train'] == 0, 'customer_id'].unique()
    #    # df_test = pd.DataFrame(cust_test, columns = ['customer_id']).merge(df_all, how = 'left')
    #    df_test = predict_proba(pd.DataFrame(cust_test, columns=['customer_id']).merge(df_all, how='left'))
    #    df_test.rename(columns={
    #        'customer_id': '_ID_',
    #        'is_home:add_lat': '_HOME_LAT_',
    #        'is_home:add_lon': '_HOME_LON_',
    #        'is_work:add_lat': '_WORK_LAT_',
    #        'is_work:add_lon': '_WORK_LON_'}, inplace=True)
    #    df_test = df_test[['_ID_', '_WORK_LAT_', '_WORK_LON_', '_HOME_LAT_', '_HOME_LON_']]
    #    ​
    #    df_test.head()
    #    _ID_
    #    _WORK_LAT_
    #    _WORK_LON_
    #    _HOME_LAT_
    #    _HOME_LON_
    #    0
    #    000216
    #    83
    #    ccb416637fe9a4cd35e4606e
    #    55.026001
    #    82.915001
    #    55.041073
    #    82.980629
    #    1
    #    0002
    #    d0f8a642272b41c292c12ab6e602
    #    44.033001
    #    42.835999
    #    44.036594
    #    42.855629
    #    2
    #    0004
    #    d182d9fede3ba2534b2d5e5ad27e
    #    43.585999
    #    39.723999
    #    43.572186
    #    39.734737
    #    3
    #    000
    #    8
    #    c2445518c9392cb356c5c3db3392
    #    51.530449
    #    46.033218
    #    51.533936
    #    46.025490
    #    4
    #    000
    #    b373cc4969c0be8e0933c08da67e1
    #    56.319836
    #    43.925976
    #    56.247688
    #    43.463734
    #    Формируем
    #    submission - файл
    #
    #    # Заполняем пропуски
    #    df_ = pd.read_csv('../data/test_set.csv', dtype=dtypes, usecols=['customer_id'])
    #    submission = pd.DataFrame(df_['customer_id'].unique(), columns=['_ID_'])
    #    ​
    #    submission = submission.merge(df_test, how='left').fillna(0)
    #    # Пишем файл submission
    #    submission.to_csv('../submissions/base_16_644_353.csv', index=None)
    #
    #    submission_2 = pd.read_csv('../submissions/base_11_625_34.csv')
    #
    #    submission.head()
    #
    #    submission_2.head()
    #
    #    submission_3 = submission_2.copy()
    #    submission_3['_WORK_LAT_'] = (submission['_WORK_LAT_'] + submission_2['_WORK_LAT_']) / 2
    #    submission_3['_WORK_LON_'] = (submission['_WORK_LON_'] + submission_2['_WORK_LON_']) / 2
    #    submission_3['_HOME_LAT_'] = (submission['_HOME_LAT_'] + submission_2['_HOME_LAT_']) / 2
    #    submission_3['_HOME_LON_'] = (submission['_HOME_LON_'] + submission_2['_HOME_LON_']) / 2
    #    submission_3.to_csv('../submissions/base_15and12/2.csv', index=None)
    #    submission_3 = submission_2.copy()
    #    submission_3['_WORK_LAT_'] = (submission['_WORK_LAT_'] + submission_2['_WORK_LAT_']) / 2
    #    submission_3['_WORK_LON_'] = (submission['_WORK_LON_'] + submission_2['_WORK_LON_']) / 2
    #    submission_3['_HOME_LAT_'] = (submission['_HOME_LAT_'] + submission_2['_HOME_LAT_']) / 2
    #    submission_3['_HOME_LON_'] = (submission['_HOME_LON_'] + submission_2['_HOME_LON_']) / 2
    #    submission_3.to_csv('../submissions/base_15and12/2.csv', index=None)
    #
    return src

def update_last_partition(dst, from_dt, to_dt):
    prev_day = datetime.strptime(from_dt, '%Y-%m-%d') - timedelta(days=1)
    res = spark.table(dst["d_train"]).checkpoint()
    res = res.where(res.day == to_dt)
    res = res.withColumn("period_to_dt", f.lit(prev_day)).withColumn("day", f.lit(prev_day.strftime('%Y-%m-%d')))
    res.coalesce(8).write.format("orc").insertInto(dst["d_train"], overwrite=True)


def calc_07(src, dst, from_dt, to_dt):
    res = algo(src, from_dt, to_dt)
    res.coalesce(8).write.format("orc").insertInto(dst["d_subway_entrance"], overwrite=True)


def sandbox_src():
    return {
        "psg_train": spark.table("sandbox_mck.train"),
        "psg_test": spark.table("sandbox_mck.test"),
        "psg_dev": spark.table("sandbox_mck.dev")
    }


def sandbox_dst():
    return {
        "psg_result": "sandbox_mck.psg_result"
    }


def prod_src():
    return {
        "psg_train": spark.table("prod_data.psg_train"),
        "psg_test": spark.table("prod_data.psg_test"),
        "psg_dev": spark.table("prod_data.psg_dev")
    }


def prod_dst():
    return {
        "psg_result": "prod_data.psg_result"
    }


if __name__ == '__main__':
    spark = SparkSession.builder.appName("calc_07_task").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    hivecontext = HiveContext(spark.sparkContext)
    hivecontext.setConf("hive.exec.dynamic.partition", "true")
    hivecontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setCheckpointDir("hdfs:///user/airflow/psg/calc_07_task")

    opts = {
        'from_dt': sys.argv[1],
        "to_dt": "9999-12-31"
    }

    update_last_partition(prod_dst(), opts["from_dt"], opts["to_dt"])
    calc_07(prod_src(), prod_dst(), opts["from_dt"], opts["to_dt"])

