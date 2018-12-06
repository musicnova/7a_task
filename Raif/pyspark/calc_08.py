import sys
from datetime import timedelta, datetime


from pyspark import HiveContext
from pyspark.sql import functions as f, SparkSession


def algo(src, from_dt, to_dt):
    res = steps(src, from_dt, to_dt)
    return res


def steps(src, from_dt, to_dt):
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
    #    Populating
    #    the
    #    interactive
    #    namespace
    #    from numpy and matplotlib
    #
    #    # Определим типы колонок для экономии памяти
    #    dtypes = {
    #        'transaction_date': str,
    #        'atm_address': str,
    #        'country': str,
    #        'city': str,
    #        'amount': np.float32,
    #        'currency': np.float32,
    #        'mcc': str,
    #        'customer_id': str,
    #        'pos_address': str,
    #        'atm_address': str,
    #        'pos_lat': np.float32,
    #        'pos_lon': np.float32,
    #        'atm_lat': np.float32,
    #        'atm_lon': np.float32,
    #        'home_lat': np.float32,
    #        'home_lon': np.float32,
    #        'work_lat': np.float32,
    #        'work_lon': np.float32,
    #    }
    #    df_all = pd.read_csv('../data/df_all.csv', dtype=dtypes)
    #    Обрабатываем
    #    дату
    #    транзакции
    #    и
    #    категориальные
    #    признаки
    #
    #    df_all['currency'] = df_all['currency'].fillna(-1).astype(np.int32)
    #    df_all['mcc'] = df_all['mcc'].apply(lambda x: int(x.replace(',', ''))).astype(np.int32)
    #    df_all['city'] = df_all['city_name'].factorize()[0].astype(np.int32)
    #    df_all['country'] = df_all['country'].factorize()[0].astype(np.int32)
    #
    #    df_all.shape
    #    Фичи
    #    для
    #    даты
    #
    #    # удаляем транзакции без даты
    #    df_all = df_all[~df_all['transaction_date'].isnull()]
    #    df_all['transaction_date'] = pd.to_datetime(df_all['transaction_date'], format='%Y-%m-%d')
    #    df_all.shape
    #
    #    df_all['month'] = df_all.transaction_date.dt.month
    #    df_all['day'] = df_all.transaction_date.dt.day
    #    df_all['dayofyear'] = df_all.transaction_date.dt.dayofyear
    #    df_all['dayofweek'] = df_all.transaction_date.dt.dayofweek
    #    df_all.shape
    #
    #    # праздники
    #    holidays_df = pd.read_csv('../data/internal/all_holidays.csv', header=None)
    #    holidays_df[0] = pd.to_datetime(holidays_df[0])
    #    holidays_df = holidays_df[holidays_df[0].dt.year == 2017]
    #    holidays = holidays_df[0].dt.dayofyear.values
    #    df_all['is_weekend'] = (df_all.dayofweek >= 6).astype(np.int8)
    #    df_all['is_state_holiday'] = df_all['dayofyear'].isin(holidays).astype(np.int8)
    #    df_all['is_holiday'] = df_all['is_weekend'] | df_all['is_state_holiday']
    #    df_all.shape
    #    Приводим
    #    адрес
    #    транзакции
    #    для
    #    pos
    #    и
    #    atm - транзакций
    #    к
    #    единообразному
    #    виду
    #    Просто
    #    объединяем
    #    в
    #    одну
    #    колонку
    #    и
    #    добавляем
    #    фичу - это
    #    атм
    #    или
    #    пос
    #
    #    df_all['is_atm'] = (~df_all['atm_lat'].isnull()).astype(np.int8)
    #    df_all['is_pos'] = (~df_all['pos_lat'].isnull()).astype(np.int8)
    #    ​
    #    df_all['add_lat'] = df_all['atm_lat'].fillna(0) + df_all['pos_lat'].fillna(0)
    #    df_all['add_lon'] = df_all['atm_lon'].fillna(0) + df_all['pos_lon'].fillna(0)
    #    ​
    #    df_all.drop(['atm_lat', 'atm_lon', 'pos_lat', 'pos_lon'], axis=1, inplace=True)
    #    ​
    #    df_all = df_all[~((df_all['add_lon'] == 0) & (df_all['add_lon'] == 0))]
    #    df_all.shape
    #
    #    % % time
    #    # грязный хак, чтобы не учить КНН на новом юзере каждый раз
    #    df_all['fake_customer_id'] = (pd.factorize(df_all.customer_id)[0] + 1) * 100
    #    ​
    #    points = df_all[['fake_customer_id', 'add_lat', 'add_lon']].drop_duplicates().values
    #    neigh = NearestNeighbors(2, radius=100000)
    #    ​
    #    # расстояние до уникальных точек
    #    # neigh.fit(np.unique(points, axis=1))
    #    neigh.fit(points)
    #    ​
    #    distances, indices = neigh.kneighbors(df_all[['fake_customer_id', 'add_lat', 'add_lon']].values)
    #    df_all['distance_to_nearest_point'] = distances[:, 1]
    #    del df_all['fake_customer_id']
    #
    #    # кластерные фичи
    #    df_cluster = pd.read_csv('../data/df_cluster.csv')
    #    df_cluster.reset_index(drop=True, inplace=True)
    #    df_all.reset_index(drop=True, inplace=True)
    #    df_all = pd.concat([df_all, df_cluster.iloc[:, 3:]], axis=1)
    #    df_cluster.head()
    #    Генерируем
    #    признаки
    #    is_home, is_work
    #    TODO: удалить
    #    чуваков
    #    у
    #    которых
    #    несколько
    #    домов
    #
    #    lat = df_all['home_lat'] - df_all['add_lat']
    #    lon = df_all['home_lon'] - df_all['add_lon']
    #    ​
    #    df_all['is_home'] = (np.sqrt((lat ** 2) + (lon ** 2)) <= 0.02).astype(np.int8)
    #    df_all['has_home'] = (~df_all['home_lon'].isnull()).astype(np.int8)
    #    ​
    #    lat = df_all['work_lat'] - df_all['add_lat']
    #    lon = df_all['work_lon'] - df_all['add_lon']
    #    df_all['is_work'] = (np.sqrt((lat ** 2) + (lon ** 2)) <= 0.02).astype(np.int8)
    #    df_all['has_work'] = (~df_all['work_lon'].isnull()).astype(np.int8)
    #    ​
    #    # df_all.drop(['work_lat','work_lon','home_lat','home_lon'], axis=1, inplace=True)
    #    Генерируем
    #    категориальный
    #    признак
    #    для
    #    адреса
    #
    #    df_all['address'] = df_all['add_lat'].apply(lambda x: "%.02f" % x) + ';' + df_all['add_lon'].apply(
    #        lambda x: "%.02f" % x)
    #    df_all['address'] = df_all['address'].factorize()[0].astype(np.int32)
    #
    #    df_all.sort_values(by=['customer_id', 'address', 'dayofyear'], inplace=True)
    #
    #    def get_max_following_equal(arr, atype='eq'):
    #        '''
    #        types = eq,  eq_gr, eq_gr_unique
    #        '''
    #        arr = arr.values
    #        val_cur = 1
    #        val_max = 1
    #        if atype == 'eq_gr_unique':
    #            arr = np.unique(arr)
    #
    #        for i in range(arr.size - 1):
    #            if atype in ['eq_gr', 'eq_gr_unique']:
    #                if arr[i] + 1 >= arr[i + 1]:
    #                    val_cur += 1
    #                else:
    #                    val_max = max(val_cur, val_max)
    #                    val_cur = 1
    #            else:
    #                if arr[i] == arr[i + 1]:
    #                    val_cur += 1
    #                else:
    #                    val_max = max(val_cur, val_max)
    #                    val_cur = 1
    #        return val_max
    #
    #    # макс покупок подряд в день
    #    gb = df_all.groupby(['customer_id', 'address'])
    #    df_eq = gb['dayofyear'].apply(lambda x: get_max_following_equal(x)).reset_index()
    #    df_eq.rename(columns={'dayofyear': 'dayofyear_streak_inday'}, inplace=True)
    #    df_all = pd.merge(df_all, df_eq, on=['customer_id', 'address'], how='left')
    #    # макс покупок дней подряд
    #    gb = df_all.groupby(['customer_id', 'address'])
    #    df_eq = gb['dayofyear'].apply(lambda x: get_max_following_equal(x, atype='eq_gr')).reset_index()
    #    df_eq.rename(columns={'dayofyear': 'dayofyear_streak'}, inplace=True)
    #    df_all = pd.merge(df_all, df_eq, on=['customer_id', 'address'], how='left')
    #    # макс дней подряд
    #    gb = df_all.groupby(['customer_id', 'address'])
    #    df_eq = gb['dayofyear'].apply(lambda x: get_max_following_equal(x, atype='eq_gr_unique')).reset_index()
    #    df_eq.rename(columns={'dayofyear': 'dayofyear_streak_days'}, inplace=True)
    #    df_all = pd.merge(df_all, df_eq, on=['customer_id', 'address'], how='left')
    #
    #    ​
    #
    #    def get_num_closer(vals, unique=False, dist=0.02):
    #        d = pairwise_distances(vals)
    #        v = (d < dist).sum(axis=1)
    #        if unique:
    #            v -= (d == 0).sum(axis=1)
    #        return pd.DataFrame(v, index=vals.index, columns=['num_neigh_dist{}_un{}'.format(dist, unique)])
    #
    #    ​
    #
    #    def get_ratio_closer(vals, unique=False, dist=0.02):
    #        d = pairwise_distances(vals)
    #        v = (d < dist).mean(axis=1)
    #        if unique:
    #            v -= (d == 0).mean(axis=1)
    #        return pd.DataFrame(v, index=vals.index, columns=['ratio_neigh_dist{}_un{}'.format(dist, unique)])
    #
    #    ​
    #
    #    def get_num_far(vals, unique=False, dist=0.02):
    #        d = pairwise_distances(vals)
    #        v = (d >= dist).sum(axis=1)
    #        return pd.DataFrame(v, index=vals.index, columns=['num_far_dist{}_un{}'.format(dist, unique)])
    #
    #    ​
    #    ​
    #
    #    def get_median_closer(vals, unique=False, dist=0.02):
    #        ind = vals.index
    #        vals = vals.values
    #        d = pairwise_distances(vals)
    #        v = (d < dist)
    #        if unique:
    #            v = (d < dist) & (d != 0)
    #        medians = []
    #        for i in range(len(vals)):
    #            medians.append(np.median(vals[v[i]], axis=0))
    #
    #    ​
    #    c1 = 'median_dist{}_un{}_lat'.format(dist, unique)
    #    c2 = 'median_dist{}_un{}_lon'.format(dist, unique)
    #    c3 = 'median_dist{}_un{}_lat_diff'.format(dist, unique)
    #    c4 = 'median_dist{}_un{}_lon_diff'.format(dist, unique)
    #    c5 = 'median_dist{}_un{}_diff'.format(dist, unique)
    #    df_ = pd.DataFrame(medians, index=ind, columns=[c1, c2])
    #    df_[c3] = np.abs(df_[c1] - vals[:, 0])
    #    df_[c4] = np.abs(df_[c2] - vals[:, 1])
    #    df_[c5] = df_[c3] + df_[c4]
    #    return df_
    #
    #
    #df_all['add_lat_'] = (df_all['add_lat'] * 30).astype(np.int32)
    #df_all['add_lon_'] = (df_all['add_lon'] * 30).astype(np.int32)
    #
    #
    #def get_num_closer_complex(vals, unique=False, dist=0.01):
    #    d = pairwise_distances(vals)
    #    v = (d < dist).sum(axis=1)
    #    if unique:
    #        v -= (d == 0).sum(axis=1)
    #    df_ = pd.DataFrame(v, index=vals.index, columns=['num_neigh_dist{}_un{}'.format(dist, unique)])
    #    for dist in [0.02, 0.03]:
    #        v = (d < dist).sum(axis=1)
    #        if unique:
    #            v -= (d == 0).sum(axis=1)
    #        df_['num_neigh_dist{}_un{}'.format(dist, unique)] = v
    #    for dist in [0.01, 0.02, 0.03]:
    #        v = (d < dist).mean(axis=1)
    #        if unique:
    #            v -= (d == 0).mean(axis=1)
    #        df_['ratio_neigh_dist{}_un{}'.format(dist, unique)] = v
    #
    #    return df_
    #
    #​
    #df_clos = df_all.groupby(['add_lat_', 'add_lon_'])[['add_lat',
    #                                                    'add_lon']].progress_apply(
    #    lambda x: get_num_closer_complex(x, False, 0.01))
    #df_clos = df_clos.add_prefix('all_df_')
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #
    #% % time
    ## медианы в радиусе
    #​
    ## df_clos = df_all.groupby('customer_id')[['add_lat',
    ##                                          'add_lon']].apply(lambda x: get_median_closer(x, False, 0.01))
    ## df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    ## df_clos = df_all.groupby('customer_id')[['add_lat',
    ##                                          'add_lon']].apply(lambda x: get_median_closer(x, True, 0.01))
    ## df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    #df_clos = df_all.groupby('customer_id')[['add_lat',
    #                                         'add_lon']].apply(lambda x: get_median_closer(x, False, 0.02))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    #df_clos = df_all.groupby('customer_id')[['add_lat',
    #                                         'add_lon']].apply(lambda x: get_median_closer(x, True, 0.02))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    ## df_clos = df_all.groupby('customer_id')[['add_lat',
    ##                                          'add_lon']].apply(lambda x: get_median_closer(x, False, 0.05))
    ## df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    ## df_clos = df_all.groupby('customer_id')[['add_lat',
    ##                                          'add_lon']].apply(lambda x: get_median_closer(x, True, 0.05))
    ## df_all = pd.concat([df_all, df_clos], axis=1)
    #
    ## кол-ва соседей за радиусом
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_far(x, False, 0.01))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_far(x, False, 0.02))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_far(x, False, 0.03))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_far(x, False, 0.04))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_far(x, False, 0.05))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_far(x, False, 0.1))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_far(x, False, 1))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #
    ## кол-ва соседей внутри радиуса
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_closer(x, False, 0.01))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_closer(x, True, 0.01))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(get_num_closer)
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_closer(x, True, 0.02))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_closer(x, False, 0.03))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_closer(x, True, 0.03))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_closer(x, False, 0.04))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_num_closer(x, True, 0.04))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #
    ## доли соседей внутри радиуса
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, False, 0.01))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, True, 0.01))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, False, 0.02))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, True, 0.02))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, False, 0.03))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, True, 0.03))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #​
    #​
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, False, 0.04))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #df_clos = df_all.groupby('customer_id')[['add_lat', 'add_lon']].apply(lambda x: get_ratio_closer(x, True, 0.04))
    #df_all = pd.concat([df_all, df_clos], axis=1)
    #Генерируем
    #абонентские
    #фичи
    #отвечающие
    #за
    #соотношения
    #между
    #точками
    #
    #df_all = df_all.merge(df_all.groupby('customer_id')['amount'].count().reset_index(name='cid_trans_count'), how='left')
    #df_all['cid_trans_count'] = df_all['cid_trans_count'].astype(np.int32)
    #​
    #df_all = df_all.merge(df_all.groupby('customer_id')['amount'].agg('sum').reset_index(name='cid_trans_sum'), how='left')
    #df_all['cid_trans_sum'] = df_all['cid_trans_sum'].astype(np.float32)
    #
    #
    #def add_count_sum_ratios(df_all, col):
    #    col_count = 'cid_{}_trans_count'.format(col)
    #    col_sum = 'cid_{}_trans_sum'.format(col)
    #    df_ = df_all.groupby(['customer_id', col])['amount'].count().reset_index(name=col_count)
    #    df_all = df_all.merge(df_, how='left')
    #    df_all[col_count] = df_all[col_count].astype(np.int32)
    #    df_all['ratio_{}_count'.format(col)] = df_all[col_count] / df_all['cid_trans_count']
    #
    #    df_ = df_all.groupby(['customer_id', col])['amount'].agg('sum').reset_index(name=col_sum)
    #    df_all = df_all.merge(df_, how='left')
    #    df_all[col_sum] = df_all[col_sum].astype(np.float32)
    #    df_all['ratio_{}_sum'.format(col)] = df_all[col_sum] / df_all['cid_trans_sum']
    #    return df_all
    #
    #
    #df_all = add_count_sum_ratios(df_all, 'address')
    #df_all = add_count_sum_ratios(df_all, 'terminal_id')
    #df_all = add_count_sum_ratios(df_all, 'mcc')
    #df_all = add_count_sum_ratios(df_all, 'is_holiday')
    #df_all = add_count_sum_ratios(df_all, 'city')
    #
    #df_all.shape
    #
    ## df_all.to_csv('../data/df_all_3983.csv', index=None)
    #
    #df_all = pd.read_csv('../data/df_all_3983.csv')
    #
    #for c in tqdm(df_all.columns):
    #    if df_all[c].dtype == np.int64:
    #        df_all[c] = df_all[c].astype(np.int32)
    #    if df_all[c].dtype == np.float64:
    #        df_all[c] = df_all[c].astype(np.float32)
    #100 % |██████████ | 102 / 102[00:15 < 00:00, 6.69
    #it / s]
    #Мои
    #фичи
    #
    ## добавим признаки после групбая
    #df_gb = df_all[['customer_id', 'amount', 'add_lat', 'add_lon']].groupby('customer_id')
    #coord_stat_df = df_gb.agg(['mean', 'max', 'min'])
    #coord_stat_df['transactions_per_user'] = df_gb.agg('size')
    #coord_stat_df.columns = ['_'.join(col).strip() for col in coord_stat_df.columns.values]
    #coord_stat_df = coord_stat_df.astype(np.float32)
    #coord_stat_df.reset_index(inplace=True)
    #df_all = pd.merge(df_all, coord_stat_df, on='customer_id', how='left')
    #
    #cols = ['add_lat', 'add_lon']
    #types = ['min', 'max', 'mean']
    #for c in cols:
    #    for t in types:
    #        df_all['{}_diff_{}'.format(c, t)] = np.abs(df_all[c] - df_all['{}_{}'.format(c, t)], dtype=np.float32)
    #
    #df_all = df_all.loc[:, ~df_all.columns.duplicated()]
    #
    ## разности
    #df_all['lat_diff_cluster_lat'] = np.abs(df_all['add_lat'] - df_all['cl_lat'], dtype=np.float32)
    #df_all['lon_diff_cluster_lon'] = np.abs(df_all['add_lon'] - df_all['cl_lon'], dtype=np.float32)
    #df_all['lon_diff_cluster'] = (df_all['lat_diff_cluster_lat'] + df_all['lon_diff_cluster_lon']).astype(np.float32)
    #Фичи
    #mcc
    #
    ## категории
    #df_all['mcc_str'] = df_all['mcc'].astype(str).str.rjust(4, '0')
    #df_mcc = pd.read_csv('../data/internal/mcc.csv')
    #df_mcc = df_mcc.iloc[1:, :3]
    #df_mcc.columns = ['mcc_str', 'mcc_cat1', 'mcc_cat2']
    #df_mcc.drop_duplicates(subset=['mcc_str'], inplace=True)
    #df_mcc['mcc_cat1'] = pd.factorize(df_mcc['mcc_cat1'])[0].astype(np.int32)
    #df_mcc['mcc_cat2'] = pd.factorize(df_mcc['mcc_cat2'])[0].astype(np.int32)
    #df_mcc.fillna('none', inplace=True)
    #df_all = pd.merge(df_all, df_mcc, on='mcc_str', how='left')
    #del df_all['mcc_str']
    #df_mcc.head()
    #mcc_str
    #mcc_cat1
    #mcc_cat2
    #1
    #0001 - 1
    #0
    #2
    #0002 - 1
    #0
    #3
    #0003 - 1
    #0
    #4
    #0004 - 1
    #0
    #5
    #0005 - 1
    #0
    #
    ## df_mcc['mcc_cat1'].fillna(-1, inplace=True)
    ## df_mcc['mcc_cat2'].fillna(-1, inplace=True)
    #​
    ## df_all = add_count_sum_ratios(df_all, 'mcc_cat1')
    ## df_all = add_count_sum_ratios(df_all, 'mcc_cat2')
    #
    #import geopandas as gpd
    #from shapely.geometry import Point, Polygon
    #
    #mos_shp = gpd.read_file('../data/internal/demography.shp')
    #​
    #_pnts = [Point(vals.T) for vals in df_all[df_all.city_name == 'Москва'][['add_lon', 'add_lat']].values]
    #pnts = gpd.GeoDataFrame(geometry=_pnts)
    #pnts.crs = mos_shp.crs
    #​
    #mos_shp.drop(['NAME', 'ABBREV_AO'], 1, inplace=True)
    #mos_shp['area'] = mos_shp['geometry'].area
    #for c in mos_shp.columns:
    #    if c not in ['geometry', 'area'] and 'index' not in c:
    #        mos_shp[c + 'dens'] = mos_shp[c] / mos_shp['area']
    #
    #% % time
    #cities_with_country = gpd.sjoin(pnts, mos_shp, how="left", op='intersects')
    #CPU
    #times: user
    #41.6
    #s, sys: 353
    #ms, total: 41.9
    #s
    #Wall
    #time: 41.9
    #s
    #
    #cols = cities_with_country.drop(['geometry', 'index_right'], 1).columns
    #for c in cols:
    #    df_all[c] = -1
    #df_all.loc[df_all.city_name == 'Москва', cols] = cities_with_country
    #
    ## частота mcc
    #df_mcc = df_all['mcc'].value_counts(normalize=True).reset_index()
    #df_mcc.columns = ['mcc', 'mcc_freq']
    #df_all = pd.merge(df_all, df_mcc, on='mcc', how='left')
    #
    ## метро
    #mos_metro = pd.read_csv('../data/internal/moscow_metro.csv')
    #pet_metro = pd.read_csv('../data/internal/peter_metro.csv')
    #df_metro = pd.concat([mos_metro, pet_metro])
    #​
    #vals1 = df_all[['add_lat', 'add_lon']].values
    #vals2 = df_metro[['metro_lat', 'metro_lon']].values
    #X = pairwise_distances(vals1, vals2)
    #dist_to_min_metro = X.min(axis=1)
    #​
    #X[X == 0] = 10000
    #df_all['dist_to_minmetro'] = X.min(axis=1)
    #df_all['metro_in_01'] = (X < 0.01).sum(axis=1)
    #df_all['metro_in_001'] = (X < 0.001).sum(axis=1)
    #df_all['metro_in_02'] = (X < 0.02).sum(axis=1)
    #df_all['metro_in_005'] = (X < 0.005).sum(axis=1)
    #df_all['metro_in_03'] = (X < 0.03).sum(axis=1)
    #
    #df_cik = pd.read_csv('../data/internal/cik_uik.csv')
    #df_cik.dropna(subset=['lat_ik'], inplace=True)
    #df_cik.dropna(subset=['lon_ik'], inplace=True)
    #​
    #df_cik = df_cik[df_cik['lon_ik'] < 45]
    #vals1 = df_all[['add_lat', 'add_lon']].drop_duplicates().values.astype(np.float32)
    #df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #vals2 = df_cik[['lat_ik', 'lon_ik']].drop_duplicates().values.astype(np.float32)
    #​
    #vals2.shape
    #(37481, 2)
    #
    #X = pairwise_distances(vals1, vals2)
    #X[X == 0] = 10000
    #​
    #df_vals['dist_to_ciktro'] = X.min(axis=1)
    #df_vals['cik_in_01'] = (X < 0.01).sum(axis=1)
    #df_vals['cik_in_001'] = (X < 0.001).sum(axis=1)
    #df_vals['cik_in_02'] = (X < 0.02).sum(axis=1)
    #df_vals['cik_in_005'] = (X < 0.005).sum(axis=1)
    #df_vals['cik_in_03'] = (X < 0.03).sum(axis=1)
    #
    #df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #del df_vals['add_lat']
    #del df_vals['add_lon']
    #​
    #df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #del X
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #
    ## погода
    ## буду смотреть погоду в 18-00
    #w1 = pd.read_csv('../data/internal/weather/moscow.csv', sep=';', index_col=False)
    #w1['city_name'] = 'Москва'
    #w1['transaction_date'] = pd.to_datetime(w1['Local time in Moscow'], format='%d.%m.%Y %H:%M')
    #del w1['Local time in Moscow']
    #w1 = w1[w1.transaction_date.dt.hour == 18].reset_index()
    #w1['transaction_date'] = w1['transaction_date'].dt.date
    #​
    #w2 = pd.read_csv('../data/internal/weather/peter.csv', sep=';', index_col=False)
    #w2['city_name'] = 'Санкт-Петербург '
    #w2['transaction_date'] = pd.to_datetime(w2['Local time in Moscow'], format='%d.%m.%Y %H:%M')
    #del w2['Local time in Moscow']
    #w2 = w2[w2.transaction_date.dt.hour == 18].reset_index()
    #w2['transaction_date'] = w2['transaction_date'].dt.date
    #​
    #df_weather = pd.concat([w1, w2], axis=0).reset_index()
    #df_weather['transaction_date'] = pd.to_datetime(df_weather['transaction_date'])
    #​
    #cn = df_weather['city_name']  # hardcode
    #df_weather = df_weather.select_dtypes(exclude=['object'])
    #df_weather['city_name'] = cn
    #for c in df_weather:
    #    if df_weather[c].isnull().mean() > 0.9:
    #        del df_weather[c]
    ## df_weather = df_weather.add_prefix('weather_')
    #df_all = pd.merge(df_all, df_weather, on=['city_name', 'transaction_date'], how='left')
    #
    #df_all['mcc_rm'] = df_all['mcc']
    #df_all.loc[~df_all['mcc_rm'].isin(df_all['mcc_rm'].value_counts().iloc[:32].index.values), 'mcc_rm'] = 99999
    #​
    #df_all['mcc_rm_cat1'] = df_all['mcc_cat1']
    #df_all.loc[~df_all['mcc_rm_cat1'].isin(df_all['mcc_rm_cat1'].value_counts().iloc[:32].index.values),
    #           'mcc_rm_cat1'] = 99999
    #
    #df_all = pd.concat([df_all,
    #                    pd.get_dummies(df_all['mcc_rm'], prefix='mcc_rm_ohe').astype(np.int8)], axis=1)
    #del df_all['mcc_rm']
    #
    #df_all = pd.concat([df_all,
    #                    pd.get_dummies(df_all['mcc_rm_cat1'], prefix='mcc_rm_cat1_ohe').astype(np.int8)], axis=1)
    #del df_all['mcc_rm_cat1']
    #​
    #df_all = pd.concat([df_all,
    #                    pd.get_dummies(df_all['mcc_cat2'], prefix='mcc_cat2_ohe').astype(np.int8)], axis=1)
    #del df_all['mcc_cat2']
    #
    #df_all = df_all.reset_index(drop=True)
    #
    ## сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #mcc_cols = [c for c in df_all.columns if 'mcc_rm_ohe' in c]
    #df_mcc = df_all.groupby('customer_id')[mcc_cols].agg(['mean', 'sum'])
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc = df_mcc.astype(np.float32)
    #df_mcc = df_mcc.reset_index()
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #
    ## по объемам
    #mcc_cols = [c for c in df_all.columns if 'mcc_rm_ohe' in c and 'mean' not in c and 'sum' not in c]
    #mcc_cols_ = [c + '_amount' for c in mcc_cols]
    #for c in mcc_cols:
    #    df_all[c + '_amount'] = df_all[c] * df_all['amount']
    #
    #df_mcc = df_all.groupby('customer_id')[mcc_cols_].agg(['mean', 'sum'])
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc = df_mcc.astype(np.float32)
    #df_mcc = df_mcc.reset_index()
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #​
    ## df_all['add_lat_'] = (df_all['add_lat'] * 40).astype(np.int32)
    ## df_all['add_lon_'] = (df_all['add_lon'] * 40).astype(np.int32)
    #​
    ## df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols_].agg(['mean', 'sum'])
    ## df_mcc = df_mcc.add_suffix('_40coord')
    ## df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    ## df_mcc = df_mcc.astype(np.float32)
    ## df_mcc.reset_index(inplace=True)
    ## df_mcc.head()
    ## df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #​
    ## del df_all['add_lat_']
    ## del df_all['add_lon_']
    #
    ## по объемам
    #mcc_cols = [c for c in df_all.columns if 'mcc_rm_cat1_ohe' in c and 'mean' not in c and 'sum' not in c]
    #mcc_cols_ = [c + '_amount' for c in mcc_cols]
    #for c in mcc_cols:
    #    df_all[c + '_amount'] = df_all[c] * df_all['amount']
    #
    #df_mcc = df_all.groupby('customer_id')[mcc_cols_].agg(['mean', 'sum'])
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc = df_mcc.astype(np.float32)
    #df_mcc = df_mcc.reset_index()
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #​
    ## df_all['add_lat_'] = (df_all['add_lat'] * 40).astype(np.int32)
    ## df_all['add_lon_'] = (df_all['add_lon'] * 40).astype(np.int32)
    #​
    ## df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols_].agg(['mean', 'sum'])
    ## df_mcc = df_mcc.add_suffix('_40coord')
    ## df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    ## df_mcc = df_mcc.astype(np.float32)
    ## df_mcc.reset_index(inplace=True)
    ## df_mcc.head()
    ## df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #​
    ## del df_all['add_lat_']
    ## del df_all['add_lon_']
    #
    ## сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    ## mcc_cols = [c for c in df_all.columns if 'mcc_cat1' in c]
    ## df_mcc = df_all.groupby('customer_id')[mcc_cols].agg(['mean'])
    ## df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    ## df_mcc.reset_index(inplace=True)
    ## df_mcc.head()
    ## df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #
    ## сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #mcc_cols = [c for c in df_all.columns if 'mcc_cat2_ohe' in c]
    #df_mcc = df_all.groupby('customer_id')[mcc_cols].agg(['mean', 'sum'])
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc = df_mcc.astype(np.float32)
    #df_mcc.reset_index(inplace=True)
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #
    ## РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА
    #df_all['add_lat_'] = (df_all['add_lat'] * 40).astype(np.int32)
    #df_all['add_lon_'] = (df_all['add_lon'] * 40).astype(np.int32)
    #​
    #df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols].agg(['mean', 'sum'])
    #df_mcc = df_mcc.add_suffix('_40coord')
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc = df_mcc.astype(np.float32)
    #df_mcc.reset_index(inplace=True)
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #​
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #
    #mcc_cols = [c for c in df_all.columns if 'mcc_rm_ohe' in c and 'mean' not in c and 'sum' not in c]
    ## РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА
    #df_all['add_lat_'] = (df_all['add_lat'] * 100).astype(np.int32)
    #df_all['add_lon_'] = (df_all['add_lon'] * 100).astype(np.int32)
    #​
    #df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols].agg(['mean', 'sum'])
    #df_mcc = df_mcc.add_suffix('_100coord')
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc = df_mcc.astype(np.float32)
    #df_mcc.reset_index(inplace=True)
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #​
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #
    ## РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА (ПРОВЕРИЛ-ЛУЧШЕ РАБОТАЕТ НА БОЛЬШИХ УЧАСТКАХ)
    #df_all['add_lat_'] = (df_all['add_lat'] * 200).astype(np.int32)
    #df_all['add_lon_'] = (df_all['add_lon'] * 200).astype(np.int32)
    #​
    #df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols].agg(['mean', 'sum'])
    #df_mcc = df_mcc.add_suffix('_200coord')
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc = df_mcc.astype(np.float32)
    #df_mcc.reset_index(inplace=True)
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #​
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #
    ## РАСПРЕДЕЛЕНИЕ MCC В ОКРЕСТНОСТИ ЧУВАКА
    ## df_all['add_lat_'] = (df_all['add_lat'] * 100).astype(np.int32)
    ## df_all['add_lon_'] = (df_all['add_lon'] * 100).astype(np.int32)
    #​
    ## df_mcc = df_all.groupby(['add_lat_', 'add_lon_'])[mcc_cols].agg(['mean', 'sum'])
    ## df_mcc = df_mcc.add_suffix('_100coord')
    ## df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    ## df_mcc = df_mcc.astype(np.float32)
    ## df_mcc.reset_index(inplace=True)
    ## df_mcc.head()
    ## df_all = pd.merge(df_all, df_mcc, on=['add_lat_', 'add_lon_'], how='left')
    #​
    ## del df_all['add_lat_']
    ## del df_all['add_lon_']
    #Игрушки
    #с
    #адресами
    #
    #df_all['string'] = df_all['string'].fillna('')
    #df_all['string'] = df_all['string'].str.lower()
    #
    #df_all['has_street'] = df_all['string'].str.contains('улиц').astype(np.int8)
    #df_all['has_pereul'] = df_all['string'].str.contains('переул').astype(np.int8)
    #df_all['has_bulvar'] = df_all['string'].str.contains('бульв').astype(np.int8)
    #df_all['has_prospekt'] = df_all['string'].str.contains('проспект').astype(np.int8)
    #df_all['has_shosse'] = df_all['string'].str.contains('шосс').astype(np.int8)
    #​
    #df_all['has_torg'] = df_all['string'].str.contains('торгов').astype(np.int8)
    #df_all['has_bus'] = df_all['string'].str.contains('бизн').astype(np.int8)
    #Медианы
    #по
    #юзеру
    #и
    #по
    #без
    #дубликатов
    #
    #df_med = df_all.groupby('customer_id')['add_lat', 'add_lon'].agg('median').reset_index()
    #df_med.columns = ['customer_id', 'add_lat_median', 'add_lon_median']
    #df_all = pd.merge(df_all, df_med, on='customer_id', how='left')
    #
    #df_med = df_all.drop_duplicates(subset=['customer_id',
    #                                        'add_lat', 'add_lon']).groupby('customer_id')['add_lat', 'add_lon'].agg(
    #    'median').reset_index()
    #df_med.columns = ['customer_id', 'add_lat_median_unique', 'add_lon_median_unique']
    #df_all = pd.merge(df_all, df_med, on='customer_id', how='left')
    #
    #df_all['lat_diff_median'] = np.abs(df_all['add_lat'] - df_all['add_lat_median'])
    #df_all['lon_diff_median'] = np.abs(df_all['add_lon'] - df_all['add_lat_median'])
    #df_all['lat_diff_median_unique'] = np.abs(df_all['add_lat'] - df_all['add_lat_median_unique'])
    #df_all['lon_diff_median_unique'] = np.abs(df_all['add_lon'] - df_all['add_lon_median_unique'])
    #​
    #df_all['diff_median'] = df_all['lat_diff_median'] + df_all['lon_diff_median']
    #df_all['diff_median_unique'] = df_all['lat_diff_median_unique'] + df_all['lon_diff_median_unique']
    #
    #df_all.to_csv('../data/df_all_2lvl.csv', index=None)
    #
    #df_all.dtypes.to_csv('../data/df_all_2lvl_dtypes.csv')
    #OSM
    #https: // wiki.openstreetmap.org / wiki / RU: % D0 % 9
    #E % D0 % B1 % D1 % 8
    #A % D0 % B5 % D0 % BA % D1 % 82 % D1 % 8
    #B_ % D0 % BA % D0 % B0 % D1 % 80 % D1 % 82 % D1 % 8
    #B  # .D0.9A.D0.BE.D0.BC.D0.BC.D0.B5.D1.80.D1.87.D0.B5.D1.81.D0.BA.D0.B8.D0.B5
    #
    #import ogr
    #
    #driver = ogr.GetDriverByName('OSM')
    #data = driver.Open('../data/internal/map.osm')
    #
    #nlayer = data.GetLayerCount()  # 5
    #print(nlayer)
    #features = []
    #for i in range(nlayer):
    #    features += [x for x in data.GetLayerByIndex(i)]
    #5
    #fast_food
    #food_court
    #файзен
    #raiffeisen
    #railway
    #
    #расстояние
    #до
    #бизнес
    #центров
    #
    #coords = []
    #for f in tqdm(features):
    #    s = str(f.ExportToJson(as_object=True)).lower()
    #    if 'бизнес' in s and 'центр' in s:
    #        el = f.ExportToJson(as_object=True)['geometry']['coordinates'][0]
    #        if type(el) != float:
    #            coords.append(el)
    #100 % |██████████ | 268802 / 268802[01:04 < 00:00, 4137.29
    #it / s]
    #
    #vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #vals2 = np.array(coords, dtype=np.float32)
    #vals1.shape, vals2.shape
    #((160184, 2), (141, 2))
    #
    #X = pairwise_distances(vals1, vals2)
    #X[X == 0] = 10000
    #
    #suf = 'bc'
    #df_vals[suf + '_dist_to'] = X.min(axis=1)
    #df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #​
    #df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #del df_vals['add_lat']
    #del df_vals['add_lon']
    #​
    #df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #del X
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #до
    #фастфудов
    #http: // andrewgaidus.com / Convert_OSM_Data /
    #
    #driver = ogr.GetDriverByName('OSM')
    #data = driver.Open('../data/internal/map.osm')
    #layer_p = data.GetLayer('points')  # 5
    #features_p = [x for x in layer_p]
    #
    #coords = []
    #for f in tqdm(features_p):
    #    s = str(f.ExportToJson(as_object=True)).lower()
    #    if 'fast_food' in s:
    #        coords.append(f.ExportToJson(as_object=True)['geometry']['coordinates'])
    #100 % |██████████ | 133083 / 133083[00:29 < 00:00, 4464.30
    #it / s]
    #
    #vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #vals2 = np.array(coords, dtype=np.float32)
    #vals1.shape, vals2.shape
    #((160184, 2), (1304, 2))
    #
    #X = pairwise_distances(vals1, vals2)
    #X[X == 0] = 10000
    #
    #suf = 'fastfood'
    #df_vals[suf + '_dist_to'] = X.min(axis=1)
    #df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #​
    #df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #del df_vals['add_lat']
    #del df_vals['add_lon']
    #​
    #df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #del X
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #станции
    #
    #coords = []
    #for f in tqdm(features_p):
    #    s = str(f.ExportToJson(as_object=True)).lower()
    #    if 'railway' in s:
    #        coords.append(f.ExportToJson(as_object=True)['geometry']['coordinates'])
    #100 % |██████████ | 133083 / 133083[00:30 < 00:00, 4411.27
    #it / s]
    #
    #vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #vals2 = np.array(coords, dtype=np.float32)
    #vals1.shape, vals2.shape
    #((160184, 2), (4229, 2))
    #
    #X = pairwise_distances(vals1, vals2)
    #X[X == 0] = 10000
    #
    #suf = 'rail'
    #df_vals[suf + '_dist_to'] = X.min(axis=1)
    #df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #​
    #df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #del df_vals['add_lat']
    #del df_vals['add_lon']
    #​
    #df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #del X
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #райф
    #
    #coords = []
    #for f in tqdm(features_p):
    #    s = str(f.ExportToJson(as_object=True)).lower()
    #    if 'райф' in s or 'raiffeisen' in s:
    #        coords.append(f.ExportToJson(as_object=True)['geometry']['coordinates'])
    #100 % |██████████ | 133083 / 133083[00:29 < 00:00, 4541.89
    #it / s]
    #
    #vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #vals2 = np.array(coords, dtype=np.float32)
    #vals1.shape, vals2.shape
    #((160184, 2), (113, 2))
    #
    #X = pairwise_distances(vals1, vals2)
    #X[X == 0] = 10000
    #
    #suf = 'raif1'
    #df_vals[suf + '_dist_to'] = X.min(axis=1)
    #df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #​
    #df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #del df_vals['add_lat']
    #del df_vals['add_lon']
    #​
    #df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #del X
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #
    #coords = []
    #for f in tqdm(features):
    #    s = str(f.ExportToJson(as_object=True)).lower()
    #    if 'райф' in s or 'raiffeisen' in s:
    #        el = f.ExportToJson(as_object=True)['geometry']['coordinates'][0]
    #        if type(el) != float:
    #            coords.append(el)
    #100 % |██████████ | 268802 / 268802[01:03 < 00:00, 4200.28
    #it / s]
    #
    #vals1 = df_all[['add_lon', 'add_lat']].drop_duplicates().values.astype(np.float32)
    #df_vals = pd.DataFrame(vals1, columns=['add_lat', 'add_lon'])
    #vals2 = np.array(coords, dtype=np.float32)
    #vals1.shape, vals2.shape
    #((160184, 2), (1, 2))
    #
    #X = pairwise_distances(vals1, vals2)
    #X[X == 0] = 10000
    #
    #suf = 'raif2'
    #df_vals[suf + '_dist_to'] = X.min(axis=1)
    #df_vals[suf + '_in_01'] = (X < 0.01).sum(axis=1)
    #df_vals[suf + '_in_001'] = (X < 0.001).sum(axis=1)
    #df_vals[suf + '_in_02'] = (X < 0.02).sum(axis=1)
    #df_vals[suf + '_in_005'] = (X < 0.005).sum(axis=1)
    #df_vals[suf + '_in_03'] = (X < 0.03).sum(axis=1)
    #​
    #df_all['add_lat_'] = np.round(df_all['add_lat'] * 10000).astype(int)
    #df_all['add_lon_'] = np.round(df_all['add_lon'] * 10000).astype(int)
    #df_vals['add_lat_'] = np.round(df_vals['add_lat'] * 10000).astype(int)
    #df_vals['add_lon_'] = np.round(df_vals['add_lon'] * 10000).astype(int)
    #del df_vals['add_lat']
    #del df_vals['add_lon']
    #​
    #df_all = pd.merge(df_all, df_vals, on=['add_lat_', 'add_lon_'], how='left')
    #del X
    #del df_all['add_lat_']
    #del df_all['add_lon_']
    #
    #del vals2
    #LightGBM
    #
    #df_all.shape, df_all.columns.duplicated().sum()
    #((2315139, 865), 0)
    #
    #df_all = df_all.loc[:, ~df_all.columns.duplicated()]
    #
    #from sklearn.model_selection import train_test_split
    #​
    #ys = ['is_home', 'is_work']
    #drop_cols = ['atm_address', 'customer_id', 'pos_address', 'terminal_id', 'transaction_date',
    #             'is_home', 'has_home', 'is_work', 'has_work', 'is_train', 'city_name']
    #drop_cols += ['work_lat', 'work_lon', 'home_lat', 'home_lon', 'string']
    #​
    #drop_cols += ['pred:is_home', 'pred:is_work']
    ## cols = [c for c in df_all.columns if 'median_dist' in c]
    ## cols = [c for c in df_all.columns if 'lat' in c or 'lon' in c and 'diff' not in c and 'median' not in c]
    ## cols += ['address']
    ## drop_cols += cols
    #​
    #cols = [c for c in df_all.columns if 'mcc_ohe' in c and 'mean' not in c]
    ## cols += ['address']
    #drop_cols += cols
    #​
    #​
    #y_cols = ['is_home', 'is_work']
    #usecols = df_all.drop(drop_cols, 1, errors='ignore').columns
    #
    #params = {
    #    'objective': 'binary',
    #    'num_leaves': 511,
    #    'learning_rate': 0.01,
    #    'metric': 'binary_logloss',
    #    'feature_fraction': 0.8,
    #    'bagging_fraction': 0.8,
    #    'bagging_freq': 1,
    #    'num_threads': 12,
    #    'verbose': 0,
    #}
    #​
    #model = {}
    #
    #y_col = 'is_home'
    #​
    #cust_train = df_all[df_all['is_train'] == 1].groupby('customer_id')[y_col.replace('is_', 'has_')].max()
    #cust_train = cust_train[cust_train > 0].index
    #​
    #cust_train, cust_valid = train_test_split(cust_train, test_size=0.2, shuffle=True, random_state=111)
    #​
    #df_train = pd.DataFrame(cust_train, columns=['customer_id']).merge(df_all, how='left')
    #df_valid = pd.DataFrame(cust_valid, columns=['customer_id']).merge(df_all, how='left')
    #​
    #lgb_train = lgb.Dataset(df_train[usecols], df_train[y_col])
    #lgb_valid = lgb.Dataset(df_valid[usecols], df_valid[y_col])
    #​
    #gbm_h = lgb.train(params,
    #                  lgb_train,
    #                  valid_sets=[lgb_valid],
    #                  num_boost_round=2000,
    #                  verbose_eval=30,
    #                  early_stopping_rounds=100)
    #​
    #model[y_col] = gbm_h
    #Training
    #until
    #validation
    #scores
    #don
    #'t improve for 100 rounds.
    #[30]
    #valid_0
    #'s binary_logloss: 0.583284
    #[60]
    #valid_0
    #'s binary_logloss: 0.516217
    #[90]
    #valid_0
    #'s binary_logloss: 0.471301
    #[120]
    #valid_0
    #'s binary_logloss: 0.441173
    #[150]
    #valid_0
    #'s binary_logloss: 0.420588
    #[180]
    #valid_0
    #'s binary_logloss: 0.405912
    #[210]
    #valid_0
    #'s binary_logloss: 0.39543
    #[240]
    #valid_0
    #'s binary_logloss: 0.387864
    #[270]
    #valid_0
    #'s binary_logloss: 0.382508
    #[300]
    #valid_0
    #'s binary_logloss: 0.378329
    #[330]
    #valid_0
    #'s binary_logloss: 0.374994
    #[360]
    #valid_0
    #'s binary_logloss: 0.372869
    #[390]
    #valid_0
    #'s binary_logloss: 0.371166
    #[420]
    #valid_0
    #'s binary_logloss: 0.370165
    #[450]
    #valid_0
    #'s binary_logloss: 0.36959
    #[480]
    #valid_0
    #'s binary_logloss: 0.369145
    #[510]
    #valid_0
    #'s binary_logloss: 0.368703
    #[540]
    #valid_0
    #'s binary_logloss: 0.368653
    #[570]
    #valid_0
    #'s binary_logloss: 0.368604
    #[600]
    #valid_0
    #'s binary_logloss: 0.369003
    #[630]
    #valid_0
    #'s binary_logloss: 0.36913
    #Early
    #stopping, best
    #iteration is:
    #[536]
    #valid_0
    #'s binary_logloss: 0.368478
    #
    #y_col = 'is_work'
    #​
    #cust_train = df_all[df_all['is_train'] == 1].groupby('customer_id')[y_col.replace('is_', 'has_')].max()
    #cust_train = cust_train[cust_train > 0].index
    #​
    #cust_train, cust_valid = train_test_split(cust_train, test_size=0.2, shuffle=True, random_state=111)
    #​
    #​
    #​
    #df_train = pd.DataFrame(cust_train, columns=['customer_id']).merge(df_all, how='left')
    #df_valid = pd.DataFrame(cust_valid, columns=['customer_id']).merge(df_all, how='left')
    #​
    #lgb_train = lgb.Dataset(df_train[usecols], df_train[y_col])
    #lgb_valid = lgb.Dataset(df_valid[usecols], df_valid[y_col])
    #​
    #gbm_w = lgb.train(params,
    #                  lgb_train,
    #                  valid_sets=[lgb_valid],
    #                  num_boost_round=2000,
    #                  verbose_eval=30,
    #                  early_stopping_rounds=100)
    #​
    #model[y_col] = gbm_w
    #Training
    #until
    #validation
    #scores
    #don
    #'t improve for 100 rounds.
    #[30]
    #valid_0
    #'s binary_logloss: 0.563332
    #[60]
    #valid_0
    #'s binary_logloss: 0.485824
    #[90]
    #valid_0
    #'s binary_logloss: 0.435135
    #[120]
    #valid_0
    #'s binary_logloss: 0.401422
    #[150]
    #valid_0
    #'s binary_logloss: 0.379112
    #[180]
    #valid_0
    #'s binary_logloss: 0.363773
    #[210]
    #valid_0
    #'s binary_logloss: 0.35287
    #[240]
    #valid_0
    #'s binary_logloss: 0.346544
    #[270]
    #valid_0
    #'s binary_logloss: 0.342485
    #[300]
    #valid_0
    #'s binary_logloss: 0.340309
    #[330]
    #valid_0
    #'s binary_logloss: 0.339276
    #[360]
    #valid_0
    #'s binary_logloss: 0.339486
    #[390]
    #valid_0
    #'s binary_logloss: 0.339817
    #[420]
    #valid_0
    #'s binary_logloss: 0.340835
    #Early
    #stopping, best
    #iteration is:
    #[329]
    #valid_0
    #'s binary_logloss: 0.339185
    #Полезные
    #MCC
    #дом
    #6011 - финансы
    #5411 - придомовые
    #магазы
    #5814 - мак
    #5912 - аптеки
    #5921 - пиво
    #5499 - магазы
    #пяторочка
    #типа
    #5812 - рестроанчики
    #работа
    #
    #figsize(14, 10)
    #lgb.plot_importance(gbm_h, max_num_features=40)
    #
    #
    #def _best(x):
    #    ret = None
    #    for col in ys:
    #        pred = ('pred:%s' % col)
    #        if pred in x:
    #            i = (x[pred].idxmax())
    #            cols = [pred, 'add_lat', 'add_lon']
    #            if col in x:
    #                cols.append(col)
    #            tmp = x.loc[i, cols]
    #            tmp.rename({
    #                'add_lat': '%s:add_lat' % col,
    #                'add_lon': '%s:add_lon' % col,
    #            }, inplace=True)
    #            if ret is None:
    #                ret = tmp
    #            else:
    #                ret = pd.concat([ret, tmp])
    #    return ret
    #
    #​
    #​
    #
    #def predict_proba(dt, ys=['is_home', 'is_work']):
    #    for col in ys:
    #        pred = ('pred:%s' % col)
    #        dt[pred] = model[col].predict(dt[usecols])
    #    return dt.groupby('customer_id').apply(_best).reset_index()
    #
    #​
    #
    #def score(dt, ys=['is_home', 'is_work'], return_df=False):
    #    dt_ret = predict_proba(dt, ys)
    #    if return_df:
    #        return dt_ret
    #    mean = 0.0
    #    for col in ys:
    #        col_mean = dt_ret[col].mean()
    #        mean += col_mean
    #    if len(ys) == 2:
    #        mean = mean / len(ys)
    #    return mean
    #
    #
    #print("Train accuracy:", score(df_train, ys=['is_home']))
    #print("Test accuracy:", score(df_valid, ys=['is_home']))
    #​
    #print("Train accuracy:", score(df_train, ys=['is_work']))
    #print("Test accuracy:", score(df_valid, ys=['is_work']))
    #Train
    #accuracy: 0.6301502666020359
    #Test
    #accuracy: 0.6356589147286822
    #Train
    #accuracy: 0.5264178380998545
    #Test
    #accuracy: 0.3313953488372093
    #Train
    #accuracy: 0.5458070770722249
    #Test
    #accuracy: 0.5494186046511628
    #Train
    #accuracy: 0.4301987396994668
    #Test
    #accuracy: 0.3536821705426357
    #
    #Анализ
    #False - Negative
    #
    ## сколько вообще людей имеют хорошую точку
    #df_all[(df_all.is_train == 1)].groupby('customer_id')['is_work'].agg('max').mean()
    #
    #df_pred = score(df_valid, ys=['is_home'], return_df=True)
    #
    #df_pred.sample(5)
    #
    #cid = 'bf66305d0ec05abb6e6a6358acb8c2a1'
    #cid = df_pred[df_pred.is_home == 0].sample(1)['customer_id'].values[0]
    #​
    #df_an = df_all[df_all.customer_id == cid]
    #center_home = df_an[['home_lat', 'home_lon']].drop_duplicates().values
    #center_work = df_an[['work_lat', 'work_lon']].drop_duplicates().values
    #​
    #​
    #predicted_home = df_pred[df_pred.customer_id == cid][['is_home:add_lat', 'is_home:add_lon']].drop_duplicates().values
    #predicted_work = df_pred[df_pred.customer_id == cid][['is_work:add_lat', 'is_work:add_lon']].drop_duplicates().values
    #​
    #points_pos = df_an[df_an.is_pos == 1][['add_lat', 'add_lon']].dropna().values
    #points_atm = df_an[df_an.is_pos == 0][['add_lat', 'add_lon']].dropna().values
    #print(center_home.shape, center_work.shape, points_pos.shape, points_atm.shape)
    #​
    ## синие - покупки
    ## красные - банкоматы
    #gmap = gmaps.Map()
    #if len(points_pos) > 0:
    #    gmap.add_layer(gmaps.symbol_layer(points_pos, hover_text='pos',
    #                                      fill_color="blue", stroke_color="blue", scale=3))
    #if len(points_atm) > 0:
    #    gmap.add_layer(gmaps.symbol_layer(points_atm, hover_text='atm',
    #                                      fill_color="red", stroke_color="red", scale=3))
    #​
    #if not np.isnan(center_home)[0][0]:
    #    gmap.add_layer(gmaps.marker_layer(center_home, label='home'))
    #if not np.isnan(center_work)[0][0]:
    #    gmap.add_layer(gmaps.marker_layer(center_work, label='work'))
    #​
    #gmap.add_layer(gmaps.marker_layer(predicted_home, label='predicted_home'))
    #gmap.add_layer(gmaps.marker_layer(predicted_work, label='predicted_work'))
    #
    #gmap
    #
    #df_all.to_csv('../data/dfpredict1903.csv', index=None)
    #Predict
    #
    #del cust_test
    #
    #cust_test = df_all.loc[df_all['is_train'] == 0, 'customer_id'].unique()
    ## df_test = pd.DataFrame(cust_test, columns = ['customer_id']).merge(df_all, how = 'left')
    #df_test = predict_proba(pd.DataFrame(cust_test, columns=['customer_id']).merge(df_all, how='left'))
    #df_test.rename(columns={
    #    'customer_id': '_ID_',
    #    'is_home:add_lat': '_HOME_LAT_',
    #    'is_home:add_lon': '_HOME_LON_',
    #    'is_work:add_lat': '_WORK_LAT_',
    #    'is_work:add_lon': '_WORK_LON_'}, inplace=True)
    #df_test = df_test[['_ID_', '_WORK_LAT_', '_WORK_LON_', '_HOME_LAT_', '_HOME_LON_']]
    #​
    #df_test.head()
    #_ID_
    #_WORK_LAT_
    #_WORK_LON_
    #_HOME_LAT_
    #_HOME_LON_
    #0
    #000216
    #83
    #ccb416637fe9a4cd35e4606e
    #55.027000
    #82.915001
    #55.041771
    #82.984329
    #1
    #0002
    #d0f8a642272b41c292c12ab6e602
    #44.028999
    #42.841000
    #44.026016
    #42.865658
    #2
    #0004
    #d182d9fede3ba2534b2d5e5ad27e
    #43.588001
    #39.724998
    #43.572430
    #39.736073
    #3
    #000
    #8
    #c2445518c9392cb356c5c3db3392
    #51.537788
    #46.018105
    #51.529018
    #46.029404
    #4
    #000
    #b373cc4969c0be8e0933c08da67e1
    #56.248314
    #43.464493
    #56.238373
    #43.457146
    #Формируем
    #submission - файл
    #
    ## Заполняем пропуски
    #df_ = pd.read_csv('../data/test_set.csv', dtype=dtypes, usecols=['customer_id'])
    #submission = pd.DataFrame(df_['customer_id'].unique(), columns=['_ID_'])
    #​
    #submission = submission.merge(df_test, how='left').fillna(0)
    ## Пишем файл submission
    #submission.to_csv('../submissions/base_14_635_331.csv', index=None)
    #
    return src

def update_last_partition(dst, from_dt, to_dt):
    prev_day = datetime.strptime(from_dt, '%Y-%m-%d') - timedelta(days=1)
    res = spark.table(dst["d_train"]).checkpoint()
    res = res.where(res.day == to_dt)
    res = res.withColumn("period_to_dt", f.lit(prev_day)).withColumn("day", f.lit(prev_day.strftime('%Y-%m-%d')))
    res.coalesce(8).write.format("orc").insertInto(dst["d_train"], overwrite=True)


def calc_08(src, dst, from_dt, to_dt):
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
    spark = SparkSession.builder.appName("calc_08_task").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    hivecontext = HiveContext(spark.sparkContext)
    hivecontext.setConf("hive.exec.dynamic.partition", "true")
    hivecontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setCheckpointDir("hdfs:///user/airflow/psg/calc_08_task")

    opts = {
        'from_dt': sys.argv[1],
        "to_dt": "9999-12-31"
    }

    update_last_partition(prod_dst(), opts["from_dt"], opts["to_dt"])
    calc_08(prod_src(), prod_dst(), opts["from_dt"], opts["to_dt"])

