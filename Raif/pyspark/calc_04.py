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
    #    ​
    #    import lightgbm as lgb
    #    ​
    #    ​
    #    from sklearn.neighbors import NearestNeighbors
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
    #        'pos_adress_lat': np.float32,
    #        'pos_adress_lon': np.float32,
    #        'pos_address_lat': np.float32,
    #        'pos_address_lon': np.float32,
    #        'atm_address_lat': np.float32,
    #        'atm_address_lon': np.float32,
    #        'home_add_lat': np.float32,
    #        'home_add_lon': np.float32,
    #        'work_add_lat': np.float32,
    #        'work_add_lon': np.float32,
    #    }
    #    ​
    #    # для экономии памяти будем загружать только часть атрибутов транзакций
    #    usecols_train = ['customer_id', 'transaction_date', 'amount', 'country', 'city', 'currency', 'mcc',
    #                     'pos_adress_lat', 'pos_adress_lon', 'atm_address_lat', 'atm_address_lon', 'home_add_lat',
    #                     'home_add_lon', 'work_add_lat', 'work_add_lon']
    #    usecols_test = ['customer_id', 'transaction_date', 'amount', 'country', 'city', 'currency', 'mcc',
    #                    'pos_address_lat', 'pos_address_lon', 'atm_address_lat', 'atm_address_lon']
    #    Читаем
    #    train_set, test_set, соединяем
    #    в
    #    один
    #    датасет
    #
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
    #        'pos_adress_lat': np.float32,
    #        'pos_adress_lon': np.float32,
    #        'pos_address_lat': np.float32,
    #        'pos_address_lon': np.float32,
    #        'atm_address_lat': np.float32,
    #        'atm_address_lon': np.float32,
    #        'home_add_lat': np.float32,
    #        'home_add_lon': np.float32,
    #        'work_add_lat': np.float32,
    #        'work_add_lon': np.float32,
    #    }
    #    ​
    #    rnm = {
    #        'atm_address_lat': 'atm_lat',
    #        'atm_address_lon': 'atm_lon',
    #        'pos_adress_lat': 'pos_lat',
    #        'pos_adress_lon': 'pos_lon',
    #        'pos_address_lat': 'pos_lat',
    #        'pos_address_lon': 'pos_lon',
    #        'home_add_lat': 'home_lat',
    #        'home_add_lon': 'home_lon',
    #        'work_add_lat': 'work_lat',
    #        'work_add_lon': 'work_lon',
    #    }
    #
    #    df_train = pd.read_csv('../data/train_set.csv', dtype=dtypes)
    #    df_test = pd.read_csv('../data/test_set.csv', dtype=dtypes)
    #    ​
    #    df_train.rename(columns=rnm, inplace=True)
    #    df_test.rename(columns=rnm, inplace=True)
    #
    #    # удалим чувак с множественными адресами
    #    print(df_train.shape)
    #    gb = df_train.groupby('customer_id')['work_lat'].agg('nunique')
    #    cid_incorrect = gb[gb == 2].index
    #    df_train = df_train[~df_train.customer_id.isin(cid_incorrect.values)]
    #    print(df_train.shape)
    #    gb = df_train.groupby('customer_id')['home_lat'].agg('nunique')
    #    cid_incorrect = gb[gb == 2].index
    #    df_train = df_train[~df_train.customer_id.isin(cid_incorrect.values)]
    #    print(df_train.shape)
    #    (1224734, 18)
    #    (1207958, 18)
    #    (1142653, 18)
    #
    #    # соединяем test/train в одном DataFrame
    #    df_train['is_train'] = np.int32(1)
    #    df_test['is_train'] = np.int32(0)
    #    df_all = pd.concat([df_train, df_test])
    #    ​
    #    del df_train, df_test
    #    Обрабатываем
    #    дату
    #    транзакции
    #    и
    #    категориальные
    #    признаки
    #
    #    df_all['currency'] = df_all['currency'].fillna(-1).astype(np.int32)
    #    df_all['mcc'] = df_all['mcc'].apply(lambda x: int(x.replace(',', ''))).astype(np.int32)
    #    df_all['city'] = df_all['city'].factorize()[0].astype(np.int32)
    #    df_all['country'] = df_all['country'].factorize()[0].astype(np.int32)
    #    ​
    #    # удаляем транзакции без даты
    #    df_all = df_all[~df_all['transaction_date'].isnull()]
    #    df_all['transaction_date'] = pd.to_datetime(df_all['transaction_date'], format='%Y-%m-%d')
    #    Фичи
    #    для
    #    даты
    #
    #    df_all['month'] = df_all.transaction_date.dt.month
    #    df_all['day'] = df_all.transaction_date.dt.day
    #    df_all['dayofyear'] = df_all.transaction_date.dt.dayofyear
    #    df_all['dayofweek'] = df_all.transaction_date.dt.dayofweek
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
    #    dfs = []
    #    for cid in tqdm(df_all.customer_id.unique()):
    #        df_an = df_all[df_all.customer_id == cid]
    #        df_an = mfuncs.add_dist_to_neighbours(df_an)
    #        dfs.append(df_an)
    #    100 % |██████████ | 19642 / 19642[2:17:07 < 00: 00, 6.49
    #    it / s]
    #
    #    df_knn = pd.concat(dfs)
    #    df_knn.head()
    #
    #    df_knn['pos2pos_1', 'pos2pos_2', 'atm2pos_1', 'atm2pos_2', 'pos2atm_1',
    #           'pos2atm_2', 'pos2atm_1', 'pos2atm_2']
    #    Index(['amount', 'atm_address', 'atm_lat', 'atm_lon', 'city', 'country',
    #           'currency', 'customer_id', 'home_lat', 'home_lon', 'is_train', 'mcc',
    #           'pos_address', 'pos_lat', 'pos_lon', 'terminal_id', 'transaction_date',
    #           'work_lat', 'work_lon', 'month', 'day', 'dayofyear', 'dayofweek',
    #           'pos2pos_1', 'pos2pos_2', 'atm2pos_1', 'atm2pos_2', 'pos2atm_1',
    #           'pos2atm_2', 'pos2atm_1', 'pos2atm_2'],
    #          dtype='object')
    #
    #    df_knn.to_csv('../data/df_knn.csv', index=None)
    #
    #    df_all = df_knn.copy()
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
    #    df_all.drop(['work_lat', 'work_lon', 'home_lat', 'home_lon'], axis=1, inplace=True)
    #    Генерируем
    #    категориальный
    #    признак
    #    для
    #    адреса
    #
    #    df_all['address'] = df_all['add_lat'].apply(lambda x: "%.02f" % x) + ';' + df_all['add_lon'].apply(
    #        lambda x: "%.02f" % x)
    #    df_all['address'] = df_all['address'].factorize()[0].astype(np.int32)
    #    Генерируем
    #    несколько
    #    абонентских
    #    фич
    #
    #    # количество транзакций каждого клиента
    #    df_all = df_all.merge(df_all.groupby('customer_id')['amount'].count().reset_index(name='cid_trans_count'),
    #                          how='left')
    #    df_all['cid_trans_count'] = df_all['cid_trans_count'].astype(np.int32)
    #    ​
    #    df_all = df_all.merge(
    #        df_all.groupby(['customer_id', 'address'])['amount'].count().reset_index(name='cid_add_trans_count'),
    #        how='left')
    #    df_all['cid_add_trans_count'] = df_all['cid_add_trans_count'].astype(np.int32)
    #    ​
    #    # какая часть транзакций клиента приходится на данный адрес
    #    # TODO: БОЛЬШЕ ТАКИХ ФИЧ
    #    df_all['ratio1'] = df_all['cid_add_trans_count'] / df_all['cid_trans_count']
    #    Мои
    #    фичи
    #
    #    df_gb[['amount', 'add_lat', 'add_lon']].agg(['mean', 'max', 'min'])
    #    ---------------------------------------------------------------------------
    #    ValueError
    #    Traceback(most
    #    recent
    #    call
    #    last)
    #    < ipython - input - 53 - 479
    #    f5089f31f > in < module > ()
    #    ----> 1
    #    df_gb[['amount', 'add_lat', 'add_lon']].agg(['mean'])
    #
    #    / usr / local / lib / python3
    #    .5 / dist - packages / pandas / core / groupby.py in aggregate(self, arg, *args, **kwargs)
    #    4034
    #    versionadded = ''))
    #    4035
    #
    #    def aggregate(self, arg, *args, **kwargs):
    #
    #-> 4036
    #return super(DataFrameGroupBy, self).aggregate(arg, *args, **kwargs)
    #4037
    #4038
    #agg = aggregate
    #
    #/ usr / local / lib / python3
    #.5 / dist - packages / pandas / core / groupby.py in aggregate(self, arg, *args, **kwargs)
    #3466
    #3467
    #_level = kwargs.pop('_level', None)
    #-> 3468
    #result, how = self._aggregate(arg, _level=_level, *args, **kwargs)
    #3469
    #if how is None:
    #    3470
    #    return result
    #
    #/ usr / local / lib / python3
    #.5 / dist - packages / pandas / core / base.py in _aggregate(self, arg, *args, **kwargs)
    #632
    #return self._aggregate_multiple_funcs(arg,
    #                                      633
    #_level = _level,
    #         --> 634
    #_axis = _axis), None
    #635 else:
    #636
    #result = None
    #
    #/ usr / local / lib / python3
    #.5 / dist - packages / pandas / core / base.py in _aggregate_multiple_funcs(self, arg, _level, _axis)
    #652
    #obj = self._selected_obj
    #653 else:
    #--> 654
    #obj = self._obj_with_exclusions
    #655
    #656
    #results = []
    #
    #pandas / _libs / src / properties.pyx in pandas._libs.lib.cache_readonly.__get__(pandas / _libs / lib.c: 44594)()
    #
    #/ usr / local / lib / python3
    #.5 / dist - packages / pandas / core / base.py in _obj_with_exclusions(self)
    #326 if self._selection is not None and isinstance(self.obj,
    #                                                  327
    #ABCDataFrame):
    #--> 328
    #return self.obj.reindex(columns=self._selection_list)
    #329
    #330
    #if len(self.exclusions) > 0:
    #
    #/ usr / local / lib / python3
    #.5 / dist - packages / pandas / core / frame.py in reindex(self, index, columns, **kwargs)
    #2731
    #
    #
    #def reindex(self, index=None, columns=None, **kwargs):
    #    2732
    #    return super(DataFrame, self).reindex(index=index, columns=columns,
    #    -> 2733 ** kwargs)
    #    2734
    #    2735 @ Appender(_shared_docs['reindex_axis'] % _shared_doc_kwargs)
    #
    #    / usr / local / lib / python3
    #    .5 / dist - packages / pandas / core / generic.py in reindex(self, *args, **kwargs)
    #    2513  # perform the reindex on the axes
    #    2514
    #    return self._reindex_axes(axes, level, limit, tolerance, method,
    #    -> 2515
    #    fill_value, copy).__finalize__(self)
    #    2516
    #    2517
    #
    #    def _reindex_axes(self, axes, level, limit, tolerance, method, fill_value,
    #
    #                      / usr / local / lib / python3.5 / dist-packages / pandas / core / frame.py in _reindex_axes(self,
    #                      axes, level, limit, tolerance, method, fill_value, copy)
    #        2672
    #
    #    if columns is not None:
    #        2673
    #    frame = frame._reindex_columns(columns, method, copy, level,
    #    -> 2674
    #    fill_value, limit, tolerance)
    #    2675
    #    2676
    #    index = axes['index']
    #
    #            / usr / local / lib / python3
    #    .5 / dist - packages / pandas / core / frame.py in _reindex_columns(self, new_columns, method, copy, level,
    #                                                                        fill_value, limit, tolerance)
    #    2697
    #    return self._reindex_with_indexers({1: [new_columns, indexer]},
    #                                       2698
    #    copy = copy, fill_value = fill_value,
    #    -> 2699
    #    allow_dups = False)
    #    2700
    #    2701
    #
    #    def _reindex_multi(self, axes, copy, fill_value):
    #
    #    / usr / local / lib / python3
    #    .5 / dist - packages / pandas / core / generic.py in _reindex_with_indexers(self, reindexers, fill_value, copy,
    #                                                                                allow_dups)
    #    2625
    #    fill_value = fill_value,
    #    2626
    #    allow_dups = allow_dups,
    #    -> 2627
    #    copy = copy)
    #    2628
    #    2629
    #    if copy and new_data is self._data:
    #        /
    #        usr / local / lib / python3
    #    .5 / dist - packages / pandas / core / internals.py in reindex_indexer(self, new_axis, indexer, axis, fill_value,
    #                                                                           allow_dups, copy)
    #    3884  # some axes don't allow reindexing with dups
    #    3885
    #    if not allow_dups:
    #        ->
    #    3886
    #    self.axes[axis]._can_reindex(indexer)
    #    3887
    #    3888
    #    if axis >= self.ndim:
    #        /
    #        usr / local / lib / python3
    #    .5 / dist - packages / pandas / core / indexes / base.py in _can_reindex(self, indexer)
    #    2834  # trying to reindex on an axis with duplicates
    #    2835
    #    if not self.is_unique and len(indexer):
    #        ->
    #    2836
    #    raise ValueError("cannot reindex from a duplicate axis")
    #    2837
    #    2838
    #
    #    def reindex(self, target, method=None, level=None, limit=None,
    #
    #                ValueError: cannot reindex
    #
    #    from a duplicate
    #    axis
    #
    #    df_all[['customer_id', 'amount', 'add_lat', 'add_lon']]
    #    customer_id
    #    amount
    #    add_lat
    #    add_lon
    #    0
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.884034
    #    59.844074
    #    30.179153
    #    1
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.775633
    #    59.844074
    #    30.179153
    #    2
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.708368
    #    59.858200
    #    30.229023
    #    3
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.787498
    #    59.844074
    #    30.179153
    #    4
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.892510
    #    59.844074
    #    30.179153
    #    5
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.909018
    #    59.844074
    #    30.179153
    #    6
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.801228
    #    59.844074
    #    30.179153
    #    7
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.838200
    #    59.844074
    #    30.179153
    #    8
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.264740
    #    59.844074
    #    30.179153
    #    9
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.118792
    #    59.844074
    #    30.179153
    #    10
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.109393
    #    59.844074
    #    30.179153
    #    11
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.694397
    #    59.860001
    #    30.250999
    #    12
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    4.133102
    #    59.860001
    #    30.247000
    #    13
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.057675
    #    59.859001
    #    30.245001
    #    14
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.620160
    #    59.860001
    #    30.245001
    #    15
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.867951
    #    59.860001
    #    30.250000
    #    16
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    4.416207
    #    59.860001
    #    30.247999
    #    17
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.948432
    #    59.855999
    #    30.250000
    #    18
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.938953
    #    59.855000
    #    30.249001
    #    19
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.662942
    #    59.861000
    #    30.250999
    #    20
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    4.621914
    #    59.860001
    #    30.245001
    #    21
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    2.988430
    #    59.858002
    #    30.246000
    #    22
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    4.260764
    #    59.848999
    #    30.268999
    #    23
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    4.081697
    #    59.849998
    #    30.225000
    #    24
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.914050
    #    59.862000
    #    30.232000
    #    25
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.382170
    #    59.862000
    #    30.229000
    #    26
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.408833
    #    59.862000
    #    30.228001
    #    27
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.312841
    #    59.858002
    #    30.233000
    #    28
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    3.832169
    #    59.858002
    #    30.230000
    #    29
    #    0
    #    dc0137d280a2a82d2dc89282450ff1b
    #    4.318061
    #    59.856998
    #    30.226999
    #        ...............
    #    2215352
    #    7198
    #    d32d7c354fc93587bc49641c6eef
    #    4.009341
    #    57.113998
    #    65.568001
    #    2215353
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    4.189260
    #    55.751999
    #    37.606998
    #    2215354
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    3.541705
    #    55.757999
    #    37.610001
    #    2215355
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    4.565164
    #    55.751999
    #    37.605000
    #    2215356
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    3.688071
    #    55.757999
    #    37.611000
    #    2215357
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    3.309568
    #    55.752998
    #    37.611000
    #    2215358
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    4.483331
    #    55.757999
    #    37.606998
    #    2215359
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    4.463511
    #    55.752998
    #    37.608002
    #    2215360
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    3.489367
    #    55.755001
    #    37.612999
    #    2215361
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    4.059295
    #    55.751999
    #    37.608002
    #    2215362
    #    6
    #    dcb0b69a981bcda27289aa05a801519
    #    4.285100
    #    55.751999
    #    37.605999
    #    2215363
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.155188
    #    54.763000
    #    83.102997
    #    2215364
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    3.989709
    #    54.769001
    #    83.098999
    #    2215365
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    3.988788
    #    54.762001
    #    83.099998
    #    2215366
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    3.692765
    #    54.762001
    #    83.103996
    #    2215367
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    3.994782
    #    54.763000
    #    83.103996
    #    2215368
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.307022
    #    54.765999
    #    83.103996
    #    2215369
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.008067
    #    54.763000
    #    83.103996
    #    2215370
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.017159
    #    54.764999
    #    83.105003
    #    2215371
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    3.996451
    #    54.764000
    #    83.100998
    #    2215372
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.463464
    #    54.755001
    #    83.119003
    #    2215373
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.012532
    #    54.755001
    #    83.116997
    #    2215374
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.491719
    #    54.758999
    #    83.120003
    #    2215375
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.295208
    #    54.756001
    #    83.112999
    #    2215376
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.480490
    #    54.755001
    #    83.119003
    #    2215377
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    3.306456
    #    54.759998
    #    83.114998
    #    2215378
    #    6e30
    #    f78725b6618176206d875f6a1aae
    #    4.487947
    #    54.757000
    #    83.119003
    #    2215379
    #    6
    #    f238e23623353aa774eacfae00b55af
    #    2.484099
    #    55.743999
    #    37.540001
    #    2215380
    #    6
    #    f238e23623353aa774eacfae00b55af
    #    2.980216
    #    55.743999
    #    37.539001
    #    2215381
    #    6
    #    f238e23623353aa774eacfae00b55af
    #    4.588100
    #    55.745998
    #    37.535999
    #    2215382
    #    rows × 4
    #    columns
    #
    #    df_gb['amount', 'add_lat', 'add_lon'].agg(['mean', 'max', 'min'])
    #
    #    df_all.reset_index(inplace=True, drop=True)
    #
    #    df_all[['customer_id', 'amount', 'add_lat', 'add_lon']].groupby('customer_id').agg('max')
    #    amount
    #    add_lat
    #    add_lon
    #    customer_id
    #    0001
    #    f322716470bf9bfc1708f06f00fc
    #    4.614833
    #    56.251347
    #    43.446255
    #    000216
    #    83
    #    ccb416637fe9a4cd35e4606e
    #    5.178286
    #    55.449772
    #    84.168541
    #    0002
    #    d0f8a642272b41c292c12ab6e602
    #    3.617783
    #    59.829090
    #    50.173374
    #    0004
    #    d182d9fede3ba2534b2d5e5ad27e
    #    4.209749
    #    43.597000
    #    39.975124
    #    00072
    #    97
    #    d86e14bd68bd87b1dbdefe302
    #    4.696914
    #    59.949146
    #    38.980770
    #    000
    #    8
    #    c2445518c9392cb356c5c3db3392
    #    4.471992
    #    53.198917
    #    46.748531
    #    000
    #    b373cc4969c0be8e0933c08da67e1
    #    3.492366
    #    59.934464
    #    49.919624
    #    000
    #    b709c6c6fb1e8efcfd95e57c2a9de
    #    4.319009
    #    56.512737
    #    86.172333
    #    000
    #    c589e94c95984721de4b2bfb9ee4e
    #    3.154039
    #    55.725166
    #    37.425613
    #    001611e3
    #    ac051a0ec91c88bbd9dbeb5a
    #    4.057639
    #    57.011002
    #    41.026821
    #    0016
    #    91
    #    ae3885e80add35148a01e75206
    #    5.016934
    #    60.017960
    #    37.411518
    #    00204415
    #    9304738
    #    ea7e3598131809851
    #    4.232937
    #    53.574001
    #    50.217651
    #    002631
    #    9
    #    faa345a573522f0a04f5c55bb
    #    3.710053
    #    54.653000
    #    39.831928
    #    0027
    #    a7618d97cc9fbda55fac457eaeb7
    #    3.375377
    #    59.008991
    #    51.009949
    #    002
    #    9
    #    d9be3692701efe66741fa74a8f8b
    #    5.019326
    #    55.622921
    #    37.605988
    #    002
    #    b9f6e118c54f1292e03d1a04d516e
    #    4.478792
    #    59.623196
    #    56.721725
    #    002
    #    c40ec938e91de248400dec824bd49
    #    5.163564
    #    59.949371
    #    43.259167
    #    00317
    #    c648bc11161417b342ad480e724
    #    3.835234
    #    59.860893
    #    30.580381
    #    0031
    #    915
    #    eb230f772681fb5dc5a8d1c31
    #    5.007967
    #    60.011047
    #    38.189163
    #    003360
    #    bff9882ca4a4f93394dd984822
    #    4.277591
    #    59.213764
    #    38.015736
    #    0037
    #    f3de3d890df1022cc760a1dfd9d6
    #    4.543791
    #    59.842239
    #    37.974369
    #    003
    #    8
    #    ea686d27899b0942409157d04ff2
    #    2.857959
    #    61.269089
    #    73.447105
    #    003
    #    bc1334379d480c7e5f28240dc40d9
    #    3.717804
    #    55.995220
    #    60.108738
    #    003
    #    fa58414cc55531fcc38423bea8f8e
    #    4.475021
    #    57.145241
    #    131.108932
    #    00450
    #    ac1c22c9ee6dda590ff5366236c
    #    3.986032
    #    53.607090
    #    55.935070
    #    0046
    #    c2952fb808aa11f74abce5abe097
    #    4.492328
    #    55.025314
    #    22.012411
    #    0050
    #    9465377
    #    a24375b276c5da9a67fa5
    #    3.781627
    #    60.059582
    #    30.678854
    #    0051
    #    94
    #    bf7238734eb49c142258c5a263
    #    4.481363
    #    55.788353
    #    136.648224
    #    0055
    #    a1c72c44451165c872cd992c1d90
    #    3.299616
    #    55.894718
    #    38.442005
    #    005
    #    b206d0ffec59e249e6f7adc1b2e83
    #    4.782627
    #    59.929787
    #    42.001141
    #        ............
    #        ff463f782f65fbde0e4f7ea4e327d99e
    #    4.160493
    #    58.557789
    #    82.653000
    #    ff4bcb4ea454c4a38b97dc20cda7932c
    #    4.319700
    #    60.090919
    #    73.352402
    #    ff4e78a42acf6bbb27d1678dc0f0e5a1
    #    4.806856
    #    61.790131
    #    124.923218
    #    ff51d9888921dc6e5f74862dc0e0f250
    #    4.398249
    #    55.104000
    #    33.257999
    #    ff578c093f6baff43818f61a52a6d03d
    #    3.462374
    #    56.004887
    #    92.939461
    #    ff652e6110b6e6ce92b46c6c9a3fa28b
    #    3.937262
    #    52.442001
    #    78.918999
    #    ff6db3bf1eb82426c894b05ae64d68a1
    #    3.307378
    #    55.606636
    #    37.795010
    #    ff6e96c6e3e5f6bfea831834c49eca2a
    #    5.141136
    #    64.955009
    #    43.478653
    #    ff6f12be73bfa5b3740bec1a420eca6b
    #    4.828726
    #    55.964073
    #    37.646362
    #    ff70bf60dcdf63f99bf1af5af5d6c145
    #    4.314423
    #    60.199024
    #    30.505405
    #    ff70c3c5f2dca00f255705305d75111d
    #    4.464280
    #    59.998505
    #    30.731922
    #    ff71bdbcba59047f1fad88dcb7052151
    #    4.462170
    #    55.751099
    #    37.757999
    #    ff78fca768ff08120c0c68bd26c719de
    #    4.676995
    #    56.231544
    #    39.579533
    #    ff7a887d347a8d598dc8e559d3aaec2f
    #    3.311437
    #    56.521294
    #    44.079964
    #    ff7e1c6c07469b28a07847540385e767
    #    4.119733
    #    59.938416
    #    44.989582
    #    ff869ee855dc3f9b382c943eb43cc4ec
    #    4.581357
    #    59.918087
    #    87.601219
    #    ff92d5420f5fb92a37e1280d1fc9e5f4
    #    4.687912
    #    57.952000
    #    56.119999
    #    ff9becfaf9e022b46fe69d10c8060776
    #    4.705500
    #    55.988026
    #    85.233429
    #    ffa959b073a8bde17f3b8b4b25409b69
    #    5.173771
    #    55.911999
    #    142.347260
    #    ffad3c72297eb6d9a4b3672cd731396c
    #    5.172408
    #    59.923897
    #    132.363922
    #    ffaeae55d4dbf29058f04e7a6a764f02
    #    3.534924
    #    55.725540
    #    37.858467
    #    ffb35ffc8a90ba9dfff70be24513010a
    #    4.346421
    #    55.772861
    #    78.436401
    #    ffb8fcf3f9d17ac3197b9e27cb757539
    #    4.695918
    #    60.061367
    #    39.586372
    #    ffba001147dc6140d84070b7bc9479df
    #    3.585759
    #    51.610809
    #    46.038181
    #    ffc5289194413ec68c3f7adc8121d69b
    #    4.152719
    #    55.911011
    #    37.701527
    #    ffd097949a4a238296a7deadfb376cc0
    #    3.445825
    #    55.989838
    #    37.872677
    #    ffd6622f135e264da543a541756e63a9
    #    5.013739
    #    56.351116
    #    41.312519
    #    ffdd5ec2a90e355cf40525eac1a6fd34
    #    4.404512
    #    57.481705
    #    42.147800
    #    ffe6875cf2566b5f273fc49f3c064031
    #    3.898256
    #    69.012062
    #    60.614304
    #    ffebf4ea02c72183128d966721976ec9
    #    5.096354
    #    56.328468
    #    44.083584
    #    19642
    #    rows × 3
    #    columns
    #
    #    # добавим признаки после групбая
    #    df_gb = df_all[['customer_id', 'amount', 'add_lat', 'add_lon']].groupby('customer_id')
    #    coord_stat_df = df_gb.agg(['mean', 'max', 'min'])
    #    coord_stat_df['transactions_per_user'] = df_gb.agg('size')
    #    coord_stat_df.columns = ['_'.join(col).strip() for col in coord_stat_df.columns.values]
    #    coord_stat_df.reset_index(inplace=True)
    #    df_all = pd.merge(df_all, coord_stat_df, on='customer_id', how='left')
    #
    #    cols = ['add_lat', 'add_lon']
    #    types = ['min', 'max', 'mean']
    #    for c in cols:
    #        for
    #    t in types:
    #    df_all['{}_ratio_{}'.format(c, t)] = np.abs(df_all[c] / df_all['{}_{}'.format(c, t)])
    #
    #    df_all = pd.concat([df_all, pd.get_dummies(df_all['mcc'], prefix='mcc')], axis=1)
    #    del df_all['mcc']
    #    LightGBM
    #
    #    df_all = df_all.loc[:, ~df_all.columns.duplicated()]
    #
    #    from sklearn.model_selection import train_test_split
    #    ​
    #    ys = ['is_home', 'is_work']
    #    drop_cols = ['atm_address', 'customer_id', 'pos_address', 'terminal_id', 'transaction_date',
    #                 'is_home', 'has_home', 'is_work', 'has_work', 'is_train']
    #    ​
    #    drop_cols += ['pred:is_home', 'pred:is_work']
    #    y_cols = ['is_home', 'is_work']
    #    usecols = df_all.drop(drop_cols, 1, errors='ignore').columns
    #
    #    params = {
    #        'objective': 'binary',
    #        'num_leaves': 63,
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
    #                      early_stopping_rounds=300)
    #    ​
    #    model[y_col] = gbm_h
    #    Training
    #    until
    #    validation
    #    scores
    #    don
    #    't improve for 300 rounds.
    #    [30]
    #    valid_0
    #    's binary_logloss: 0.60709
    #    [60]
    #    valid_0
    #    's binary_logloss: 0.55481
    #    [90]
    #    valid_0
    #    's binary_logloss: 0.52188
    #    [120]
    #    valid_0
    #    's binary_logloss: 0.500737
    #    [150]
    #    valid_0
    #    's binary_logloss: 0.486256
    #    [180]
    #    valid_0
    #    's binary_logloss: 0.476052
    #    [210]
    #    valid_0
    #    's binary_logloss: 0.46878
    #    [240]
    #    valid_0
    #    's binary_logloss: 0.463383
    #    [270]
    #    valid_0
    #    's binary_logloss: 0.458828
    #    [300]
    #    valid_0
    #    's binary_logloss: 0.455549
    #    [330]
    #    valid_0
    #    's binary_logloss: 0.452839
    #    [360]
    #    valid_0
    #    's binary_logloss: 0.450904
    #    [390]
    #    valid_0
    #    's binary_logloss: 0.448986
    #    [420]
    #    valid_0
    #    's binary_logloss: 0.447638
    #    [450]
    #    valid_0
    #    's binary_logloss: 0.446429
    #    [480]
    #    valid_0
    #    's binary_logloss: 0.445285
    #    [510]
    #    valid_0
    #    's binary_logloss: 0.444496
    #    [540]
    #    valid_0
    #    's binary_logloss: 0.443656
    #    [570]
    #    valid_0
    #    's binary_logloss: 0.443121
    #    [600]
    #    valid_0
    #    's binary_logloss: 0.44271
    #    [630]
    #    valid_0
    #    's binary_logloss: 0.442535
    #    [660]
    #    valid_0
    #    's binary_logloss: 0.442225
    #    [690]
    #    valid_0
    #    's binary_logloss: 0.441854
    #    [720]
    #    valid_0
    #    's binary_logloss: 0.44158
    #    [750]
    #    valid_0
    #    's binary_logloss: 0.441439
    #    [780]
    #    valid_0
    #    's binary_logloss: 0.441242
    #    [810]
    #    valid_0
    #    's binary_logloss: 0.441204
    #    [840]
    #    valid_0
    #    's binary_logloss: 0.441231
    #    [870]
    #    valid_0
    #    's binary_logloss: 0.441259
    #    [900]
    #    valid_0
    #    's binary_logloss: 0.441044
    #    [930]
    #    valid_0
    #    's binary_logloss: 0.440819
    #    [960]
    #    valid_0
    #    's binary_logloss: 0.440762
    #    [990]
    #    valid_0
    #    's binary_logloss: 0.440506
    #    [1020]
    #    valid_0
    #    's binary_logloss: 0.440368
    #    [1050]
    #    valid_0
    #    's binary_logloss: 0.440271
    #    [1080]
    #    valid_0
    #    's binary_logloss: 0.440108
    #    [1110]
    #    valid_0
    #    's binary_logloss: 0.440068
    #    [1140]
    #    valid_0
    #    's binary_logloss: 0.440076
    #    [1170]
    #    valid_0
    #    's binary_logloss: 0.440098
    #    [1200]
    #    valid_0
    #    's binary_logloss: 0.440219
    #    [1230]
    #    valid_0
    #    's binary_logloss: 0.440263
    #    [1260]
    #    valid_0
    #    's binary_logloss: 0.440486
    #    [1290]
    #    valid_0
    #    's binary_logloss: 0.440679
    #    [1320]
    #    valid_0
    #    's binary_logloss: 0.440664
    #    [1350]
    #    valid_0
    #    's binary_logloss: 0.440489
    #    [1380]
    #    valid_0
    #    's binary_logloss: 0.44051
    #    [1410]
    #    valid_0
    #    's binary_logloss: 0.440349
    #    Early
    #    stopping, best
    #    iteration is:
    #    [1124]
    #    valid_0
    #    's binary_logloss: 0.439947
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
    #                      early_stopping_rounds=300)
    #    ​
    #    model[y_col] = gbm_w
    #    Training
    #    until
    #    validation
    #    scores
    #    don
    #    't improve for 300 rounds.
    #    [30]
    #    valid_0
    #    's binary_logloss: 0.577203
    #    [60]
    #    valid_0
    #    's binary_logloss: 0.505615
    #    [90]
    #    valid_0
    #    's binary_logloss: 0.46027
    #    [120]
    #    valid_0
    #    's binary_logloss: 0.429884
    #    [150]
    #    valid_0
    #    's binary_logloss: 0.409737
    #    [180]
    #    valid_0
    #    's binary_logloss: 0.395657
    #    [210]
    #    valid_0
    #    's binary_logloss: 0.385574
    #    [240]
    #    valid_0
    #    's binary_logloss: 0.378012
    #    [270]
    #    valid_0
    #    's binary_logloss: 0.372507
    #    [300]
    #    valid_0
    #    's binary_logloss: 0.367728
    #    [330]
    #    valid_0
    #    's binary_logloss: 0.364101
    #    [360]
    #    valid_0
    #    's binary_logloss: 0.36112
    #    [390]
    #    valid_0
    #    's binary_logloss: 0.359536
    #    [420]
    #    valid_0
    #    's binary_logloss: 0.357894
    #    [450]
    #    valid_0
    #    's binary_logloss: 0.35652
    #    [480]
    #    valid_0
    #    's binary_logloss: 0.3556
    #    [510]
    #    valid_0
    #    's binary_logloss: 0.354502
    #    [540]
    #    valid_0
    #    's binary_logloss: 0.353899
    #    [570]
    #    valid_0
    #    's binary_logloss: 0.353522
    #    [600]
    #    valid_0
    #    's binary_logloss: 0.353235
    #    [630]
    #    valid_0
    #    's binary_logloss: 0.353279
    #    [660]
    #    valid_0
    #    's binary_logloss: 0.353185
    #    [690]
    #    valid_0
    #    's binary_logloss: 0.353026
    #    [720]
    #    valid_0
    #    's binary_logloss: 0.353079
    #    [750]
    #    valid_0
    #    's binary_logloss: 0.353015
    #    [780]
    #    valid_0
    #    's binary_logloss: 0.35353
    #    [810]
    #    valid_0
    #    's binary_logloss: 0.353381
    #    [840]
    #    valid_0
    #    's binary_logloss: 0.353388
    #    [870]
    #    valid_0
    #    's binary_logloss: 0.353113
    #    [900]
    #    valid_0
    #    's binary_logloss: 0.353014
    #    [930]
    #    valid_0
    #    's binary_logloss: 0.35277
    #    [960]
    #    valid_0
    #    's binary_logloss: 0.353147
    #    [990]
    #    valid_0
    #    's binary_logloss: 0.3532
    #    [1020]
    #    valid_0
    #    's binary_logloss: 0.353182
    #    [1050]
    #    valid_0
    #    's binary_logloss: 0.353325
    #    [1080]
    #    valid_0
    #    's binary_logloss: 0.353466
    #    [1110]
    #    valid_0
    #    's binary_logloss: 0.353251
    #    [1140]
    #    valid_0
    #    's binary_logloss: 0.353296
    #    [1170]
    #    valid_0
    #    's binary_logloss: 0.353393
    #    [1200]
    #    valid_0
    #    's binary_logloss: 0.353586
    #    [1230]
    #    valid_0
    #    's binary_logloss: 0.353567
    #    Early
    #    stopping, best
    #    iteration is:
    #    [930]
    #    valid_0
    #    's binary_logloss: 0.35277
    #
    #    lgb.plot_importance(gbm_w, max_num_features=15)
    #    < matplotlib.axes._subplots.AxesSubplot
    #    at
    #    0x7f1889ff2400 >
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
    #
    #    def predict_proba(dt, ys=['is_home', 'is_work']):
    #        for col in ys:
    #            pred = ('pred:%s' % col)
    #            dt[pred] = model[col].predict(dt[usecols])
    #        return dt.groupby('customer_id').apply(_best).reset_index()
    #
    #    ​
    #
    #    def score(dt, ys=['is_home', 'is_work']):
    #        dt_ret = predict_proba(dt, ys)
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
    #    accuracy: 0.5152524993591386
    #    Test
    #    accuracy: 0.5204918032786885
    #    Predict
    #
    #    cust_test = df_all[df_all['is_train'] == 0]['customer_id'].unique()
    #    df_test = pd.DataFrame(cust_test, columns=['customer_id']).merge(df_all, how='left')
    #    df_test = predict_proba(df_test)
    #    df_test.rename(columns={
    #        'customer_id': '_ID_',
    #        'is_home:add_lat': '_HOME_LAT_',
    #        'is_home:add_lon': '_HOME_LON_',
    #        'is_work:add_lat': '_WORK_LAT_',
    #        'is_work:add_lon': '_WORK_LON_'}, inplace=True)
    #    df_test = df_test[['_ID_', '_WORK_LAT_', '_WORK_LON_', '_HOME_LAT_', '_HOME_LON_']]
    #    ​
    #    df_test.head()
    #    Формируем
    #    submission - файл
    #
    #    # Заполняем пропуски
    #    df_ = pd.read_csv('../data/test_set.csv', dtype=dtypes, usecols=['customer_id'])
    #    submission = pd.DataFrame(df_['customer_id'].unique(), columns=['_ID_'])
    #    ​
    #    submission = submission.merge(df_test, how='left').fillna(0)
    #    # Пишем файл submission
    #    submission.to_csv('../submissions/base_2_47_32.csv', index=None)
    return src

def update_last_partition(dst, from_dt, to_dt):
    prev_day = datetime.strptime(from_dt, '%Y-%m-%d') - timedelta(days=1)
    res = spark.table(dst["d_train"]).checkpoint()
    res = res.where(res.day == to_dt)
    res = res.withColumn("period_to_dt", f.lit(prev_day)).withColumn("day", f.lit(prev_day.strftime('%Y-%m-%d')))
    res.coalesce(8).write.format("orc").insertInto(dst["d_train"], overwrite=True)


def calc_04(src, dst, from_dt, to_dt):
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
    spark = SparkSession.builder.appName("calc_04_task").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    hivecontext = HiveContext(spark.sparkContext)
    hivecontext.setConf("hive.exec.dynamic.partition", "true")
    hivecontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setCheckpointDir("hdfs:///user/airflow/psg/calc_04_task")

    opts = {
        'from_dt': sys.argv[1],
        "to_dt": "9999-12-31"
    }

    update_last_partition(prod_dst(), opts["from_dt"], opts["to_dt"])
    calc_04(prod_src(), prod_dst(), opts["from_dt"], opts["to_dt"])

