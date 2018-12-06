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
    #    import gmaps
    #    API_KEY = 'AIzaSyCG_RL0_kavuEaJAqEN5xXbU4h0VJUbA9M'
    #    gmaps.configure(api_key=API_KEY)  # Your Google API key
    #    % pylab
    #    inline
    #    The
    #    autoreload
    #    extension is already
    #    loaded.To
    #    reload
    #    it, use:
    #    % reload_ext
    #    autoreload
    #
    #
    #Populating
    #the
    #interactive
    #namespace
    #from numpy and matplotlib
    #/ usr / local / lib / python3
    #.5 / dist - packages / IPython / core / magics / pylab.py: 160: UserWarning: pylab
    #import has
    #
    #clobbered
    #these
    #variables: ['f']
    #` % matplotlib
    #` prevents
    #importing *
    #from pylab and numpy
    #"\n`%matplotlib` prevents importing * from pylab and numpy"
    #
    #import pickle
    #
    #
    #def get_city(x):
    #    if 'Москва' in x:
    #        return 'Москва'
    #    if 'Санкт-Петербург' in x:
    #        return 'Санкт-Петербург'
    #
    #    x = x.split(',')
    #    if len(x) < 6:
    #        return 'none'
    #    else:
    #        return x[-6]
    #
    #
    #with open('../data/internal/test_coords.pcl', 'rb') as f:
    #    coords1 = pickle.load(f, encoding='latin1')
    #with open('../data/internal/train_coords.pcl', 'rb') as f:
    #    coords2 = pickle.load(f, encoding='latin1')
    #​
    #coords = pd.concat([coords1, coords2])
    #coords['city_name'] = coords['string'].progress_apply(get_city)
    #​
    #coords['add_lat'] = (coords['action_lat'].round(4).fillna(0) * 1000).astype(int)
    #coords['add_lon'] = (coords['action_lon'].round(4).fillna(0) * 1000).astype(int)
    #​
    #(df_all['atm_lat'].round(4).fillna(0) * 1000).astype(int) + (df_all['pos_lat'].round(4).fillna(0) * 1000).astype(int)
    #​
    #​
    #coords.drop_duplicates(inplace=True)
    #coords[coords.string.str.contains('азан')].head(20)
    ## del coords['string']
    #100 % |██████████ | 243861 / 243861[00:00 < 00:00, 710780.37
    #it / s]
    #action_lat
    #action_lon
    #string
    #city_name
    #add_lat
    #add_lon
    #27
    #55.791915
    #49.109698
    #DoubleTree
    #by
    #Hilton
    #Hotel
    #Kazan
    #City
    #Center, 21, улица
    #Чернышевского, Старо - Татарская
    #слобода, Вахитовский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420111, РФ
    #Казань
    #55791
    #49109
    #680
    #55.852706
    #49.189704
    #44, 4 - я
    #Станционная
    #улица, Малые
    #Дербышки, Советский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420133, РФ
    #Казань
    #55852
    #49189
    #855
    #56.291382
    #44.075155
    #улица
    #Касьянова, Казанское
    #шоссе, Верхние
    #Печёры, Нижегородский
    #район, Нижний
    #Новгород, городской
    #округ
    #Нижний
    #Новгород, Нижегородская
    #область, Приволжский
    #федеральный
    #округ, 603163, РФ
    #Нижний
    #Новгород
    #56291
    #44075
    #889
    #55.781112
    #49.116999
    #Татарская
    #усадьба, 8, улица
    #Шигабутдина
    #Марджани, Старо - Татарская
    #слобода, Вахитовский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420021, РФ
    #Казань
    #55781
    #49117
    #914
    #55.794052
    #49.114874
    #15 / 25, Кремлёвская
    #улица, Старо - Татарская
    #слобода, Вахитовский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420066, РФ
    #Казань
    #55794
    #49114
    #1032
    #55.916448
    #49.148837
    #Осиновая
    #улица, Щербаково, Авиастроительный
    #район, Дачное
    #сельское
    #поселение, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420099, РФ
    #Дачное
    #сельское
    #поселение
    #55916
    #49148
    #1200
    #55.722931
    #37.803331
    #1
    #Б, улица
    #Красный
    #Казанец, Вешняки, район
    #Вешняки, Восточный
    #административный
    #округ, Москва, ЦФО, 111395, РФ
    #Москва
    #55722
    #37803
    #1222
    #59.930872
    #30.318425
    #17 - 19, Казанская
    #улица, Апраксин
    #двор, округ № 78, Центральный
    #район, Санкт - Петербург, Северо - Западный
    #федеральный
    #округ, 191014, РФ
    #Санкт - Петербург
    #59930
    #30318
    #1343
    #55.818358
    #49.132824
    #Шамиль, 3, проспект
    #Фатыха
    #Амирхана, Дружба, Ново - Савиновский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420133, РФ
    #Казань
    #55818
    #49132
    #1501
    #59.930528
    #30.317599
    #26 / 27, Казанская
    #улица, Сенной
    #округ, Адмиралтейский
    #район, Санкт - Петербург, Северо - Западный
    #федеральный
    #округ, 190000, РФ
    #Санкт - Петербург
    #59930
    #30317
    #2021
    #55.851614
    #49.234232
    #2 - я
    #Клеверная
    #улица, Нагорный, Советский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420075, РФ
    #Казань
    #55851
    #49234
    #2329
    #56.298490
    #44.079637
    #Подновье, Казанское
    #шоссе, Верхние
    #Печёры, Нижегородский
    #район, Нижний
    #Новгород, городской
    #округ
    #Нижний
    #Новгород, Нижегородская
    #область, Приволжский
    #федеральный
    #округ, 603163, РФ
    #Нижний
    #Новгород
    #56298
    #44079
    #2411
    #55.788924
    #49.117883
    #70
    #А, улица
    #Баумана, Старо - Татарская
    #слобода, Вахитовский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420066, РФ
    #Казань
    #55788
    #49117
    #2585
    #59.932740
    #30.322259
    #8 - 10, Казанская
    #улица, Апраксин
    #двор, округ № 78, Центральный
    #район, Санкт - Петербург, Северо - Западный
    #федеральный
    #округ, 191014, РФ
    #Санкт - Петербург
    #59932
    #30322
    #3076
    #55.787008
    #49.161250
    #ул.Достоевского, улица
    #Абжалилова, Калуга, Вахитовский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420133, РФ
    #Казань
    #55787
    #49161
    #3534
    #55.780012
    #49.211031
    #Crazy
    #Park, 141, проспект
    #Победы, Азино - 1, Советский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420100, РФ
    #Казань
    #55780
    #49211
    #3541
    #55.780478
    #49.213015
    #Мега, 141, проспект
    #Победы, Азино - 1, Советский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420100, РФ
    #Казань
    #55780
    #49213
    #3544
    #55.750827
    #49.209874
    #Проспект
    #Победы, проспект
    #Победы, Горки - 3, Приволжский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420110, РФ
    #Казань
    #55750
    #49209
    #3546
    #55.791920
    #49.099091
    #Макдоналдс, 4, улица
    #Саид - Галеева, Ново - Татарская
    #слобода, Вахитовский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420111, РФ
    #Казань
    #55791
    #49099
    #3550
    #55.799080
    #49.119108
    #9, улица
    #Бехтерева, Старо - Татарская
    #слобода, Вахитовский
    #район, Казань, городской
    #округ
    #Казань, Татарстан, Приволжский
    #федеральный
    #округ, 420066, РФ
    #Казань
    #55799
    #49119
    #
    ## Определим типы колонок для экономии памяти
    #dtypes = {
    #    'transaction_date': str,
    #    'atm_address': str,
    #    'country': str,
    #    'city': str,
    #    'amount': np.float32,
    #    'currency': np.float32,
    #    'mcc': str,
    #    'customer_id': str,
    #    'pos_address': str,
    #    'atm_address': str,
    #    'pos_adress_lat': np.float32,
    #    'pos_adress_lon': np.float32,
    #    'pos_address_lat': np.float32,
    #    'pos_address_lon': np.float32,
    #    'atm_address_lat': np.float32,
    #    'atm_address_lon': np.float32,
    #    'home_add_lat': np.float32,
    #    'home_add_lon': np.float32,
    #    'work_add_lat': np.float32,
    #    'work_add_lon': np.float32,
    #}
    #​
    ## для экономии памяти будем загружать только часть атрибутов транзакций
    #usecols_train = ['customer_id', 'transaction_date', 'amount', 'country', 'city', 'currency', 'mcc', 'pos_adress_lat',
    #                 'pos_adress_lon', 'atm_address_lat', 'atm_address_lon', 'home_add_lat', 'home_add_lon', 'work_add_lat',
    #                 'work_add_lon']
    #usecols_test = ['customer_id', 'transaction_date', 'amount', 'country', 'city', 'currency', 'mcc', 'pos_address_lat',
    #                'pos_address_lon', 'atm_address_lat', 'atm_address_lon']
    #Читаем
    #train_set, test_set, соединяем
    #в
    #один
    #датасет
    #
    #dtypes = {
    #    'transaction_date': str,
    #    'atm_address': str,
    #    'country': str,
    #    'city': str,
    #    'amount': np.float32,
    #    'currency': np.float32,
    #    'mcc': str,
    #    'customer_id': str,
    #    'pos_address': str,
    #    'atm_address': str,
    #    'pos_adress_lat': np.float32,
    #    'pos_adress_lon': np.float32,
    #    'pos_address_lat': np.float32,
    #    'pos_address_lon': np.float32,
    #    'atm_address_lat': np.float32,
    #    'atm_address_lon': np.float32,
    #    'home_add_lat': np.float32,
    #    'home_add_lon': np.float32,
    #    'work_add_lat': np.float32,
    #    'work_add_lon': np.float32,
    #}
    #​
    #rnm = {
    #    'atm_address_lat': 'atm_lat',
    #    'atm_address_lon': 'atm_lon',
    #    'pos_adress_lat': 'pos_lat',
    #    'pos_adress_lon': 'pos_lon',
    #    'pos_address_lat': 'pos_lat',
    #    'pos_address_lon': 'pos_lon',
    #    'home_add_lat': 'home_lat',
    #    'home_add_lon': 'home_lon',
    #    'work_add_lat': 'work_lat',
    #    'work_add_lon': 'work_lon',
    #}
    #
    #df_train = pd.read_csv('../data/train_set.csv', dtype=dtypes)
    #df_test = pd.read_csv('../data/test_set.csv', dtype=dtypes)
    #​
    #df_train.rename(columns=rnm, inplace=True)
    #df_test.rename(columns=rnm, inplace=True)
    #
    ## соединяем test/train в одном DataFrame
    #df_train['is_train'] = np.int32(1)
    #df_test['is_train'] = np.int32(0)
    #df_all = pd.concat([df_train, df_test])
    #​
    #del df_train, df_test
    #
    #df_all['add_lat'] = (df_all['atm_lat'].round(4).fillna(0) * 1000).astype(int) + (
    #            df_all['pos_lat'].round(4).fillna(0) * 1000).astype(int)
    #df_all['add_lon'] = (df_all['atm_lon'].round(4).fillna(0) * 1000).astype(int) + (
    #            df_all['pos_lon'].round(4).fillna(0) * 1000).astype(int)
    #
    #% % time
    #df_all = pd.merge(df_all, coords, on=['add_lat', 'add_lon'], how='left')
    #CPU
    #times: user
    #513
    #ms, sys: 40
    #ms, total: 553
    #ms
    #Wall
    #time: 552
    #ms
    #
    #coords.drop_duplicates(subset=['add_lat', 'add_lon'], inplace=True)
    #
    #df_all.drop(['add_lat', 'add_lon', 'action_lat', 'action_lon', ], axis=1, inplace=True)
    #
    #df_all.columns
    #Index(['amount', 'atm_address', 'atm_lat', 'atm_lon', 'city', 'country',
    #       'currency', 'customer_id', 'home_lat', 'home_lon', 'is_train', 'mcc',
    #       'pos_address', 'pos_lat', 'pos_lon', 'terminal_id', 'transaction_date',
    #       'work_lat', 'work_lon', 'string', 'city_name'],
    #      dtype='object')
    #
    #df_all.to_csv('../data/df_all.csv', index=None)
    #Замена
    #городов
    #Чето
    #слегка
    #ухудшило
    #скор
    #
    #city_replace = [
    #    ['peter|stpete|spb', 'SANKT-PETERBU'],
    #    ['moscow|moskva|mosocw|moskow', 'MOSCOW'],
    #    ['novosib|nvsibr', 'NOVOSIBIRSK'],
    #    ['kater', 'EKATERINBURG'],
    #    ['n.*novg', 'NIZHNIY NOV'],
    #    ['novg', 'VEL.NOVGOROD'],
    #    ['erep', 'CHEREPOVETS'],
    #    ['rasnod', 'KRASNODAR'],
    #    ['rasno[yj]', 'KRASNOYARSK'],
    #    ['sama', 'SAMARA'],
    #    ['kazan', 'KAZAN'],
    #    ['soch[iy]', 'SOCHI'],
    #    ['r[yj]aza', 'RYAZAN'],
    #    ['arza', 'ARZAMAS'],
    #    ['podol.?sk', 'PODOLSK'],
    #    ['himki', 'KHIMKI'],
    #    ['rostov', 'ROSTOV'],  # will ovveride Rostov-Na-Don later
    #    ['rostov.*do', 'ROSTOV-NA-DON'],
    #    ['ufa', 'UFA'],
    #    ['^orel|ory[oe]l', 'OREL'],
    #    ['korol', 'KOROLEV'],
    #    ['vkar', 'SYKTYVKAR'],
    #    ['rozavo|rzavo', 'PETROZAVODSK'],
    #    ['c.*abinsk', 'CHELYABINSK'],
    #    ['g omsk|^omsk', 'OMSK'],
    #    ['tomsk', 'TOMSK'],
    #    ['vorone', 'VORONEZH'],
    #    ['[yj]arosl', 'YAROSLAVL'],
    #    ['novoros', 'NOVOROSSIYSK'],
    #    ['m[yie]t[yi]s', 'MYTISHCHI'],
    #    ['kal..?ga', 'KALUGA'],
    #    ['perm', 'PERM'],
    #    ['volgog|volgrd', 'VOLGOGRAD'],
    #    ['kirov[^a-z]|kirov$', 'KIROV'],
    #    ['krasnogo', 'KRASNOGORSK'],
    #    ['^mo\W+$|^mo$', 'MO'],
    #    ['irk', 'IRKUTSK'],
    #    ['balashi', 'BALASHIKHA'],
    #    ['kaliningrad', 'KALININGRAD'],
    #    ['anap', 'ANAPA'],
    #    ['surgut', 'SURGUT'],
    #    ['odin[tc]', 'ODINTSOVO'],
    #    ['kemer', 'KEMEROVO'],
    #    ['t[yuio].?men', 'TYUMEN'],
    #    ['sarat', 'SARATOV'],
    #    ['t[uoy]u?la', 'TULA'],
    #    ['bert', 'LYUBERTSY'],
    #    ['kotel', 'KOTELNIKI'],
    #    ['lipet', 'LIPETSK'],
    #    ['leznodor', 'ZHELEZNODOROZ'],
    #    ['domod', 'DOMODEDOVO'],
    #    ['br[yji][a]nsk|braynsk', 'BRYANSK'],
    #    ['saransk', 'SARANSK'],
    #    ['znogor', 'ZHELEZNOGORSK'],
    #    ['smol', 'SMOLENSK'],
    #    ['sevolo', 'VSEVOLOZHSK'],
    #    ['p[uy].*kino', 'PUSHKINO'],
    #    ['re..?tov', 'REUTOV'],
    #    ['kursk|koursk', 'KURSK'],
    #    ['belgorod', 'BELGOROD'],
    #    ['r[yj]azan', 'RYAZAN'],
    #    ['solnechno', 'SOLNECHNOGORS'],
    #    ['utorovsk', 'YALUTOROVSK'],
    #    ['tver', 'TVER'],
    #    ['barn', 'BARNAUL'],
    #    ['to.?l..?.?tt[iy]', 'TOLYATTI'],
    #    ['i[zjg].?evsk', 'IZHEVSK']
    #]
    #​
    #df_all['city'] = df_all['city'].str.lower()
    #df_all['city'].fillna('nan_city', inplace=True)
    #for city_reg, city_name in tqdm(city_replace):
    #    df_all.loc[df_all['city'].str.contains(city_reg), 'city'] = city_name
    #Обрабатываем
    #дату
    #транзакции
    #и
    #категориальные
    #признаки
    #
    #df_all['currency'] = df_all['currency'].fillna(-1).astype(np.int32)
    #df_all['mcc'] = df_all['mcc'].apply(lambda x: int(x.replace(',', ''))).astype(np.int32)
    #df_all['city'] = df_all['city'].factorize()[0].astype(np.int32)
    #df_all['country'] = df_all['country'].factorize()[0].astype(np.int32)
    #Фичи
    #для
    #даты
    #
    ## удаляем транзакции без даты
    #df_all = df_all[~df_all['transaction_date'].isnull()]
    #df_all['transaction_date'] = pd.to_datetime(df_all['transaction_date'], format='%Y-%m-%d')
    #
    #df_all['month'] = df_all.transaction_date.dt.month
    #df_all['day'] = df_all.transaction_date.dt.day
    #df_all['dayofyear'] = df_all.transaction_date.dt.dayofyear
    #df_all['dayofweek'] = df_all.transaction_date.dt.dayofweek
    #
    ## праздники
    #holidays_df = pd.read_csv('../data/internal/all_holidays.csv', header=None)
    #holidays_df[0] = pd.to_datetime(holidays_df[0])
    #holidays_df = holidays_df[holidays_df[0].dt.year == 2017]
    #holidays = holidays_df[0].dt.dayofyear.values
    #df_all['is_weekend'] = (df_all.dayofweek >= 6).astype(np.int8)
    #df_all['is_state_holiday'] = df_all['dayofyear'].isin(holidays).astype(np.int8)
    #df_all['is_holiday'] = df_all['is_weekend'] | df_all['is_state_holiday']
    #Приводим
    #адрес
    #транзакции
    #для
    #pos
    #и
    #atm - транзакций
    #к
    #единообразному
    #виду
    #Просто
    #объединяем
    #в
    #одну
    #колонку
    #и
    #добавляем
    #фичу - это
    #атм
    #или
    #пос
    #
    #df_all['is_atm'] = (~df_all['atm_lat'].isnull()).astype(np.int8)
    #df_all['is_pos'] = (~df_all['pos_lat'].isnull()).astype(np.int8)
    #​
    #df_all['add_lat'] = df_all['atm_lat'].fillna(0) + df_all['pos_lat'].fillna(0)
    #df_all['add_lon'] = df_all['atm_lon'].fillna(0) + df_all['pos_lon'].fillna(0)
    #​
    #df_all.drop(['atm_lat', 'atm_lon', 'pos_lat', 'pos_lon'], axis=1, inplace=True)
    #​
    #df_all = df_all[~((df_all['add_lon'] == 0) & (df_all['add_lon'] == 0))]
    #
    #% % time
    ## грязный хак, чтобы не учить КНН на новом юзере каждый раз
    #df_all['fake_customer_id'] = (pd.factorize(df_all.customer_id)[0] + 1) * 100
    #​
    #points = df_all[['fake_customer_id', 'add_lat', 'add_lon']].drop_duplicates().values
    #neigh = NearestNeighbors(2, radius=100000)
    #​
    ## расстояние до уникальных точек
    ## neigh.fit(np.unique(points, axis=1))
    #neigh.fit(points)
    #​
    #distances, indices = neigh.kneighbors(df_all[['fake_customer_id', 'add_lat', 'add_lon']].values)
    #df_all['distance_to_nearest_point'] = distances[:, 1]
    #del df_all['fake_customer_id']
    #Кластерные
    #признаки
    #Сохранены
    #в
    #df_cluster
    #
    ## фичи с кластерами из тинькова
    #dfs = []
    #customers = df_all.customer_id.unique()
    #np_values = df_all[['customer_id', 'add_lat', 'add_lon']].values
    #​
    #for i in tqdm(range(len(customers))):
    #    customer = customers[i]
    #    points = np_values[np_values[:, 0] == customer][:, 1:]
    #    # оцениваем число кластеров
    #    #     avgs = []
    #    #     max_cluster = min(10,len(points))
    #    #     for i in range(2,max_cluster):
    #    #         kmeans = KMeans(n_clusters=i, random_state=2).fit(points)
    #    #         labels = kmeans.labels_
    #    #         silhouette_avg = silhouette_score(points, labels)
    #    #         avgs.append(silhouette_avg)
    #
    #    #     if max_cluster == 2:
    #    #         kmeans = KMeans(n_clusters=2, random_state=2).fit(points)
    #    #         labels = kmeans.labels_
    #    #         silhouette_avg = silhouette_score(points, labels)
    #    #         avgs.append(silhouette_avg)
    #
    #    #     n_cluster = avgs.index(max(avgs)) + 2 # так как индексы с 0 а кластеры с 2
    #    # получаем лучший кластер
    #    if np.unique(points).size == 2:
    #        dfs.append(np.zeros((len(points), 4)))
    #        continue
    #    n_cluster = 2
    #    kmeans = KMeans(n_clusters=n_cluster, random_state=2).fit(points)
    #    # kmeans = AgglomerativeClustering(n_clusters=n_cluster,linkage='average').fit(points)
    #    labels = kmeans.labels_
    #    centers = kmeans.cluster_centers_
    #    silhouette_avg = silhouette_score(points, labels)
    #    # формируем датафрейм
    #    sample_silhouette_values = silhouette_samples(points, labels)
    #    #     cluster_df = pd.DataFrame(data=np.vstack((labels, sample_silhouette_values)).T,columns=['label','score'])
    #    #     cluster_df.label = cluster_df.label.astype(np.int32)
    #    #     cluster_df['cluster_center_lat'] = cluster_df.apply(lambda row: centers[int(row['label'])][0], axis=1)
    #    #     cluster_df['cluster_center_lon'] = cluster_df.apply(lambda row: centers[int(row['label'])][1], axis=1)
    #    arr_label_score = np.vstack((labels, sample_silhouette_values)).T
    #    arr_label_score = np.hstack([arr_label_score, centers[labels]])
    #    dfs.append(arr_label_score)
    #
    #df_cluster = pd.DataFrame(np.vstack(dfs), columns=['cl_label', 'cl_score', 'cl_lat', 'cl_lon'])
    #df_all.reset_index(inplace=True, drop=True)
    #df_all = pd.concat([df_all, df_cluster], axis=1)
    #
    #df_all.to_csv('../data/df_all_1.csv', index=None)
    #
    #df_all = pd.read_csv('../data/df_all_1.csv')
    #df_all[['customer_id', 'add_lat', 'add_lon', 'cl_label',
    #        'cl_score', 'cl_lat', 'cl_lon']].to_csv('../data/df_cluster.csv', index=None)
    #df_all.head()
    #загружаем
    #кластерные
    #признаки
    #
    #df_cluster = pd.read_csv('../data/df_cluster.csv')
    #df_cluster.reset_index(drop=True, inplace=True)
    #df_cluster.head()
    #
    #df_all.reset_index(drop=True, inplace=True)
    #
    #df_all = pd.concat([df_all, df_cluster.iloc[:, 3:]], axis=1)
    #Генерируем
    #признаки
    #is_home, is_work
    #TODO: удалить
    #чуваков
    #у
    #которых
    #несколько
    #домов
    #
    #lat = df_all['home_lat'] - df_all['add_lat']
    #lon = df_all['home_lon'] - df_all['add_lon']
    #​
    #df_all['is_home'] = (np.sqrt((lat ** 2) + (lon ** 2)) <= 0.02).astype(np.int8)
    #df_all['has_home'] = (~df_all['home_lon'].isnull()).astype(np.int8)
    #​
    #lat = df_all['work_lat'] - df_all['add_lat']
    #lon = df_all['work_lon'] - df_all['add_lon']
    #df_all['is_work'] = (np.sqrt((lat ** 2) + (lon ** 2)) <= 0.02).astype(np.int8)
    #df_all['has_work'] = (~df_all['work_lon'].isnull()).astype(np.int8)
    #​
    ## df_all.drop(['work_lat','work_lon','home_lat','home_lon'], axis=1, inplace=True)
    #Генерируем
    #категориальный
    #признак
    #для
    #адреса
    #
    #df_all['address'] = df_all['add_lat'].apply(lambda x: "%.02f" % x) + ';' + df_all['add_lon'].apply(
    #    lambda x: "%.02f" % x)
    #df_all['address'] = df_all['address'].factorize()[0].astype(np.int32)
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
    #Мои
    #фичи
    #
    ## добавим признаки после групбая
    #df_gb = df_all[['customer_id', 'amount', 'add_lat', 'add_lon']].groupby('customer_id')
    #coord_stat_df = df_gb.agg(['mean', 'max', 'min'])
    #coord_stat_df['transactions_per_user'] = df_gb.agg('size')
    #coord_stat_df.columns = ['_'.join(col).strip() for col in coord_stat_df.columns.values]
    #coord_stat_df.reset_index(inplace=True)
    #df_all = pd.merge(df_all, coord_stat_df, on='customer_id', how='left')
    #
    #cols = ['add_lat', 'add_lon']
    #types = ['min', 'max', 'mean']
    #for c in cols:
    #    for t in types:
    #        df_all['{}_diff_{}'.format(c, t)] = np.abs(df_all[c] - df_all['{}_{}'.format(c, t)])
    #
    ## разности
    #df_all['lat_diff_cluster_lat'] = np.abs(df_all['add_lat'] - df_all['cl_lat'])
    #df_all['lon_diff_cluster_lon'] = np.abs(df_all['add_lon'] - df_all['cl_lon'])
    #Фичи
    #mcc
    #
    ## категории
    #df_all['mcc_str'] = df_all['mcc'].astype(str).str.rjust(4, '0')
    #df_mcc = pd.read_csv('../data/internal/mcc.csv')
    #df_mcc = df_mcc.iloc[1:, :3]
    #df_mcc.columns = ['mcc_str', 'mcc_cat1', 'mcc_cat2']
    #df_mcc.drop_duplicates(subset=['mcc_str'], inplace=True)
    #df_mcc['mcc_cat1'] = pd.factorize(df_mcc['mcc_cat1'])[0]
    #df_mcc['mcc_cat2'] = pd.factorize(df_mcc['mcc_cat2'])[0]
    #df_mcc.fillna('none', inplace=True)
    #df_all = pd.merge(df_all, df_mcc, on='mcc_str', how='left')
    #del df_all['mcc_str']
    #df_mcc.head()
    #
    ### WTF???
    #df_all = add_count_sum_ratios(df_all, 'mcc_cat1')
    #df_all = add_count_sum_ratios(df_all, 'mcc_cat2')
    #
    ## частота mcc
    #df_mcc = df_all['mcc'].value_counts(normalize=True).reset_index()
    #df_mcc.columns = ['mcc', 'mcc_freq']
    #df_all = pd.merge(df_all, df_mcc, on='mcc', how='left')
    #
    #df_all = pd.concat([df_all, pd.get_dummies(df_all['mcc'], prefix='mcc')], axis=1)
    #del df_all['mcc']
    #
    ## df_all = pd.concat([df_all, pd.get_dummies(df_all['mcc_cat1'], prefix='mcc_cat1')], axis=1)
    ## del df_all['mcc_cat1']
    #​
    #df_all = pd.concat([df_all, pd.get_dummies(df_all['mcc_cat2'], prefix='mcc_cat2')], axis=1)
    #del df_all['mcc_cat2']
    #
    ## сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #mcc_cols = [c for c in df_all.columns if 'mcc' in c and 'cat' not in c]
    #df_mcc = df_all.groupby('customer_id')[mcc_cols].agg(['max', 'mean'])
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc.reset_index(inplace=True)
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #
    ## сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #mcc_cols = [c for c in df_all.columns if 'mcc_cat1' in c]
    #df_mcc = df_all.groupby('customer_id')[mcc_cols].agg(['max', 'mean'])
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc.reset_index(inplace=True)
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #
    ## сделаем групбай какие вообще есть mcc у посетителя. Это поможет понять его привычки
    #mcc_cols = [c for c in df_all.columns if 'mcc_cat2' in c]
    #df_mcc = df_all.groupby('customer_id')[mcc_cols].agg(['max', 'mean'])
    #df_mcc.columns = ['_'.join(col).strip() for col in df_mcc.columns.values]
    #df_mcc.reset_index(inplace=True)
    #df_mcc.head()
    #df_all = pd.merge(df_all, df_mcc, on='customer_id', how='left')
    #LightGBM
    #
    #df_all.shape
    #
    #df_all = df_all.loc[:, ~df_all.columns.duplicated()]
    #
    #from sklearn.model_selection import train_test_split
    #​
    #ys = ['is_home', 'is_work']
    #drop_cols = ['atm_address', 'customer_id', 'pos_address', 'terminal_id', 'transaction_date',
    #             'is_home', 'has_home', 'is_work', 'has_work', 'is_train']
    #drop_cols += ['work_lat', 'work_lon', 'home_lat', 'home_lon']
    #​
    #drop_cols += ['pred:is_home', 'pred:is_work']
    #y_cols = ['is_home', 'is_work']
    #usecols = df_all.drop(drop_cols, 1, errors='ignore').columns
    #
    #params = {
    #    'objective': 'binary',
    #    'num_leaves': 63,
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
    #                  early_stopping_rounds=300)
    #​
    #model[y_col] = gbm_h
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
    #                  early_stopping_rounds=300)
    #​
    #model[y_col] = gbm_w
    #
    #lgb.plot_importance(gbm_h, max_num_features=15)
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
    #df_an
    #Predict
    #
    #cust_test = df_all[df_all['is_train'] == 0]['customer_id'].unique()
    #df_test = pd.DataFrame(cust_test, columns=['customer_id']).merge(df_all, how='left')
    #df_test = predict_proba(df_test)
    #df_test.rename(columns={
    #    'customer_id': '_ID_',
    #    'is_home:add_lat': '_HOME_LAT_',
    #    'is_home:add_lon': '_HOME_LON_',
    #    'is_work:add_lat': '_WORK_LAT_',
    #    'is_work:add_lon': '_WORK_LON_'}, inplace=True)
    #df_test = df_test[['_ID_', '_WORK_LAT_', '_WORK_LON_', '_HOME_LAT_', '_HOME_LON_']]
    #​
    #df_test.head()
    #Формируем
    #submission - файл
    #
    ## Заполняем пропуски
    #df_ = pd.read_csv('../data/test_set.csv', dtype=dtypes, usecols=['customer_id'])
    #submission = pd.DataFrame(df_['customer_id'].unique(), columns=['_ID_'])
    #​
    #submission = submission.merge(df_test, how='left').fillna(0)
    ## Пишем файл submission
    #submission.to_csv('../submissions/base_3_529_333.csv', index=None)
    #
    #
    return src

def update_last_partition(dst, from_dt, to_dt):
    prev_day = datetime.strptime(from_dt, '%Y-%m-%d') - timedelta(days=1)
    res = spark.table(dst["d_train"]).checkpoint()
    res = res.where(res.day == to_dt)
    res = res.withColumn("period_to_dt", f.lit(prev_day)).withColumn("day", f.lit(prev_day.strftime('%Y-%m-%d')))
    res.coalesce(8).write.format("orc").insertInto(dst["d_train"], overwrite=True)


def calc_03(src, dst, from_dt, to_dt):
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
    spark = SparkSession.builder.appName("calc_03_task").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    hivecontext = HiveContext(spark.sparkContext)
    hivecontext.setConf("hive.exec.dynamic.partition", "true")
    hivecontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setCheckpointDir("hdfs:///user/airflow/psg/calc_03_task")

    opts = {
        'from_dt': sys.argv[1],
        "to_dt": "9999-12-31"
    }

    update_last_partition(prod_dst(), opts["from_dt"], opts["to_dt"])
    calc_03(prod_src(), prod_dst(), opts["from_dt"], opts["to_dt"])

