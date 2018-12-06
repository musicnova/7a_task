import sys
from datetime import timedelta, datetime


from pyspark import HiveContext
from pyspark.sql import functions as f, SparkSession


def algo(src, from_dt, to_dt):
    res = steps(src, from_dt, to_dt)
    return res


def steps(src, from_dt, to_dt):

    import sys
    MODULES_PATH = '../code/'
    if MODULES_PATH not in sys.path:
        sys.path.append(MODULES_PATH)
    import mfuncs

    import pandas as pd
    import numpy as np
    from tqdm import tqdm
    tqdm.pandas()
    pd.options.display.max_columns = 1000

    import lightgbm as lgb

    from sklearn.neighbors import NearestNeighbors

    # start of step 01
    df_train = pd.read_csv('../data/train_set.csv')
    df_test = pd.read_csv('../data/test_set.csv')
    rnm = {
        'atm_address_lat': 'atm_lat',
        'atm_address_lon': 'atm_lon',
        'pos_adress_lat': 'pos_lat',
        'pos_adress_lon': 'pos_lon',
        'home_add_lat': 'home_lat',
        'home_add_lon': 'home_lon',
        'work_add_lat': 'work_lat',
        'work_add_lon': 'work_lon',
    }
    df_train.rename(columns=rnm, inplace=True)
    df_test.rename(columns=rnm, inplace=True)


    # start of step 02
    df_train['target_work'] = df_train.progress_apply(mfuncs.add_poswork_target, axis=1)
    df_train['target_home'] = df_train.progress_apply(mfuncs.add_poshome_target, axis=1)


    # start of step 03
    df_train.to_csv('../data/train_1.csv', index=None)

    # start of step 04
    df_train.info()

    # start of step 05
    df_train.head()

    # start of step 06
    df_train.country.value_counts(normalize=True)[:10]
    print(df_train.shape, df_test.shape)
    df_train = df_train[df_train.country.isin(['RUS', 'RU'])]
    df_test = df_test[df_test.country.isin(['RUS', 'RU'])]
    print(df_train.shape, df_test.shape)
    del df_train['country'], df_test['country']

    # start of step 07
    print(df_train.shape, df_train.currency.value_counts(normalize=True))
    df_train = df_train[df_train.currency == 643]
    print(df_train.shape)
    del df_train['currency']

    # start of step 08
    print(df_train.shape, df_train.currency.value_counts(normalize=True))
    df_train = df_train[df_train.currency == 643]
    print(df_train.shape)
    del df_train['currency']

    # start of step 09
    print(df_train.shape)
    gb = df_train.groupby('customer_id')['work_lat'].agg('nunique')
    cid_incorrect = gb[gb == 2].index
    df_train = df_train[~df_train.customer_id.isin(cid_incorrect.values)]
    print(df_train.shape)
    gb = df_train.groupby('customer_id')['home_lat'].agg('nunique')
    cid_incorrect = gb[gb == 2].index
    df_train = df_train[~df_train.customer_id.isin(cid_incorrect.values)]
    print(df_train.shape)

    # start of step 10
    print(df_train.shape)
    df_train = df_train[df_train[['atm_lat', 'pos_lat']].isnull().sum(axis=1) == 1]
    print(df_train.shape)
    df_train['type'] = 'atm'
    df_train.loc[~df_train['pos_lat'].isnull(), 'type'] = 'pos'
    df_train['type'].value_counts()

    # start of step 11
    cid = df_train.sample(1)['customer_id'].values[0]
    df_an = df_train[df_train.customer_id == cid]
    df_point_dup = df_an.groupby(['pos_lat', 'pos_lon']).agg('size').reset_index()
    df_point_dup.columns = ['pos_lat', 'pos_lon', 'pos_customer_freq']
    df_an = pd.merge(df_an, df_point_dup, on=['pos_lat', 'pos_lon'], how='left')

    df_an.head()

    # start of step 12
    df_train.head()
    df_train[df_train.type == 'pos'].drop_duplicates(['pos_lat',
                                                      'pos_lon']).groupby(['terminal_id']).agg('size').value_counts()
    df_train[df_train.type == 'atm'].drop_duplicates(['atm_lat',
                                                      'atm_lon']).groupby(['terminal_id']).agg('size').value_counts()
    df_train[df_train.terminal_id == '1e15d02895068c3a864432f0c06f5ece']['atm_address'].unique()
    df_train[df_train.type == 'atm'].drop_duplicates(['atm_lat',
                                                      'atm_lon']).groupby(['terminal_id']).agg('size')

    import gmaps
    API_KEY = 'AIzaSyCG_RL0_kavuEaJAqEN5xXbU4h0VJUbA9M'
    gmaps.configure(api_key=API_KEY)  # Your Google API key

    cid = '0dc0137d280a2a82d2dc89282450ff1b'
    cid = df_train.sample(1)['customer_id'].values[0]
    df_an = df_train[df_train.customer_id == cid]
    center_home = df_an[['home_lat', 'home_lon']].drop_duplicates().values
    center_work = df_an[['work_lat', 'work_lon']].drop_duplicates().values
    points_pos = df_an[['pos_lat', 'pos_lon']].dropna().values
    points_atm = df_an[['atm_lat', 'atm_lon']].dropna().values
    print(center_home.shape, center_work.shape, points_pos.shape, points_atm.shape)

    gmap = gmaps.Map()
    if len(points_pos) > 0:
        gmap.add_layer(gmaps.symbol_layer(points_pos, hover_text='pos',
                                          fill_color="blue", stroke_color="blue", scale=3))
    if len(points_atm) > 0:
        gmap.add_layer(gmaps.symbol_layer(points_atm, hover_text='atm',
                                          fill_color="red", stroke_color="red", scale=3))

    if not np.isnan(center_home)[0][0]:
        gmap.add_layer(gmaps.marker_layer(center_home, label='home'))
    if not np.isnan(center_work)[0][0]:
        gmap.add_layer(gmaps.marker_layer(center_work, label='work'))

    gmap

    center_home = df_train[['home_lat', 'home_lon']].dropna().values
    center_work = df_train[['work_lat', 'work_lon']].dropna().values

    gmap = gmaps.Map()
    gmap.add_layer(gmaps.symbol_layer(center_home, fill_color="red", stroke_color="red"))
    gmap

    np.isnan(center_home)

    df_train.groupby(['customer_id']).agg('size').sort_values().value_counts()

    df_test.customer_id.drop_duplicates().isin(df_train.customer_id.unique()).mean()

    df_train['duplicated'] = df_train.duplicated()

    df_pos = df_train[df_train['type'] == 'pos']
    # target == pos in
    df_pos['target_work'] = df_pos.progress_apply(mfuncs.add_poswork_target, axis=1)
    df_pos['target_home'] = df_pos.progress_apply(mfuncs.add_poshome_target, axis=1)

    df_pos['target_work'].mean(), df_pos['target_home'].mean()

    df_pos.to_csv('../data/df_pos.csv', index=None)

    df_pos = pd.read_csv('../data/df_pos.csv')

    df_point_dup = df_pos.groupby(['customer_id', 'pos_lat', 'pos_lon']).agg('size').reset_index()
    df_point_dup.columns = ['customer_id', 'pos_lat', 'pos_lon', 'pos_customer_freq']
    df_pos = pd.merge(df_pos, df_point_dup, on=['customer_id', 'pos_lat', 'pos_lon'], how='left')

    dfs = []
    for cid in tqdm(df_pos.customer_id.unique()):
        df_an = df_pos[df_pos.customer_id == cid]
        df_an = mfuncs.add_dist_to_neighbours(df_an)
        dfs.append(df_an)

    df_pos['transaction_date'] = pd.to_datetime(df_pos['transaction_date'], format='%Y-%m-%d')
    df_pos['month'] = df_pos.transaction_date.dt.month
    df_pos['day'] = df_pos.transaction_date.dt.day
    df_pos['dayofyear'] = df_pos.transaction_date.dt.dayofyear
    df_pos['dayofweek'] = df_pos.transaction_date.dt.dayofweek
    df_pos.transaction_date.dtype

    df_gb = df_pos.groupby('customer_id')
    coord_stat_df = df_gb[['amount', 'pos_lat', 'pos_lon']].agg(['mean', 'max', 'min'])
    coord_stat_df['transactions_per_user'] = df_gb.agg('size')
    coord_stat_df.columns = ['_'.join(col).strip() for col in coord_stat_df.columns.values]
    coord_stat_df.reset_index(inplace=True)
    df_pos = pd.merge(df_pos, coord_stat_df, on='customer_id', how='left')

    cols = ['pos_lat', 'pos_lon']
    types = ['min', 'max', 'mean']
    for c in cols:
        for t in types:
            df_pos['{}_diff_{}'.format(c, t)] = np.abs(df_pos[c] - df_pos['{}_{}'.format(c, t)])

    df_pos = pd.concat([df_pos, pd.get_dummies(df_pos['mcc'], prefix='mcc')], axis=1)
    del df_pos['mcc']

    df_pos.head()

    drop_cols = ['customer_id', 'terminal_id', 'target_home', 'target_work', 'atm_address',
                 'pos_address', 'work_add_lat', 'work_add_lon', 'home_add_lat', 'home_add_lon',
                 'city', 'type', 'transaction_date']
    drop_cols += ['atm_address', 'atm_address_lat', 'atm_address_lon']
    df_pos.drop(drop_cols, 1, errors='ignore').head()
    # drop_cols = ['pos_address', 'pos_address_lat', 'pos_address_lon']

    from sklearn.model_selection import train_test_split, StratifiedKFold, KFold
    df_pos_id = df_pos.customer_id.drop_duplicates().reset_index(drop=True)
    skf_id = list(KFold(n_splits=5, shuffle=True, random_state=15).split(df_pos_id))
    skf = []
    for train_ind, test_ind in skf_id:
        train_ind_ = df_pos[df_pos.customer_id.isin(df_pos_id.loc[train_ind].values)].index.values
        test_ind_ = df_pos[df_pos.customer_id.isin(df_pos_id.loc[test_ind].values)].index.values
        skf.append([train_ind_, test_ind_])

    df_pos['target_work'].mean()

    df_pos.head()

    cid = '442fd7e3af4d8c3acd7807aa65bb5e85'
    df_an = df_pos[df_pos.customer_id == cid]

    df_an = mfuncs.add_dist_to_neighbours(df_an)

    df_pos.customer_id.unique

    if np.array([1]).size:
        print(1)

    lgb_train = lgb.Dataset(df_pos.drop(drop_cols, 1, errors='ignore'), df_pos['target_home'])

    params = {
        'objective': 'binary',
        'num_leaves': 511,
        'learning_rate': 0.05,
        #     'metric' : 'error',
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 1,
        'num_threads': 12,
        'verbose': 0,
    }

    gbm = lgb.cv(params,
                 lgb_train,
                 num_boost_round=2000,
                 folds=skf,
                 verbose_eval=10,
                 early_stopping_rounds=500)

    df_pos.loc[i2].shape

    i1, i2 = skf[0]
    df_pos[df_pos.loc[i1]]['customer_id'].unique

    df_pos.loc[i1]

    df_pos.dtypes

    res = df_pos
    return res

def update_last_partition(dst, from_dt, to_dt):
    prev_day = datetime.strptime(from_dt, '%Y-%m-%d') - timedelta(days=1)
    res = spark.table(dst["d_train"]).checkpoint()
    res = res.where(res.day == to_dt)
    res = res.withColumn("period_to_dt", f.lit(prev_day)).withColumn("day", f.lit(prev_day.strftime('%Y-%m-%d')))
    res.coalesce(8).write.format("orc").insertInto(dst["d_train"], overwrite=True)


def calc_06(src, dst, from_dt, to_dt):
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
    spark = SparkSession.builder.appName("calc_06_task").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    hivecontext = HiveContext(spark.sparkContext)
    hivecontext.setConf("hive.exec.dynamic.partition", "true")
    hivecontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setCheckpointDir("hdfs:///user/airflow/psg/calc_06_task")

    opts = {
        'from_dt': sys.argv[1],
        "to_dt": "9999-12-31"
    }

    update_last_partition(prod_dst(), opts["from_dt"], opts["to_dt"])
    calc_06(prod_src(), prod_dst(), opts["from_dt"], opts["to_dt"])

