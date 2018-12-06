import sys
from datetime import timedelta, datetime


from pyspark import HiveContext
from pyspark.sql import functions as f, SparkSession


def algo(src, from_dt, to_dt):

    pass

def update_last_partition(dst, from_dt, to_dt):
    prev_day = datetime.strptime(from_dt, '%Y-%m-%d') - timedelta(days=1)
    res = spark.table(dst["d_train"]).checkpoint()
    res = res.where(res.day == to_dt)
    res = res.withColumn("period_to_dt", f.lit(prev_day)).withColumn("day", f.lit(prev_day.strftime('%Y-%m-%d')))
    res.coalesce(8).write.format("orc").insertInto(dst["d_train"], overwrite=True)


def calc_05(src, dst, from_dt, to_dt):
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
    spark = SparkSession.builder.appName("calc_05_task").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    hivecontext = HiveContext(spark.sparkContext)
    hivecontext.setConf("hive.exec.dynamic.partition", "true")
    hivecontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setCheckpointDir("hdfs:///user/airflow/psg/calc_05_task")

    opts = {
        'from_dt': sys.argv[1],
        "to_dt": "9999-12-31"
    }

    update_last_partition(prod_dst(), opts["from_dt"], opts["to_dt"])
    calc_05(prod_src(), prod_dst(), opts["from_dt"], opts["to_dt"])

