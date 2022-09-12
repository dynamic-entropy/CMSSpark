from datetime import datetime, timedelta
import click
import pandas as pd

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, collect_list, collect_set, concat_ws, first, format_number, from_unixtime, greatest, lit, lower, when,
    avg as _avg,
    count as _count,
    hex as _hex,
    max as _max,
    min as _min,
    round as _round,
    split as _split,
    sum as _sum,
)
from pyspark.sql.window import Window
import pyspark.sql.functions as func

from pyspark.sql.types import (
    LongType,
    DecimalType
)

print("Running spark script")
TODAY = (datetime.today()-timedelta(1)).strftime('%Y-%m-%d')
HDFS_RUCIO_LOCKS = f"/project/awg/cms/rucio/{TODAY}/locks/part*.avro"
HDFS_RUCIO_RSES = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/RSES/part*.avro'
HDFS_RUCIO_CONTENTS = f"/project/awg/cms/rucio/{TODAY}/contents/part*.avro"
HDFS_RUCIO_RULES = f"/project/awg/cms/rucio/{TODAY}/rules/part*.avro"


def get_df_locks(spark):
    return spark.read.format('avro').load(HDFS_RUCIO_LOCKS) \
        .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('rule_id', lower(_hex(col('RULE_ID')))) \
        .withColumn('file_size', col('BYTES').cast(LongType())) \
        .withColumnRenamed('NAME', 'file_name') \
        .select(['file_name', 'rse_id', 'rule_id', 'account', 'file_size'])


def get_df_rses(spark):
    df_rses = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_RSES) \
        .filter(col('DELETED_AT').isNull()) \
        .withColumn('id', lower(_hex(col('ID')))) \
        .withColumn('rse_tier', _split(col('RSE'), '_').getItem(0)) \
        .withColumn('rse_country', _split(col('RSE'), '_').getItem(1)) \
        .withColumn('rse_kind',
                    when((col("rse").endswith('Temp') | col("rse").endswith('temp') | col("rse").endswith('TEMP')),
                         'temp')
                    .when((col("rse").endswith('Test') | col("rse").endswith('test') | col("rse").endswith('TEST')),
                          'test')
                    .otherwise('prod')
                    ) \
        .withColumnRenamed('id', 'rse_id') \
        .withColumnRenamed('RSE', 'rse_name') \
        .withColumnRenamed('RSE_TYPE', 'rse_type') \
        .select(['rse_id', 'rse_name', 'rse_type', 'rse_tier', 'rse_country', 'rse_kind'])
    return df_rses


def get_df_contents(spark):
    return spark.read.format('avro').load(HDFS_RUCIO_CONTENTS) \
        .filter(col("scope")=="cms")\
        .select(['name', 'child_name', 'did_type', 'child_type'])


#TODO: Do we need to consider replicating rules here?
def get_df_rules(spark):
    return spark.read.format("avro").load(HDFS_RUCIO_RULES)\
        .filter(col('state')=='O') \
        .withColumn("rule_id", lower(_hex(col("id")))) \
        .select(["rule_id", "account", "did_type", "rse_expression", "copies"])


#get actual dataset names here
#We drop the datasets names with /CONTAINER - these are created for data transfer challenges and deletion campaigns
# If needed we deal with them separately
def get_dataset_file_map(spark):
    df_contents = get_df_contents(spark)
    
    block_file_map = df_contents.filter(col("child_type")=="F")\
                                  .filter(col("name").endswith("#DATASET")==False)\
                                  .filter(col("name").endswith("#DATASET2")==False)\
                                  .withColumnRenamed("child_name", "file")\
                                  .withColumnRenamed("name", "block")
                                 
        
    dataset_block_map = df_contents.filter(col("child_type")=="D")\
                                  .filter(col("name").endswith("/CONTAINER")==False)\
                                  .withColumnRenamed("child_name", "block")\
                                  .withColumnRenamed("name", "dataset")
        
    windowPartitionDataset = Window.partitionBy("dataset")
    #we do a right join to capture files that do not map to a dataset
    #later we club these files in a Unknown Container category
    dataset_file_map = dataset_block_map.alias("dbm").join(block_file_map.alias("bfm"), col("dbm.block")==col("bfm.block"), "right")\
                                  .na.fill({"dataset":"/UnknownDataset"})\
                                  .withColumn("data_tier", func.element_at(func.split("dataset","/"),-1))\
                                  .withColumn("file_count_contents",  func.count(col("dataset")).over(windowPartitionDataset))\
                                  .select(["dataset", "bfm.block", "bfm.file", "data_tier", "file_count_contents"])
    
    #We are creating some dummy datasets here (derived from block names), to get data_tier info for files as much as possible
#     df_dataset_file_map = df_contents_file\
#                                 .withColumn("dataset", func.element_at(func.split("name","#"),1))\
#                                 .withColumn("data_tier", func.element_at(func.split("dataset","/"),-1))\
#                                 .select(["file", "dataset", "data_tier"])
        
    return dataset_file_map

def get_df_filtered(spark, df_map, df_locks, df_rses, df_rules):
    
    df_filtered = df_map.alias("map").join(df_locks.alias("lock"), col("map.file")==col("lock.file_name"), "right")\
        .na.fill({"dataset":"/UnknownBlock", "block":"/UnknownBlock#unknown", "data_tier":"UnknownBlock"})\
        .filter(col("data_tier").isin(["UnknownBlock", "UnknownDataset"])==False)\
        .join(df_rses.alias("rse"), col("lock.rse_id")==col("rse.rse_id"))\
        .select(["dataset", "rse_name", "data_tier", "file_name", "file_size", "block", "rule_id", "account", "file_count_contents"])\
        .groupby(["dataset", "rse_name", "data_tier", "file_count_contents"]).agg(func.countDistinct("file_name").alias("file_count"), 
                                                        func.count("file_name").alias("total_file_locks_count"),
                                                        func.sum("file_size").alias("file_sum"),
                                                        func.countDistinct("block").alias("block_count"),
                                                        func.countDistinct("rule_id").alias("ruleid_count"),
                                                        collect_set("rule_id").alias("ruleid_set"),
                                                        func.countDistinct("account").alias("account_count"),
                                                        collect_set("account").alias("account_set"),
            )\
        .withColumn("percent_count", func.col("file_count")*100/func.col("file_count_contents"))

    return df_filtered


@click.command()
@click.option('--hdfs_out_dir', default=None, type=str, required=True,
              help='I.e. /tmp/${KERBEROS_USER}/ruciods/$(date +%Y-%m-%d) ')
def main(hdfs_out_dir):
    """Main function that run Spark dataframe creations and save results to HDFS directory as JSON lines
    """

    # HDFS output file format. If you change, please modify bin/cron4rucio_ds_mongo.sh accordingly.

    print("Running main function script")
    print("\n\n*************\n\n")
    # 
    # 
    # 
    # 

    write_format = 'json'
    write_mode = 'overwrite'

    spark = get_spark_session(app_name='cms-monitoring-rucio-rules')
    # Set TZ as UTC. Also set in the spark-submit confs.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df_map = get_dataset_file_map(spark)
    df_locks = get_df_locks(spark)
    df_rses = get_df_rses(spark)
    df_rules = get_df_rules(spark)

    filepath = hdfs_out_dir + ".json"
    df_filtered = get_df_filtered(spark, df_map, df_locks, df_rses, df_rules)
    df_filtered.write.save(path=filepath, format=write_format, mode=write_mode)

    print(f"saved file to {hdfs_out_dir}")
    print("\n\n*************\n\n")


if __name__ == '__main__':
    main()