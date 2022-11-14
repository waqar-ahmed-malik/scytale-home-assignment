import json
from pyspark.sql.functions import udf, col, explode, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, BooleanType
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

REPOSITORY_RESPONSE_FILE = 'gitlab-repo-response.json'
PULL_REQUEST_RESPONSE_FILE = 'gitlab-pr-response.json'


def get_json_data(json_file_path):
  with open(json_file_path) as f:
    return json.load(f)


repo_response_schema = ArrayType(
  StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("owner", StructType([
      StructField("username", StringType())
    ]))
  ])
)

pr_response_schema = ArrayType(
  StructType([
    StructField("iid", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("state", StringType(), True),
    StructField("merged_by", StructType([
      StructField("username", StringType())
    ]))
  ])
)

repo_udf = udf(get_json_data, repo_response_schema)
pr_udf = udf(get_json_data, pr_response_schema)

spark = SparkSession.builder.appName("First Spark Test").getOrCreate()

# requests
GetJsonDataRequest = Row("json_file_path")

repo_df = spark.createDataFrame([
            GetJsonDataRequest(REPOSITORY_RESPONSE_FILE)
          ])\
          .withColumn("execute", repo_udf(col("json_file_path")))

pr_df = spark.createDataFrame([
            GetJsonDataRequest(PULL_REQUEST_RESPONSE_FILE)
          ])\
          .withColumn("execute", pr_udf(col("json_file_path")))


repo_df = repo_df.select(explode(col("execute")).alias("results"))\
    .select(col("results.id").alias('repo_id'), col("results.name").alias('repo_name'), col("results.visibility").alias('visibility'), col('results.owner.username').alias('repo_owner')).withColumn('visibility', when(col("visibility").like("%+"), True).otherwise(False))


pr_df = pr_df.select(explode(col("execute")).alias("results"))\
    .select(col("results.iid").alias('pr_number'), col("results.title").alias('pr_title'), col("results.state").alias('pr_state'), col('results.merged_by.username').alias('pr_owner'))

repo_df.join(pr_df).show()

repo_df.join(pr_df).withColumn('source', lit('GitLab')).toPandas().to_json('gitlab-normalized.json', orient='records')

spark.stop()