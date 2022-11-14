import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from github_extract import GitHub
from gitlab_extract import GitLab


def extract_data():
    github_obj = GitHub(os.environ['GITHUB_ACCESS_TOKEN'])
    github_obj.get_pull_requests()
    gitlab_obj = GitLab(
        os.environ['GITLAB_ACCESS_TOKEN'], os.environ['GITLAB_USER_ID'])
    gitlab_obj.get_merge_requests()


def normalize_data():
    spark = SparkSession.builder.appName(
        "Scytale Home Assignment").getOrCreate()

    for source_config in os.listdir('configs/'):
        with open(os.path.join('configs/', source_config), 'r') as f:
            config = json.load(f)
        source_name = source_config.split('-')[0]

        repo_df = spark.read.option("multiline", "true").json(
            f"{source_name}/repositories/*.json")

        pr_df = spark.read.option("multiline", "true").json(
            f"{source_name}/pull-requests/*.json")

        repo_df = repo_df.select([col(config_field['source_field']).alias(config_field['warehouse_field'])
                                 for config_field in config if not config_field['transformation_expression'] and config_field['source'] == 'repository_response'])
        pr_df = pr_df.select([col(config_field['source_field']).alias(config_field['warehouse_field'])
                             for config_field in config if not config_field['transformation_expression'] and config_field['source'] == 'pull_requests_response'])

        normalized_df = repo_df.join(pr_df)

        for config_field in config:
            if config_field['transformation_expression'] == 'Field Addition':
                normalized_df = normalized_df.withColumn(
                    config_field['warehouse_field'], (lit(config_field['warehouse_value'])))

        if source_name == 'gitlab':
            normalized_df = normalized_df.withColumn('is_private', when(
                col("is_private").like("private"), True).otherwise(False))

        normalized_df.toPandas().to_json(
            f'{source_name}-normalized.json', orient='records')
        print("File Created: ", source_name)

    spark.stop()


extract_data()
normalize_data()
