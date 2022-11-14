import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from github_extract import GitHub
from gitlab_extract import GitLab


def extract_data():
    """Extraction python modules `github_extraction.py` and `gitlab_extraction.py`.
    Both the modules authenticate with the respective APIs using Access Tokens via
    Python's `request` module and extracts:
        - List of repositories for the given user
        - List of Pull Requests for each repository
        Once the raw data is extracted, it's being stored in JSON files under source folder name
    """
    github_obj = GitHub(os.environ['GITHUB_ACCESS_TOKEN'])
    github_obj.get_pull_requests()
    gitlab_obj = GitLab(
        os.environ['GITLAB_ACCESS_TOKEN'], os.environ['GITLAB_USER_ID'])
    gitlab_obj.get_merge_requests()


def normalize_data():
    """Based on the extracted data, both repositories and pull requests raw data is being read by
    spark using `pyspark` python module for each source. The read JSON data is being transformed as
    a spark DataFrame and gets through multiple transformations including, field rename, field
    addition and field calculation based on the source config.
    Once both the responses and transformed, a final DataFrame is being created by merging the
    transformed response and the final normalized data is being written to a new JSOn file
    `<source-name>-normalized.json`
    """
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
