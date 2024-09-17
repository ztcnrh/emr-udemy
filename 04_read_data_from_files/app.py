import os
from util import get_spark_session
from read import from_files
from pyspark.sql.functions import count


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    spark = get_spark_session(env, 'GitHub Activity - Reading Data')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df.printSchema()
    df.select('repo.*').show()
    # ----------TZ code below:
    # Count the rows
    row_count = df.count()

    # Print the row count
    print(f"Total number of rows: {row_count}")

    # Alternatively, if you want to see the count as part of a DataFrame:
    count_df = df.select(count('*')).alias('row_count')
    count_df.show()


if __name__ == '__main__':
    main()
