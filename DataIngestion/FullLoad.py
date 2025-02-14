from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

def FullLoad(postgres_url, postgres_properties, postgres_table_name, hive_data):
    # Create spark session with hive enabled
    spark = SparkSession.builder \
        .appName("PySpark_Full_Load") \
        .enableHiveSupport() \
        .getOrCreate()

    
    

    # Read data from PostgresSQL
    df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
    df_postgres.show(3)

    # Rename column from "ID" to "policy_number"
    df_postgres = df_postgres.withColumnRenamed("ID", "POLICY_NUMBER")
    df_postgres.show(3)


    # Check if Hive table exists
    hive_database_name = hive_data['hive_database_name']
    hive_table_name = hive_data['hive_table_name']
    if not spark.catalog.tableExists("{}.{}".format(hive_database_name, hive_table_name)):
        # Create database if not exists
        spark.sql("CREATE DATABASE IF NOT EXISTS project1db")

        # Create Hive Internal table over project1db
        df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))

        # Read Hive table
        df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
        df.show()
    else:
        print("{}.{} exists!! Will not do the Full Load".format(hive_database_name, hive_table_name))

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    postgres_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver",
    }
    postgres_table_name = "car_insurance_claims"
    
    hive_data = {
        "hive_database_name": "project1db",
        "hive_table_name": "carInsuranceClaims"
    }
    FullLoad(postgres_url, postgres_properties, postgres_table_name, hive_data)
