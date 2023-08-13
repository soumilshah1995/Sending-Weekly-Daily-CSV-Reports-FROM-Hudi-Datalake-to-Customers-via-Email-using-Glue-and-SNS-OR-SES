"""
Author : Soumil Nitin Shah
Email shahsoumil519@gmail.com
--additional-python-modules  | faker==11.3.0
--conf  |  spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
--datalake-formats | hudi
"""

try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    import os, sys, json, uuid, boto3, ast, time, datetime, re
    from enum import Enum
    from abc import ABC, abstractmethod
    from datetime import datetime
    from dataclasses import dataclass
    import pandas as pd
    from ast import literal_eval
    from io import BytesIO

    from pyspark.sql.functions import lit, udf
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
except Exception as e:
    print("Error*** : {}".format(e))

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Create a Spark session
spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

# Create a Spark context and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)
# ---------------------- Settings --------------------
global BUCKET_NAME
DYNAMODB_LOCK_TABLE_NAME = 'hudi-lock-table'
curr_session = boto3.session.Session()
curr_region = curr_session.region_name
BUCKET_NAME = "XXX"


# ---------------------------------------------------------


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):
        self.BucketName = bucket
        self.client = boto3.client("s3")

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "

    def generate_pre_signed_url(self, file_key, expires_in=86400):
        url = self.client.generate_presigned_url('get_object',
                                                 Params={'Bucket': self.BucketName, 'Key': file_key},
                                                 ExpiresIn=expires_in)
        return url


@dataclass
class HUDISettings:
    """Class for keeping track of an item in inventory."""

    table_name: str
    path: str


class HUDIIncrementalReader(AWSS3):
    def __init__(self, bucket, hudi_settings, spark_session):
        AWSS3.__init__(self, bucket=bucket)
        if type(hudi_settings).__name__ != "HUDISettings": raise Exception("please pass correct settings ")
        self.hudi_settings = hudi_settings
        self.spark = spark_session

    def __check_meta_data_file(self):
        """
        check if metadata for table exists
        :return: Bool
        """
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        return self.item_exists(Key=file_name)

    def __read_meta_data(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"

        return ast.literal_eval(self.get_item(Key=file_name).decode("utf-8"))

    def __push_meta_data(self, json_data):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.put_files(
            Key=file_name, Response=json.dumps(json_data)
        )

    def clean_check_point(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.delete_object(Key=file_name)

    def __get_begin_commit(self):
        self.spark.read.format("hudi").load(self.hudi_settings.path).createOrReplaceTempView("hudi_snapshot")
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_snapshot order by commitTime asc").limit(
            50).collect()))

        """begin from start """
        begin_time = int(commits[0]) - 1
        return begin_time

    def __read_inc_data(self, commit_time):
        incremental_read_options = {
            'hoodie.datasource.query.type': 'incremental',
            'hoodie.datasource.read.begin.instanttime': commit_time,
        }
        incremental_df = self.spark.read.format("hudi").options(**incremental_read_options).load(
            self.hudi_settings.path).createOrReplaceTempView("hudi_incremental")

        df = self.spark.sql("select * from  hudi_incremental")

        return df

    def __get_last_commit(self):
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_incremental order by commitTime asc").limit(
            50).collect()))
        last_commit = commits[len(commits) - 1]
        return last_commit

    def __run(self):
        """Check the metadata file"""
        flag = self.__check_meta_data_file()
        """if metadata files exists load the last commit and start inc loading from that commit """
        if flag:
            meta_data = json.loads(self.__read_meta_data())
            print(f"""
            ******************LOGS******************
            meta_data {meta_data}
            last_processed_commit : {meta_data.get("last_processed_commit")}
            ***************************************
            """)

            read_commit = str(meta_data.get("last_processed_commit"))
            df = self.__read_inc_data(commit_time=read_commit)

            """if there is no INC data then it return Empty DF """
            if not df.rdd.isEmpty():
                last_commit = self.__get_last_commit()
                self.__push_meta_data(json_data=json.dumps({
                    "last_processed_commit": last_commit,
                    "table_name": self.hudi_settings.table_name,
                    "path": self.hudi_settings.path,
                    "inserted_time": datetime.now().__str__(),

                }))
                return df
            else:
                return df

        else:

            """Metadata files does not exists meaning we need to create  metadata file on S3 and start reading from begining commit"""

            read_commit = self.__get_begin_commit()

            df = self.__read_inc_data(commit_time=read_commit)
            last_commit = self.__get_last_commit()

            self.__push_meta_data(json_data=json.dumps({
                "last_processed_commit": last_commit,
                "table_name": self.hudi_settings.table_name,
                "path": self.hudi_settings.path,
                "inserted_time": datetime.now().__str__(),

            }))

            return df

    def read(self):
        """
        reads INC data and return Spark Df
        :return:
        """

        return self.__run()


def get_spark_df_from_dynamodb_table(dynamodb_table):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table)
    response = table.scan()

    # Convert DynamoDB items to a list of dictionaries
    items = response['Items']

    # Create a Spark DataFrame from the list of dictionaries
    spark_df = spark.createDataFrame(items)
    return spark_df


def load_hudi_tables(loaders):
    """load Hudi tables """

    for items in loaders.get("source"):
        table_name = items.get("table_name")
        path = items.get("hudi_path")

        if items.get("type") == "FULL":
            spark.read.format("hudi").load(path).createOrReplaceTempView(table_name)

        if items.get("type") == "INC":
            helper = HUDIIncrementalReader(
                bucket=BUCKET_NAME,
                hudi_settings=HUDISettings(
                    table_name=table_name,
                    path=path
                ),
                spark_session=spark
            )
            spark_df = helper.read()
            spark_df.createOrReplaceTempView(table_name)

    if loaders.get("transform", None) is not None:
        query = loaders.get("transform", None).get("query")
        spark_df = spark.sql(query)
        return spark_df


class EmailTemplate(ABC):
    @abstractmethod
    def render(self, **kwargs):
        pass


class DownloadReportEmailTemplate(EmailTemplate):
    def __init__(self, url):
        self.url = url

    def render(self):
        html_content = """
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            background-color: #f2f2f2;
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 0;
        }}

        .container {{
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            background-color: white;
            border-radius: 5px;
            box-shadow: 0px 2px 5px rgba(0, 0, 0, 0.1);
        }}

        .button {{
            background-color: #0074cc;
            border: none;
            color: white;
            padding: 10px 20px;
            text-align: center;
            text-decoration: none;
            display: inline-block;
            font-size: 16px;
            border-radius: 5px;
            cursor: pointer;
            margin-top: 15px;
        }}
    </style>
</head>
<body>
<div class="container">
    <p>Hello,</p>
    <p>Thank you for using our service. Your requested report is ready for download.</p>
    <p>You can download the report by clicking the button below:</p>

    <!-- Blue Download Button -->
    <a href="{}" style="color: white;" class="button">Download Report</a>

    <p>If you have any questions or need further assistance, please don't hesitate to contact us.</p>
    <p>Best regards,</p>
    <p>DataTeam</p>
</div>
</body>
</html>
""".format(self.url)
        return html_content


class Email(object):
    def __init__(self,
                 sender_email: str,
                 recipient_email: str,
                 subject: str,
                 html_template: str
                 ):
        """

        :param sender_email:  'abc@abc.com'
        :param recipient_email: 'abc@abc.com, abcd@abcd.com'
        :param subject: test subject
        :param html_template:  '<h1>Hello</h1>'
        """
        self.subject = subject
        self.sender_email = sender_email
        self.recipient_email = recipient_email
        self.html_template = html_template

        self.ses = boto3.client('ses'

                                )

    def send(self):
        """
        Sends Emails
        :return: Bool
        """
        try:
            self.ses.send_email(Source=self.sender_email,
                                Destination={'ToAddresses': [
                                    self.recipient_email
                                ]
                                },
                                Message={
                                    'Subject': {'Data': self.subject},
                                    'Body': {'Html': {'Data': self.html_template}}
                                })

            return True
        except Exception as e:
            print("error ", e)
            return False


class DateUtils:
    @staticmethod
    def get_current_year():
        return datetime.now().year

    @staticmethod
    def get_current_month():
        return datetime.now().month

    @staticmethod
    def get_current_day():
        return datetime.now().day


def write_spark_df_into_s3_generate_pre_signed_url(spark_df, loaders):
    bucket = loaders.get("email").get("report_bucket")
    s3 = boto3.client('s3')

    csv_buffer = BytesIO()
    df = spark_df.toPandas()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3_output_path = f"reports/year={DateUtils.get_current_year()}/month={DateUtils.get_current_month()}/day={DateUtils.get_current_day()}/{uuid.uuid4().__str__()}.csv"
    s3.upload_fileobj(csv_buffer, bucket, s3_output_path)

    url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': bucket, 'Key': s3_output_path},
                                    ExpiresIn=loaders.get("email").get("file_expires_in"), )

    return url


def main():
    loaders = {
        "source": [
            {
                "table_name": "orders",
                "hudi_path": "s3://XXX/silver/table_name=orders",
                "type": "FULL"  # FULL | INC
            },
            {
                "table_name": "customers",
                "hudi_path": "s3://XXXX/silver/table_name=customers",
                "type": "FULL"  # FULL | INC
            }
        ],
        "transform": {
            "query": """SELECT o.*,
            c.name AS customer_name,
        c.email AS customer_email
            FROM orders AS o
            JOIN customers AS c ON o.customer_id = c.customer_id
    WHERE o.priority = 'URGENT' """
        },
        "email": {
            "sender_email": "shahsoumil519@gmail.com",
            "recipient_email": "shahsoumil519@gmail.com",
            "subject": "Download Link for Data",
            "report_bucket": "hudi-learn-demo-1995",
            "file_expires_in": 86400
        }

    }

    spark_df = load_hudi_tables(loaders=loaders)
    print(spark_df.show())

    url = write_spark_df_into_s3_generate_pre_signed_url(
        spark_df=spark_df, loaders=loaders
    )
    if url != '':
        html_template = DownloadReportEmailTemplate(url=url).render()
        helper_email = Email(
            sender_email=loaders.get("email").get("sender_email").__str__(),
            recipient_email=loaders.get("email").get("recipient_email").__str__(),
            subject=loaders.get("email").get("subject").__str__(),
            html_template=html_template
        )
        helper_email.send()


main()
