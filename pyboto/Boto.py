import boto3
from io import StringIO, BytesIO
import pandas as pd

class Boto:

    def __init__(self, region='us-east-1'):
        self.__region = region


# ##########################################################################
    def put_csv(self, bucket: str, key: str, dataframe, header: bool, index: bool):
        buffer = StringIO()  # creating an empty buffer
        dataframe.to_csv(buffer, sep=',', header=header, index=index)  # filling the buffer
        buffer.seek(0)  # set to the start of the stream
        buffer = BytesIO(buffer.getvalue().encode())

        s3_client = boto3.client('s3')
        result = s3_client.put_object(
            Body=buffer,
            Bucket=bucket,
            Key=key,
        )
        metadata = result['ResponseMetadata']
        key_check = self.list_keys(bucket, key)[0]
        response = {'HTTPStatusCode':str(metadata['HTTPStatusCode']),
                    'Key': key_check['Key'],
                    'LastModified': key_check['LastModified']
                    }
        return response


# ##########################################################################
    def get_csv(self, bucket: str, key: str):
        s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(BytesIO(obj['Body'].read()), names=None, skiprows=None, header='infer')

        return df

# ##########################################################################
    def get_keys(self, bucket: str, prefix: str):

        s3_client = boto3.client('s3')
        result = s3_client.list_objects(
            Bucket=bucket,
            Delimiter=',',
            MaxKeys=1000,
            Prefix=prefix
        )
        object_list = []
        for obj in result['Contents']:
            obj_dict = {'Key': obj['Key'], 'LastModified': str(obj['LastModified'])}
            object_list.append(obj_dict)
        return object_list


# ##########################################################################