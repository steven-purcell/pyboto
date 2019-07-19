# pyboto
Wrapper for commonly used boto3 when working with data stored in AWS S3.

## Install

## Setup

Be sure to set up your AWS authentication credentials. You can do so by using the aws cli and running

pip install awscli
aws configure
More help on configuring the aws cli here https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

## Simple Usage
```
import pyboto

boto_obj = pyboto.Boto('us-east-1')
result = boto_obj.get_keys('myBucket', 'myKeyPrefix')
print(result)
```

## Full Usage
```
boto_obj.get_keys('myBucket', 'myKeyPrefix')

boto_obj.get_file('myBucket', 'myKey')

boto_obj.put_df('myBucket', 'myKey', pandas_dataframe, header: bool, index: bool, sep=',')

boto_obj.put_file(bucket='g2l-dev', key='tmp/test.csv', file_or_buffer=contents)
```

## Note