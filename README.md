# pyboto
Wrapper for commonly used boto3 operations in S3, ECR, and others.

## Install

## Setup

Be sure to set up your AWS authentication credentials. You can do so by using the aws cli and running

pip install awscli
aws configure
More help on configuring the aws cli here https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

## Simple Usage
```
import pyboto

boto_object = pyboto.Boto('us-east-1')
result = boto_object.get_keys('myBucket', 'myKeyPrefix')
print(result)
```

## Full Usage
```
pyboto.get_keys('myBucket', 'myKeyPrefix')

pyboto.get_csv('myBucket', 'myKey')

pyboto.put_csv('myBucket', 'myKey', pandas_dataframe, header: bool, index: bool)
```

## Note