from enum import Enum


class AWSServices(Enum):
    S3 = 's3'
    DYNAMO_DB = 'dynamodb'
    ECR = 'ecr'
    EMR = 'emr'
    ELASTICACHE = 'elasticache'
    ELASTIC_SEARCH = 'elasticsearch'
    IAM = 'iam'
    RDS = 'rds'
    REDSHIFT = 'redshift'
    SECRETS_MANAGER = 'secretsmanager'
    EC2_INSTANCES = 'ec2:instances'
    EC2_KEYPAIR = 'ec2:keypair'
