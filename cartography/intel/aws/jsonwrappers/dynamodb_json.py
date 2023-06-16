import datetime
import logging
import time
import os
import json
from typing import Dict
from typing import List
from typing import Any

import boto3
import neo4j

import cartography.intel.aws.jsonwrappers.json_utils as json_utils

from cartography.intel.aws.dynamodb import get_dynamodb_tables, transform_dynamodb_tables

from cartography.stats import get_stats_client
from cartography.util import dict_value_to_str
from cartography.util import timeit

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)


def _attach_gsi_to_aws_account(gsi: Dict[str, Any], aws_account_id: str, dynamo_dict: dict, update_tag: int) -> None:
    relationship_details = {
        'to_id': gsi['Arn'], 'from_id': aws_account_id,
        'to_label': 'DynamoDBGlobalSecondaryIndex', 'from_label': 'AWSAccount',
        'type': 'RESOURCE'
    }

    json_utils.add_relationship(relationship_details, dynamo_dict, update_tag)


def _attach_gsi_to_dynamo_table(gsi: Dict[str, Any], dynamo_dict: dict, update_tag: int) -> None:
    relationship_details = {
        'to_id': gsi['Arn'], 'from_id': gsi['TableArn'],
        'to_label': 'DynamoDBGlobalSecondaryIndex', 'from_label': 'DynamoDBTable',
        'type': 'GLOBAL_SECONDARY_INDEX'
    }

    json_utils.add_relationship(relationship_details, dynamo_dict, update_tag)

@timeit
def load_dynamodb_gsi(
        neo4j_session: neo4j.Session, gsi_data: List[Dict[str, Any]], region: str, current_aws_account_id: str,
        aws_update_tag: int, dynamo_dict: dict) -> None:

    entities = dynamo_dict['entities']
    for gsi in gsi_data:
        entities[gsi['Arn']] = {
            'identity': gsi['Arn'],
            'labels': ['DynamoDBGlobalSecondaryIndex'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
            'Region': region,
            'id': gsi['Arn'],
        }

        entities[gsi['Arn']].update(gsi)

        _attach_gsi_to_aws_account(gsi, current_aws_account_id, dynamo_dict, aws_update_tag)
        _attach_gsi_to_dynamo_table(gsi, dynamo_dict, aws_update_tag)




@timeit
def load_dynamodb_tables(
        neo4j_session: neo4j.Session, tables_data: List[Dict[str, Any]], region: str, current_aws_account_id: str,
        aws_update_tag: int, dynamo_dict: dict) -> None:

    entities = dynamo_dict['entities']
    for table in tables_data:
        entities[table['Arn']] = {
            'identity': table['Arn'],
            'labels': ['DynamoDBTable'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
            'region': region,
            'id': table['Arn']
        }

        entities[table['Arn']].update(table)

        relationship_details = {
            'to_id': table['Arn'], 'from_id': current_aws_account_id,
            'to_label': 'DynamoDBTable', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }

        json_utils.add_relationship(relationship_details, dynamo_dict, aws_update_tag)


@timeit
def sync_dynamodb_tables(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str, aws_update_tag: int, common_job_parameters: Dict, dynamo_dict: dict) -> None:
    for region in regions:
        logger.info("Syncing DynamoDB for region in '%s' in account '%s'.", region, current_aws_account_id)
        dynamodb_tables = get_dynamodb_tables(boto3_session, region)
        ddb_table_data, ddb_gsi_data = transform_dynamodb_tables(dynamodb_tables, region)
        load_dynamodb_tables(neo4j_session, ddb_table_data, region, current_aws_account_id, aws_update_tag, dynamo_dict)
        load_dynamodb_gsi(neo4j_session, ddb_gsi_data, region, current_aws_account_id, aws_update_tag, dynamo_dict)


@timeit
def sync(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict) -> None:

    dynamo_dict = {
        'entities': {},
        'relationships': []
    }

    sync_dynamodb_tables(
        neo4j_session, boto3_session, regions, current_aws_account_id, update_tag, common_job_parameters, dynamo_dict
    )

    """
    To exclude or override any properties the following methods should be used with the dictionaries provided
    as function parameters in the given format
    remove_properties = {
        # use None as key to remove throughout the entities. for all.
        None: ['SomeProperty', 'SomeOtherProperty', ...]
        # use labels as keys to remove property only specific to that label
        'DynamoDBTable': ['size', ...]
    }
    override_properties = {
        # use None for all 
        None: {
            'size': 23
        }
        # use label as dict key for specific labels
        'DynamoDBTable': {
            'region': 'us-east-2' 
        }
    }
    """
    json_utils.override_properties(dynamo_dict, properties={})
    json_utils.exclude_properties(dynamo_dict, properties={})
    json_utils.create_folder('dynamodb', current_aws_account_id)

    # write relationships to json
    json_utils.write_relationship_to_json(dynamo_dict, 'dynamodb', current_aws_account_id)

    # write nodes to json
    dynamo_list: list[dict] = list(dynamo_dict['entities'].values())
    json_utils.write_to_json(dynamo_list, 'dynamodb.json', 'dynamodb', current_aws_account_id)
