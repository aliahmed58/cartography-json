import datetime
import hashlib
import logging
import json
from typing import Dict
from typing import List

import boto3
import neo4j
from cartography.intel.aws.jsonwrappers.json_utils import add_relationship, default_json_serializer, create_folder
from cartography.stats import get_stats_client
from cartography.util import timeit
from cartography.intel.aws.s3 import get_s3_bucket_list, parse_acl, parse_policy, parse_policy_statements, \
    parse_encryption, parse_versioning, parse_public_access_block, get_s3_bucket_details

import os

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)

json_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))


@timeit
def _append_s3_configs(configs: List[Dict], update_tag: int, s3_dict: dict) -> None:
    """
    :param configs: the config List[Dict] that needs to be added to S3Bucket properties
    :param s3_dict: the s3_dict that holds the entities and relationships
    :return: None, modifies the s3_dict with the appended details
    """
    entities = s3_dict['entities']
    for config in configs:
        entities[config['bucket']]['properties'].update(config)
        # every config contains a bucket key that should not be present in S3Bucket properties hence deleted
        del entities[config['bucket']]['properties']['bucket']
        # updating the last updated in properties
        entities[config['bucket']]['properties']['lastupdate'] = update_tag


@timeit
def _load_s3_buckets(bucket_data, aws_account_id: int, aws_update_tag: int, s3_dict: dict) -> None:
    entities = s3_dict['entities']

    for bucket in bucket_data['Buckets']:
        for key, value in bucket.items():
            if isinstance(value, datetime.datetime):
                bucket[key] = str(value)

        entities[bucket['Name']] = {
            'identity': bucket['Name'],
            'label': [
                'S3Bucket'
            ],
            'properties': bucket

        }
        entities[bucket['Name']]['properties'].update({'lastupdated': aws_update_tag})

        relationship_details = {
            'type': 'RESOURCE',
            'to_id': aws_account_id,
            'from_id': bucket['Name'],
            'from_label': 'S3Bucket',
            'to_label': 'AWSAccount'
        }

        add_relationship(relationship_details, s3_dict)


def _load_s3_acls(acls: List[Dict], update_tag: int, s3_dict: dict) -> None:
    entities = s3_dict['entities']

    for acl in acls:
        entities[acl['id']] = {
            'identity': acl['id'],
            'label': ['S3Acl'],
            'properties': acl,
        }

        entities[acl['id']]['properties'].update({
            'firstseen': str(datetime.datetime.now()),
            'lastupdated': update_tag
        })

        relationship_details = {
            'type': 'APPLIES_TO',
            'to_id': acl['bucket'],
            'from_id': acl['id'],
            'to_label': 'S3Bucket',
            'from_label': 'S3Acl'
        }

        add_relationship(relationship_details, s3_dict)


def _load_s3_policy_statements(statements: List[Dict], update_tag: int, s3_dict: dict) -> None:
    entities = s3_dict['entities']

    for statement in statements:
        statement_id = hashlib.md5(statement['statement_id'].encode()).hexdigest()
        entities[statement_id] = {
            'labels': ['S3PolicyStatement'],
            'identity': statement_id,
            'properties': statement
        }

        entities[statement_id]['properties'].update({
            'firstseen': str(datetime.datetime.now()),
            'lastseen': update_tag
        })

        relationship_details = {
            'to_label': 'S3PolicyStatement',
            'from_label': 'S3Bucket',
            'to_id': statement_id,
            'from_id': statement['bucket'],
            'type': 'POLICY_STATEMENT'
        }

        add_relationship(relationship_details, s3_dict)


def _load_s3_policies(policies: List[Dict], update_tag: int, s3_dict: dict) -> None:
    _append_s3_configs(policies, update_tag, s3_dict)


def _load_s3_encryption(encryption_configs: List[Dict], update_tag: int, s3_dict: dict) -> None:
    _append_s3_configs(encryption_configs, update_tag, s3_dict)


def _load_s3_versioning(versioning_configs: List[Dict], update_tag: int, s3_dict: dict) -> None:
    _append_s3_configs(versioning_configs, update_tag, s3_dict)


def _load_public_access_block(public_access_block_configs: List[Dict], update_tag: int, s3_dict: dict) -> None:
    _append_s3_configs(public_access_block_configs, update_tag, s3_dict)


def _set_default_values(aws_account_id: str, s3_dict: dict) -> None:
    """
    :param aws_account_id: AWSAccount id
    :param s3_dict: the s3_dict that contains all the entities and relationships
    :return: None
    """
    # get all the S3Buckets linked with the aws_account_id
    relationships = s3_dict['relationships']
    entities = s3_dict['entities']
    for connection in relationships:
        if connection['type'] == 'RESOURCE':
            bucket_name = connection['from_id']
            if entities[bucket_name]['properties'].get('anonymous_actions') is None:
                entities[bucket_name]['properties']['anonymous_actions'] = []
                entities[bucket_name]['properties']['anonymous_access'] = 'false'

            if entities[bucket_name]['properties'].get('default_encryption') is None:
                entities[bucket_name]['properties']['default_encryption'] = 'false'


def write_to_json(s3_dict: dict, folder_path: str, aws_account_id: str) -> None:
    try:
        # save entities to json
        entities = s3_dict['entities']
        list_of_nodes = list(entities.values())
        nodes_json_dump = json.dumps(list_of_nodes, default=default_json_serializer, indent=4)
        with open(f'{folder_path}/{aws_account_id}_nodes.json', 'w+') as nodes_file:
            nodes_file.write(nodes_json_dump)
        nodes_file.close()

        relationships = s3_dict['relationships']
        relationships_json = json.dumps(relationships, default=default_json_serializer, indent=4)
        with open(f'{folder_path}/{aws_account_id}_relationships.json', 'w+') as relationship_file:
            relationship_file.write(relationships_json)
        relationship_file.close()

    except Exception as e:
        logger.warning("Error occurred while saving to JSON")


@timeit
def load_s3_details(bucket_data, s3_details_iter, aws_account_id, aws_update_tag) -> None:
    acls: List[Dict] = []
    policies: List[Dict] = []
    statements = []
    encryption_configs: List[Dict] = []
    versioning_configs: List[Dict] = []
    public_access_block_configs: List[Dict] = []
    for bucket, acl, policy, encryption, versioning, public_access_block in s3_details_iter:
        parsed_acls = parse_acl(acl, bucket, aws_account_id)
        if parsed_acls is not None:
            acls.extend(parsed_acls)
        parsed_policy = parse_policy(bucket, policy)
        if parsed_policy is not None:
            policies.append(parsed_policy)
        parsed_statements = parse_policy_statements(bucket, policy)
        if parsed_statements is not None:
            statements.extend(parsed_statements)
        parsed_encryption = parse_encryption(bucket, encryption)
        if parsed_encryption is not None:
            encryption_configs.append(parsed_encryption)
        parsed_versioning = parse_versioning(bucket, versioning)
        if parsed_versioning is not None:
            versioning_configs.append(parsed_versioning)
        parsed_public_access_block = parse_public_access_block(bucket, public_access_block)
        if parsed_public_access_block is not None:
            public_access_block_configs.append(parsed_public_access_block)

    # list of relationships and nested entity dictionaries
    s3_dict = {
        'relationships': [],
        'entities': {}
    }

    # Load all the S3 data and insert into s3_dict
    _load_s3_buckets(bucket_data, aws_account_id, aws_update_tag, s3_dict)
    _load_s3_acls(acls, aws_update_tag, s3_dict)
    _load_s3_policy_statements(statements, aws_update_tag, s3_dict)
    _load_s3_policies(policies, aws_update_tag, s3_dict)
    _load_s3_encryption(encryption_configs, aws_update_tag, s3_dict)
    _load_s3_versioning(versioning_configs, aws_update_tag, s3_dict)
    _load_public_access_block(public_access_block_configs, aws_update_tag, s3_dict)
    _set_default_values(aws_account_id, s3_dict)

    create_folder(json_directory)

    logger.warning(json_directory)

    write_to_json(s3_dict, f'{json_directory}/jsonassets/', aws_account_id)


@timeit
def sync(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing S3 for account '%s'.", current_aws_account_id)
    bucket_data = get_s3_bucket_list(boto3_session)
    acl_and_policy_data_iter = get_s3_bucket_details(boto3_session, bucket_data)
    load_s3_details(bucket_data, acl_and_policy_data_iter, current_aws_account_id, update_tag)

    logger.info("S3 sync completed for account '%s'.", current_aws_account_id)
