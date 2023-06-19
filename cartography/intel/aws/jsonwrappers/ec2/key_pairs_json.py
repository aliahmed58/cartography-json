import json
import logging
import os
import pprint
from typing import Dict
from typing import List

import boto3
import neo4j

import cartography.intel.aws.jsonwrappers.json_utils as json_utils
from cartography.intel.aws.ec2.key_pairs import get_ec2_key_pairs
from cartography.util import timeit

logger = logging.getLogger(__name__)



@timeit
def load_ec2_key_pairs(
    neo4j_session: neo4j.Session, data: List[Dict], region: str, current_aws_account_id: str,
    update_tag: int, key_pair: Dict) -> None:

    entities = key_pair['entities']
    for key_pair in data:
        key_name = key_pair["KeyName"]
        key_fingerprint = key_pair.get("KeyFingerprint")
        key_pair_arn = f'arn:aws:ec2:{region}:{current_aws_account_id}:key-pair/{key_name}'

        if entities.get(key_pair_arn):
            entities[key_pair_arn].update({
                'KeyName': key_name,
                'KeyFingerPrint': key_fingerprint,
                'lastupdated': update_tag
            })

        # attach to AWSAccount:RESOURCE if not exists
        relationships = key_pair['relationships']
        exists: bool = False
        for relationship in relationships:
            if relationship['to_id'] == key_pair_arn and relationships['from_id'] == current_aws_account_id:
                exists = True
                break

        if not exists:
            relationship_details = {
                'to_id': key_pair_arn, 'from_id': current_aws_account_id,
                'to_label': 'EC2KeyPair', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
            }
            json_utils.add_relationship(relationship_details, key_pair, update_tag)


@timeit
def sync_ec2_key_pairs_json(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict,) -> None:

    key_pair: Dict = {
        'entities': {},
        'relationship': []
    }

    # check if ec2_key_pairs.json already exists or not and load from file
    folder_path = f'{json_utils.out_directory}{current_aws_account_id}/ec2'
    files = os.listdir(folder_path)
    if 'ec2_key_pairs.json' in files:
        with open(f'{folder_path}/ec2_key_pairs.json', 'r') as f:
            entities = json.loads(f.read())
            for entity in entities['entities']:
                key_pair['entities'][entity['identity']] = entity
        f.close()
    if 'relationships.json' in files:
        with open(f'{folder_path}/relationships.json', 'r') as r:
            relationships = json.loads(r.read())
            key_pair['relationship'] = relationships

    for region in regions:
        logger.info("Syncing EC2 key pairs for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_ec2_key_pairs(boto3_session, region)
        load_ec2_key_pairs(neo4j_session, data, region, current_aws_account_id, update_tag, key_pair)
