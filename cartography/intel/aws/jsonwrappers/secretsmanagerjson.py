import logging
import time
from typing import Dict
from typing import List

import boto3
import neo4j

import cartography.intel.aws.jsonwrappers.json_utils as json_utils

from cartography.util import aws_handle_regions
from cartography.util import dict_date_to_epoch
from cartography.util import run_cleanup_job
from cartography.util import timeit
from cartography.intel.aws.secretsmanager import get_secret_list

logger = logging.getLogger(__name__)

@timeit
def load_secrets(
    neo4j_session: neo4j.Session, data: List[Dict], region: str, current_aws_account_id: str,
    aws_update_tag: int, secrets_dict) -> None:

    entities = secrets_dict['entities']
    for secret in data:
        entities[secret['ARN']] = {
            'identity': secret['ARN'],
            'labels': ['SecretsManagerSecret'],
            'firstseen': int(time.time()),
            'lastupdate': aws_update_tag,
            'id': secret['ARN'],
            'Region': region
        }

        entities[secret['ARN']].update(secret)

        relationship_details = {
            'to_id': secret['ARN'], 'from_id': current_aws_account_id,
            'to_label': 'SecretsManagerSecret', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }

        json_utils.add_relationship(relationship_details, secrets_dict)
@timeit
def sync(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict) -> None:

    secrets_dict: Dict = {
        'entities': {},
        'relationships': []
    }

    for region in regions:
        logger.info("Syncing Secrets Manager for region '%s' in account '%s'.", region, current_aws_account_id)
        secrets = get_secret_list(boto3_session, region)
        load_secrets(neo4j_session, secrets, region, current_aws_account_id, update_tag, secrets_dict)

    """
    Any properties that need to be overriden or excluded should be done below
    exclude_properties = {
        None: ['SomeProperty', ...], # For all entities regardless of label
        'ElasticacheCluster': [...] # label specific properties
    }
    override_properties = {
        None: { # for all entities regardless of properties
            'property_key': 'property_value'
        },
        'ElasticacheCluster': {
            'property_key': 'value' # label specific overriding
        }
    }
    """
    json_utils.override_properties(secrets_dict, properties={})
    remove_properties = {
        None: ['RotationRules', 'SecretVersionsToStages']
    }
    json_utils.exclude_properties(secrets_dict, remove_properties)

    json_utils.create_folder(json_utils.out_directory, current_aws_account_id)
    folder_path = json_utils.get_out_folder_path('secretsmanager', current_aws_account_id)

    json_utils.write_relationship_to_json(secrets_dict, folder_path)

    secrets_list = list(secrets_dict['entities'].values())
    json_utils.write_to_json(secrets_list, f'{folder_path}/secretsmanager.json')