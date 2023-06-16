import logging
import time
from typing import Any
from typing import Dict
from typing import List

import boto3
import botocore.exceptions
import neo4j
import cartography.intel.aws.jsonwrappers.json_utils as json_utils
from cartography.intel.aws.jsonwrappers.service_enum import AWSServices

from cartography.intel.aws.emr import get_emr_clusters, get_emr_describe_cluster
from cartography.util import timeit

logger = logging.getLogger(__name__)

# EMR API is subject to aggressive throttling, so we need to sleep a second between each call.
LIST_SLEEP = 1
DESCRIBE_SLEEP = 1


@timeit
def load_emr_clusters(
        neo4j_session: neo4j.Session, cluster_data: List[Dict[str, Any]],
        region: str, current_aws_account_id: str, aws_update_tag: int, emr_dict: Dict) -> None:
    logger.info(f"Loading EMR {len(cluster_data)} clusters for region '{region}' into graph.")

    entities = emr_dict['entities']
    for cluster in cluster_data:

        cluster_id = cluster['Id']

        entities[cluster_id] = {
            'identity': cluster_id,
            'labels': ['EMRCluster'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
        }

        entities[cluster_id].update(cluster)

        relationship_details = {
            'to_id': cluster_id, 'from_id': current_aws_account_id,
            'to_label': 'EMRCluster', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }

        json_utils.add_relationship(relationship_details, emr_dict, aws_update_tag)




@timeit
def sync(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str, update_tag: int, common_job_parameters: Dict[str, Any]) -> None:
    emr_dict: Dict = {
        'entities': {},
        'relationships': []
    }

    for region in regions:
        logger.info(f"Syncing EMR for region '{region}' in account '{current_aws_account_id}'.")

        clusters = get_emr_clusters(boto3_session, region)

        cluster_data: List[Dict[str, Any]] = []
        for cluster in clusters:
            cluster_id = cluster['Id']
            cluster_details = get_emr_describe_cluster(boto3_session, region, cluster_id)
            if cluster_details:
                cluster_data.append(cluster_details)
            time.sleep(DESCRIBE_SLEEP)

        load_emr_clusters(neo4j_session, cluster_data, region, current_aws_account_id, update_tag, emr_dict)

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
    json_utils.override_properties(emr_dict, properties={})
    excluded_properties = {
        None: ['Status', 'Ec2InstanceAttributes', 'Applications', 'Tags', 'Configurations']
    }
    json_utils.exclude_properties(emr_dict, excluded_properties)

    # create folders if not exist
    json_utils.create_folder('emr', current_aws_account_id)

    # write relationships to json file
    json_utils.write_relationship_to_json(emr_dict, AWSServices.EMR.value, current_aws_account_id)

    # write entities to json file
    emr_entities_list = list(emr_dict['entities'].values())
    json_utils.write_to_json(emr_entities_list, 'emr.json', AWSServices.EMR.value, current_aws_account_id)
