import json
import logging
import time
from typing import Dict
from typing import List
from typing import Set

import boto3
import neo4j
import cartography.intel.aws.jsonwrappers.json_utils as json_utils

from cartography.intel.aws.elasticache import get_elasticache_clusters
from cartography.stats import get_stats_client
from cartography.util import aws_handle_regions
from cartography.util import merge_module_sync_metadata
from cartography.util import timeit

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)


def _attach_ecluster_to_aws_account(es_cache_dict: Dict, cluster: Dict, aws_account_id: str, aws_update_tag: int) -> None:
    relationship_details = {
        'to_id': cluster['ARN'], 'from_id': aws_account_id,
        'to_label': 'ElasticacheCluster', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
    }

    json_utils.add_relationship(relationship_details, es_cache_dict)


def _attach_cluster_to_topic(es_cache_dict: Dict, cluster: Dict, update_tag: int) -> None:

    topic = cluster['NotificationConfiguration']

    relationship_details = {
        'to_id': cluster['ARN'], 'from_id': topic['TopicArn'],
        'to_label': 'ElasticacheCluster', 'from_label': 'ElasticacheTopic',
        'type': 'CACHE_CLUSTER'
    }

    json_utils.add_relationship(relationship_details, es_cache_dict)


def _attach_topic_to_aws_account(es_cache_dict: Dict, cluster: Dict, aws_account_id: str, update_tag: int) -> None:

    topic = cluster['NotificationConfiguration']

    relationship_details = {
        'to_id': topic['TopicArn'], 'from_id': aws_account_id,
        'to_label': 'ElasticacheTopic', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
    }

    json_utils.add_relationship(relationship_details, es_cache_dict)


def split_and_write_to_json(es_cache_dict: Dict, folder_path: str) -> None:
    entities = es_cache_dict['entities']

    es_clusters = []
    es_topics = []

    for _, entity in entities.items():
        l_list = entity['labels']
        if 'ElasticacheCluster' in l_list:
            es_clusters.append(entity)
        if 'ElasticacheTopic' in l_list:
            es_topics.append(entity)

    json_utils.write_to_json(es_clusters, f'{folder_path}/ElastiCacheClusters.json')
    json_utils.write_to_json(es_topics, f'{folder_path}/ElastiCacheTopics.json')

@timeit
def load_elasticache_clusters(
    neo4j_session: neo4j.Session, clusters: List[Dict], region: str,
    aws_account_id: str, update_tag: int, es_cache_dict: Dict) -> None:

    entities = es_cache_dict['entities']
    for cluster in clusters:
        entities[cluster['ARN']] = {
            'identity': cluster['ARN'],
            'labels': ['ElasticacheCluster'],
            'firstseen': int(time.time()),
            'lastupdated': update_tag,
            'Region': region
        }

        entities[cluster['ARN']].update(cluster)
        _attach_ecluster_to_aws_account(es_cache_dict, cluster, aws_account_id, update_tag)

        if cluster.get('NotificationConfiguration') is not None:
            if cluster['NotificationConfiguration'].get('TopicArn') is not None:
                entities[cluster['NotificationConfiguration']['TopicArn']] = {
                    'identity': cluster['NotificationConfiguration']['TopicArn'],
                    'labels': ['ElasticacheTopic'],
                    'firstseen': int(time.time()),
                    'lastupdated': update_tag,
                    'arn': cluster['NotificationConfiguration']['TopicArn'],
                    'status': cluster['NotificationConfiguration']['TopicStatus']
                }

            _attach_cluster_to_topic(es_cache_dict, cluster, update_tag)
            _attach_topic_to_aws_account(es_cache_dict, cluster, aws_account_id, update_tag)

    logger.info(f"Loading {len(clusters)} ElastiCache clusters for region '{region}' into json file.")



@timeit
def sync(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict) -> None:

    es_cache_dict: Dict = {
        'entities': {},
        'relationships': []
    }

    for region in regions:
        logger.info(f"Syncing ElastiCache clusters for region '{region}' in account {current_aws_account_id}")
        clusters = get_elasticache_clusters(boto3_session, region)
        load_elasticache_clusters(neo4j_session, clusters, region, current_aws_account_id, update_tag, es_cache_dict)

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
    remove_properties = {
        None: ['NotificationConfiguration', 'CacheParameterGroup', 'SecurityGroups', 'ConfigurationEndpoint']
    }

    json_utils.exclude_properties(es_cache_dict, remove_properties)
    json_utils.override_properties(es_cache_dict, properties={})

    # write relationships to file
    folder_path = f'{json_utils.out_directory}/jsonassets/{current_aws_account_id}/elasticache/'
    json_utils.create_folder(json_utils.out_directory, current_aws_account_id)

    json_utils.write_relationship_to_json(es_cache_dict, folder_path)
    es_cache_list = list(es_cache_dict['entities'].values())
    split_and_write_to_json(es_cache_dict, folder_path)
