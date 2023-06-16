import logging
import time
import os
from typing import Dict
from typing import List

import boto3
import neo4j

from cartography.intel.aws.redshift import get_redshift_cluster_data, transform_redshift_cluster_data
from cartography.intel.aws.jsonwrappers.service_enum import AWSServices

import cartography.intel.aws.jsonwrappers.json_utils as json_utils
from cartography.util import timeit

logger = logging.getLogger(__name__)
json_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

@timeit
def _attach_ec2_security_groups(neo4j_session: neo4j.Session, cluster: Dict,
                                aws_update_tag: int, redshift_dict: dict) -> None:
    for group in cluster.get('VpcSecurityGroups', []):
        relationship_details = {
            'to_id': group['VpcSecurityGroupId'], 'from_id': cluster['arn'],
            'to_label': 'EC2SecurityGroup', 'from_label': 'RedshiftCluster',
            'type': 'MEMBER_OF_EC2_SECURITY_GROUP'
        }

        json_utils.add_relationship(relationship_details, redshift_dict, aws_update_tag)

@timeit
def _attach_iam_roles(neo4j_session: neo4j.Session, cluster: Dict,
                      aws_update_tag: int, redshift_dict: dict) -> None:
    for role in cluster.get('IamRoles', []):
        relationship_details = {
            'to_id': role['IamRoleArn'], 'from_id': cluster['arn'],
            'to_label': 'AWSPrincipal', 'from_label': 'RedshiftCluster', 'type': 'STS_ASSUMEROLE_ALLOW'
        }

        json_utils.add_relationship(relationship_details, redshift_dict, aws_update_tag)

@timeit
def _attach_aws_vpc(neo4j_session: neo4j.Session, cluster: Dict, aws_update_tag: int, redshift_dict: dict) -> None:
    if cluster.get('VpcId'):
        relationship_details = {
            'to_id': cluster['VpcId'], 'from_id': cluster['arn'],
            'to_label': 'AWSVpc', 'from_label': 'RedshiftCluster', 'type': 'MEMBER_OF_AWS_VPC '
        }
        json_utils.add_relationship(relationship_details, redshift_dict, aws_update_tag)


@timeit
def load_redshift_cluster_data(
        neo4j_session: neo4j.Session, clusters: List[Dict], region: str,
        current_aws_account_id: str, aws_update_tag: int, redshift_dict: dict) -> None:

    entities = redshift_dict['entities']
    for cluster in clusters:
        entities[cluster['arn']] = {
            'identity': cluster['arn'],
            'labels': ['RedshiftCluster'],
            'Region': region,
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag
        }

        entities[cluster['arn']].update(cluster)

        # add relationship with AWSAccount
        relationship_details = {
            'to_id': cluster['arn'], 'from_id': current_aws_account_id,
            'to_label': 'RedshiftCluster', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(relationship_details, redshift_dict, aws_update_tag)

        _attach_ec2_security_groups(neo4j_session, cluster, aws_update_tag, redshift_dict)
        _attach_iam_roles(neo4j_session, cluster, aws_update_tag, redshift_dict)
        _attach_aws_vpc(neo4j_session, cluster, aws_update_tag, redshift_dict)

@timeit
def sync_redshift_clusters(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str,
        current_aws_account_id: str, aws_update_tag: int, redshift_dict: dict) -> None:
    data = get_redshift_cluster_data(boto3_session, region)
    transform_redshift_cluster_data(data, region, current_aws_account_id)
    load_redshift_cluster_data(neo4j_session, data, region, current_aws_account_id, aws_update_tag, redshift_dict)


@timeit
def sync(neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
         current_aws_account_id: str, update_tag: int, common_job_parameters: Dict) -> None:

    redshift_dict = {
        'entities': {},
        'relationships': []
    }

    for region in regions:
        logger.info("Syncing Redshift clusters for region '%s' in account '%s'.", region, current_aws_account_id)
        sync_redshift_clusters(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, redshift_dict)

    """
    Any properties to be omitted or overriden should be done in the following dictionaries
    Note: None key value means for all instead of a specific label
    override_properties = {
        # for all
        None: {
            'arn': 'changed'
        }
        # for label specific
        'RedshiftCluster': {
            'db_name': 'something'
        }
    }
    remove_properties = {
        # for all
        None: ['db_name', 'arn', ...]
        # for label specific
        'RedshiftCluster': ['encrypted', ...]
    }
    """
    excluded_props = {
        None: [
            'IamRoles', 'AquaConfiguration', 'ClusterNodes', 'VpcSecurityGroups', 'ClusterParameterGroups'
        ]
    }
    json_utils.exclude_properties(redshift_dict, excluded_props)
    json_utils.override_properties(redshift_dict, properties={})

    # write to json files
    json_utils.create_folder(AWSServices.REDSHIFT.value, current_aws_account_id)

    json_utils.write_relationship_to_json(redshift_dict, AWSServices.REDSHIFT.value, current_aws_account_id)
    redshift_list = list(redshift_dict['entities'].values())
    json_utils.write_to_json(redshift_list, 'redshift.json', AWSServices.REDSHIFT.value, current_aws_account_id)

