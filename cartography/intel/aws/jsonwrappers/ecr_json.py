import json
import logging
import time
from typing import Dict
from typing import List

import boto3
import neo4j
import cartography.intel.aws.jsonwrappers.json_utils as json_utils

from cartography.intel.aws.ecr import get_ecr_repositories, get_ecr_repository_images, transform_ecr_repository_images
from cartography.util import batch
from cartography.util import timeit

logger = logging.getLogger(__name__)

SERVICE_NAME = 'ecr'


@timeit
def load_ecr_repositories(
        neo4j_session: neo4j.Session, repos: List[Dict], region: str, current_aws_account_id: str,
        aws_update_tag: int, ecr_dict: Dict) -> None:
    entities = ecr_dict['entities']
    for repo in repos:
        repo_id = repo['repositoryArn']

        entities[repo_id] = {
            'identity': repo_id,
            'labels': ['ECRRepository'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
            'Region': region,
        }
        entities[repo_id].update(repo)

        # add relationship with AWS account
        relationship_details = {
            'to_id': repo_id, 'from_id': current_aws_account_id, 'to_label': 'ECRRepository',
            'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(relationship_details, ecr_dict, aws_update_tag)


def _attach_image_to_repo(image: Dict, image_repo_id, ecr_dict: Dict, aws_update_tag: int) -> None:
    relationship_details = {
        'to_id': image['imageDigest'], 'from_id': image_repo_id,
        'to_label': 'ECRImage', 'from_label': 'ECRRepositoryImage', 'type': 'IMAGE'
    }
    json_utils.add_relationship(relationship_details, ecr_dict, aws_update_tag)


def _attach_image_repo_to_repo(image: Dict, image_repo_id, ecr_dict: Dict, aws_update_tag: int) -> None:
    relationship_details = {
        'to_id': image_repo_id, 'from_id': image['repo_uri'],
        'to_label': 'ECRRepositoryImage', 'from_label': 'ECRRepository', 'type': 'REPO_IMAGE'
    }

    json_utils.add_relationship(relationship_details, ecr_dict, aws_update_tag)


def _load_ecr_repo_img(repo_images_list: List[Dict], aws_update_tag: int,
                       region: str, ecr_dict: Dict) -> None:

    entities: Dict = ecr_dict['entities']
    for image in repo_images_list:
        image_id: str = f'{image["repo_uri"]}:{image["imageTag"]}' if image.get('imageTag') else f'{image["repo_uri"]}'

        # add repository image node
        entities[image_id] = {
            'identity': image_id,
            'labels': ['ECRRepositoryImage'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
            'ImageTag': image['imageTag'],
            'uri': image_id
        }

        # add ECRImage node
        entities[image['imageDigest']] = {
            'identity': image['imageDigest'],
            'labels': ['ECRImage'],
            'imageDigest': image['imageDigest'],
            'Region': region,
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
        }

        _attach_image_to_repo(image, image_id, ecr_dict, aws_update_tag)
        _attach_image_repo_to_repo(image, image_id, ecr_dict, aws_update_tag)


@timeit
def load_ecr_repository_images(
        neo4j_session: neo4j.Session, repo_images_list: List[Dict], region: str,
        aws_update_tag: int, ecr_dict: Dict) -> None:
    logger.info(f"Loading {len(repo_images_list)} ECR repository images in {region} into graph.")
    for repo_image_batch in batch(repo_images_list, size=10000):
        _load_ecr_repo_img(repo_image_batch, aws_update_tag, region, ecr_dict)


def split_and_write_to_json(ecr_dict: Dict, aws_acc_id: str) -> None:
    entities: Dict = ecr_dict['entities']

    ecr_repos: List = []
    ecr_images_and_image_repos: List = []
    for _, entity in entities.items():
        l_list = entity['labels']
        if 'ECRRepository' in l_list:
            ecr_repos.append(entity)
        if 'ECRImage' in l_list or 'ECRRepositoryImage' in l_list:
            ecr_images_and_image_repos.append(entity)

    json_utils.write_to_json(ecr_repos, 'ecr_repos.json', 'ecr', aws_acc_id)
    json_utils.write_to_json(ecr_images_and_image_repos, 'ecr_image_repos.json', 'ecr', aws_acc_id)


@timeit
def sync(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict) -> None:
    ecr_dict: Dict = {
        'entities': {},
        'relationships': []
    }

    for region in regions:
        logger.info("Syncing ECR for region '%s' in account '%s'.", region, current_aws_account_id)
        image_data = {}
        repositories = get_ecr_repositories(boto3_session, region)
        for repo in repositories:
            repo_image_obj = get_ecr_repository_images(boto3_session, region, repo['repositoryName'])
            image_data[repo['repositoryUri']] = repo_image_obj
        load_ecr_repositories(neo4j_session, repositories, region, current_aws_account_id, update_tag, ecr_dict)
        repo_images_list = transform_ecr_repository_images(image_data)
        load_ecr_repository_images(neo4j_session, repo_images_list, region, update_tag, ecr_dict)

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
    json_utils.override_properties(ecr_dict, properties={})
    excluded_properties = {
        'ECRRepository': ['imageScanningConfiguration', 'encryptionConfiguration']
    }
    json_utils.exclude_properties(ecr_dict, excluded_properties)

    # create folders
    json_utils.create_folder(SERVICE_NAME, current_aws_account_id)

    # write relationships to json
    json_utils.write_relationship_to_json(ecr_dict, SERVICE_NAME, current_aws_account_id)

    # write nodes to json
    split_and_write_to_json(ecr_dict, current_aws_account_id)
