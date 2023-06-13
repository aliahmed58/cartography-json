import datetime
import enum
import json
import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import boto3
import neo4j

from cartography.intel.aws.permission_relationships import parse_statement_node
from cartography.intel.aws.permission_relationships import principal_allowed_on_resource
from cartography.stats import get_stats_client
from cartography.util import merge_module_sync_metadata
from cartography.util import run_cleanup_job
from cartography.util import timeit
from cartography.intel.aws.iam import get_user_list_data, get_user_policy_data, transform_policy_data, \
    transform_policy_id, get_policy_name_from_arn
from cartography.intel.aws.jsonwrappers.json_utils import add_relationship

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)


class PolicyType(enum.Enum):
    managed = 'managed'
    inline = 'inline'


@timeit
def load_users(users: List[Dict], current_aws_account_id: str, aws_update_tag: int, iam_dict: dict) -> None:
    # users are uniquely identified by their ARN throughout
    entities = iam_dict['entities']
    for user in users:
        for key, value in user.items():
            if isinstance(value, datetime.datetime):
                user[key] = str(value)

        # set default value for password last used
        user.setdefault('PasswordLastUsed', "")

        if entities.get(user['Arn']) is None:
            entities[user['Arn']] = {
                'identity': user['Arn'],
                'labels': [
                    'AWSUser'
                ],
                'properties': user
            }
            entities[user['Arn']]['properties'].update({'lastupdated': aws_update_tag})

            # add relationship with the AWSAccount
            relationship_details = {
                'to_id': current_aws_account_id,
                'from_id': user['Arn'],
                'to_label': 'AWSAccount',
                'from_label': 'AWSUser',
                'type': 'RESOURCE'
            }

            add_relationship(relationship_details, iam_dict)


@timeit
def load_policy(policy_id: str, policy_name: str, policy_type: str, principal_arn: str, aws_update_tag: int,
                iam_dict: dict) -> None:
    entities = iam_dict['entities']
    entities[policy_id] = {
        'identity': policy_id,
        'labels': ['AWSPolicy'],
        'properties': {
            'firstseen': int(str(datetime.datetime.now()))


        }
    }


@timeit
def load_policy_statements(policy_id: str, policy_name: str, statements: Any, aws_update_tag: int,
                           iam_dict: dict) -> None:
    pass

def load_policy_data(principal_policy_map: Dict[str, Dict[str, Any]],
                     policy_type: str,
                     aws_update_tag: int, iam_dict: dict) -> None:
    for principal_arn, policy_statement_map in principal_policy_map.items():
        logger.debug(f"Loading policies for principal {principal_arn}")
        for policy_key, statements in policy_statement_map.items():
            policy_name = policy_key if policy_type == PolicyType.inline.value else get_policy_name_from_arn(policy_key)
            policy_id = transform_policy_id(
                principal_arn,
                policy_type,
                policy_key,
            ) if policy_type == PolicyType.inline.value else policy_key

            load_policy(policy_id, policy_name, policy_type, principal_arn, aws_update_tag, iam_dict)
            load_policy_statements(policy_id, policy_name, statements, aws_update_tag, iam_dict)


@timeit
def sync_user_inline_policies(
        boto3_session: boto3.session.Session, data: Dict, neo4j_session: neo4j.Session,
        aws_update_tag: int, iam_dict: dict
) -> None:
    policy_data = get_user_policy_data(boto3_session, data['Users'])
    transform_policy_data(policy_data, PolicyType.inline.value)
    load_policy_data(policy_data, PolicyType.inline.value, aws_update_tag, iam_dict)


@timeit
def sync_users(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, current_aws_account_id: str,
        aws_update_tag: int, common_job_parameters: Dict, iam_dict: dict
) -> None:
    logger.info("Syncing IAM users for account '%s'.", current_aws_account_id)
    data = get_user_list_data(boto3_session)

    load_users(data['Users'], current_aws_account_id, aws_update_tag, iam_dict)

    sync_user_inline_policies(boto3_session, data, neo4j_session, aws_update_tag, iam_dict)
    #
    # sync_user_managed_policies(boto3_session, data, neo4j_session, aws_update_tag)
    #
    # run_cleanup_job('aws_import_users_cleanup.json', neo4j_session, common_job_parameters)

    with open('test.json', 'w+') as f:
        f.write(json.dumps(iam_dict, indent=4))
    f.close()


@timeit
def sync(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing IAM for account '%s'.", current_aws_account_id)
    # This module only syncs IAM information that is in use.
    # As such only policies that are attached to a user, role or group are synced

    iam_dict: dict = {
        'entities': {},
        'relationships': []
    }

    sync_users(neo4j_session, boto3_session, current_aws_account_id, update_tag, common_job_parameters, iam_dict)
