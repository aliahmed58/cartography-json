import json
import logging
import time
from typing import Dict
from typing import List

import boto3
import botocore.config
import neo4j
import cartography.intel.aws.jsonwrappers.json_utils as json_utils
from cartography.intel.aws.jsonwrappers.service_enum import AWSServices

from policyuniverse.policy import Policy
from cartography.intel.aws.elasticsearch import _get_es_domains, _get_botocore_config
from cartography.intel.dns import ingest_dns_record_by_fqdn
from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
def _link_es_domains_to_dns(
        neo4j_session: neo4j.Session, domain_id: str, domain_data: Dict,
        aws_update_tag: int, es_dict: Dict) -> None:

    """

    """
    if domain_data.get('Endpoint'):
        pass
    else:
        logger.debug(f"No es endpoint data for domain id {domain_id}")


@timeit
def _link_es_domain_vpc(neo4j_session: neo4j.Session, domain_id: str,
                        domain_data: Dict, aws_update_tag: int, es_dict: Dict) -> None:

    if domain_data.get("VPCOptions"):
        vpc_data = domain_data["VPCOptions"]
        subnetList = vpc_data.get("SubnetIds", [])
        groupList = vpc_data.get("SecurityGroupIds", [])

        for subnet in subnetList:
            relationship_details = {
                'to_id': subnet, 'from_id': domain_id, 'to_label': 'EC2Subnet', 'from_label': 'ESDomain',
                'type': 'PART_OF_SUBNET'
            }
            json_utils.add_relationship(relationship_details, es_dict, aws_update_tag)

        for sec_grp in groupList:
            sec_relationship = {
                'to_id': sec_grp, 'from_id': domain_id, 'to_label': 'EC2SecurityGroup',
                'from_label': 'ESDomain', 'type': 'MEMBER_OF_EC2_SECURITY_GROUP'
            }
            json_utils.add_relationship(sec_relationship, es_dict, aws_update_tag)

@timeit
def _load_es_domains(
        neo4j_session: neo4j.Session, domain_list: List[Dict],
        aws_account_id: str, aws_update_tag: int, es_dict: Dict) -> None:

    entities = es_dict['entities']

    for domain in domain_list:

        domain_id = domain['DomainId']

        entities[domain_id] = {
            'identity': domain_id,
            'labels': ['ESDomain'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
        }

        entities[domain_id].update(domain)

        relationship_details = {
            'to_id': domain_id, 'from_id': aws_account_id,
            'to_label': 'ESDomain', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(relationship_details, es_dict, aws_update_tag)

        # TODO see if carto connects EC2SecurityGroups with ESDomain
        _link_es_domains_to_dns(neo4j_session, domain_id, domain, aws_update_tag, es_dict)
        # TODO Figure out connecting domain to vpc
        # _link_es_domain_vpc(neo4j_session, domain_id, domain, aws_update_tag)
        _process_access_policy(neo4j_session, domain_id, domain, es_dict)


@timeit
def _process_access_policy(neo4j_session: neo4j.Session, domain_id: str, domain_data: Dict, es_dict: Dict) -> None:
    """
    Link the ES domain to its DNS FQDN endpoint and create associated nodes in the graph
    if needed

    :param neo4j_session: Neo4j session object
    :param domain_id: ES domain id
    :param domain_data: domain data
    """

    exposed_internet = False

    if domain_data.get("Endpoint") and domain_data.get("AccessPolicies"):
        policy = Policy(json.loads(domain_data['AccessPolicies']))
        if policy.is_internet_accessible():
            exposed_internet = True

    entities = es_dict['entities']
    entities[domain_id]['InternetExposed'] = exposed_internet


def split_and_write_to_json(es_dict: Dict, aws_acc_id: str) -> None:
    entities: Dict = es_dict['entities']

    es_domains: list = []
    for _, entity in entities.items():
        l_list = entity['labels']
        if 'ESDomain' in l_list:
            es_domains.append(entity)

    json_utils.write_to_json(es_domains, 'es_domains.json', AWSServices.ELASTIC_SEARCH.value, aws_acc_id)


@timeit
def sync(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict) -> None:

    es_dict: Dict = {
        'entities': {},
        'relationships': []
    }

    for region in regions:
        logger.info("Syncing Elasticsearch Service for region '%s' in account '%s'.", region, current_aws_account_id)
        client = boto3_session.client('es', region_name=region, config=_get_botocore_config())
        data = _get_es_domains(client)
        _load_es_domains(neo4j_session, data, current_aws_account_id, update_tag, es_dict)

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
    json_utils.override_properties(es_dict, properties={})
    remove_properties = {
        None: ['NotificationConfiguration', 'CacheParameterGroup', 'SecurityGroups', 'ConfigurationEndpoint']
    }
    json_utils.exclude_properties(es_dict, remove_properties)

    # create folders
    json_utils.create_folder(AWSServices.ELASTIC_SEARCH.value, current_aws_account_id)

    # write relationships to json
    json_utils.write_relationship_to_json(es_dict, AWSServices.ELASTIC_SEARCH.value, current_aws_account_id)

    split_and_write_to_json(es_dict, current_aws_account_id)
