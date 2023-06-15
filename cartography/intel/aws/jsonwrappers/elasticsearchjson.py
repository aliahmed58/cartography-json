import json
import logging
import time
from typing import Dict
from typing import List

import boto3
import botocore.config
import neo4j
import cartography.intel.aws.jsonwrappers.json_utils as json_utils

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
def _load_es_domains(
        neo4j_session: neo4j.Session, domain_list: List[Dict],
        aws_account_id: str, aws_update_tag: int, es_dict: Dict) -> None:

    entities = es_dict['entities']

    for domain in domain_list:

        domain_id = domain['DomainId']

        entities[domain['DomainId']] = {
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
        json_utils.add_relationship(relationship_details, es_dict)

        _link_es_domains_to_dns(neo4j_session, domain_id, domain, aws_update_tag, es_dict)
        # _link_es_domain_vpc(neo4j_session, domain_id, domain, aws_update_tag)
        # _process_access_policy(neo4j_session, domain_id, domain)

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