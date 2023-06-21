import logging
import pprint
from typing import Any
from typing import Dict
from typing import List

import boto3
import neo4j

import cartography.intel.aws.jsonwrappers.json_utils as json_utils
from cartography.util import timeit
from cartography.intel.aws.ec2.instances import get_ec2_instances, transform_ec2_instances
from cartography.intel.aws.jsonwrappers.service_enum import AWSServices

logger = logging.getLogger(__name__)


@timeit
def load_ec2_reservations(
        neo4j_session: neo4j.Session, reservation_list: List[Dict[str, Any]],
        region: str, current_aws_account_id: str,
        update_tag: int, ec2_dict: Dict) -> None:
    entities = ec2_dict['entities']

    for reservation in reservation_list:
        res_id = reservation['ReservationId']
        json_utils.add_to_entities(entities, reservation,
                                   res_id, update_tag, ['EC2Reservation'], region)

        # attach to AWS Account
        relationship_details = {
            'to_id': res_id, 'from_id': current_aws_account_id, 'to_label': 'EC2Reservation',
            'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(relationship_details, ec2_dict, update_tag)


@timeit
def load_ec2_instance_nodes(
        neo4j_session: neo4j.Session, data: List[Dict],
        region: str, current_aws_account_id: str,
        update_tag: int, ec2_dict: Dict) -> None:
    entities = ec2_dict['entities']

    for node in data:
        node_id = node['InstanceId']

        # add to entities
        json_utils.add_to_entities(entities, node, node_id, update_tag, ['EC2Instance'], region)

        # add relationships with AWS Account:RESOURCE
        aws_relationship = {
            'to_id': node_id, 'from_id': current_aws_account_id, 'to_label': 'EC2Instance', 'from_label': 'AWSAccount',
            'type': 'RESOURCE'
        }
        json_utils.add_relationship(aws_relationship, ec2_dict, update_tag)

        # add relationships with EC2Reservation:MEMBER_OF_EC2_RESERVATION
        ec2_reservation_relationship = {
            'to_id': node['ReservationId'], 'from_id': node_id, 'to_label': 'EC2Reservation',
            'from_label': 'EC2Instance', 'type': 'MEMBER_OF_EC2_RESERVATION'
        }
        json_utils.add_relationship(ec2_reservation_relationship, ec2_dict, update_tag)


@timeit
def load_ec2_subnets(
        neo4j_session: neo4j.Session, subnet_list: List[Dict[str, Any]],
        region: str, current_aws_account_id: str,
        update_tag: int, ec2_dict: Dict) -> None:
    entities = ec2_dict['entities']
    for subnet in subnet_list:
        subnet_id = subnet['SubnetId']
        json_utils.add_to_entities(entities, subnet, subnet_id, update_tag, ['EC2Subnet'], region)

        # Add relationship with AWS Account
        aws_relationship = {
            'to_id': subnet_id, 'from_id': current_aws_account_id, 'to_label': 'EC2Subnet',
            'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(aws_relationship, ec2_dict, update_tag)

        # Add relationship with EC2Instance
        ec2_instance_relationship = {
            'to_id': subnet_id, 'from_id': subnet['InstanceId'], 'to_label': 'EC2Subnet',
            'from_label': 'EC2Instance', 'type': 'PART_OF_SUBNET'
        }
        json_utils.add_relationship(ec2_instance_relationship, ec2_dict, update_tag)


@timeit
def load_ec2_security_groups(
        neo4j_session: neo4j.Session,
        sg_list: List[Dict[str, Any]], region: str,
        current_aws_account_id: str,
        update_tag: int, ec2_dict: Dict) -> None:
    entities = ec2_dict['entities']
    for sg in sg_list:
        sg_id = sg['GroupId']
        # add to entities
        json_utils.add_to_entities(entities, sg, sg_id, update_tag, ['EC2SecurityGroup'], region)

        # add relationship with AWS Account
        aws_relationship = {
            'to_id': sg_id, 'from_id': current_aws_account_id, 'to_label': 'EC2SecurityGroup',
            'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(aws_relationship, ec2_dict, update_tag)

        # add relationship with EC2Instance:MEMBER_OF_EC2_SECURITY_GROUP
        ec2_instance_relationship = {
            'to_id': sg_id, 'from_id': sg['InstanceId'], 'to_label': 'EC2SecurityGroup',
            'from_label': 'EC2Instance', 'type': 'MEMBER_OF_EC2_SECURITY_GROUP'
        }
        json_utils.add_relationship(ec2_instance_relationship, ec2_dict, update_tag)


@timeit
def load_ec2_key_pairs(
        neo4j_session: neo4j.Session,
        key_pair_list: List[Dict[str, Any]],
        region: str, current_aws_account_id: str,
        update_tag: int, ec2_dict: Dict) -> None:
    entities = ec2_dict['entities']
    for key_pair in key_pair_list:
        key_pair_id = key_pair['KeyPairArn']
        json_utils.add_to_entities(entities, key_pair, key_pair_id, update_tag, ['EC2KeyPair'], region)

        # add relationship with AWS Account:RESOURCE
        aws_relationship = {
            'to_id': key_pair_id, 'from_id': current_aws_account_id, 'to_label': 'EC2KeyPair',
            'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(aws_relationship, ec2_dict, update_tag)

        # add relationship with EC2Instance:SSH_LOGIN_TO
        ec2_instance_relationship = {
            'to_id': key_pair['InstanceId'], 'from_id': key_pair_id, 'to_label': 'EC2Instance',
            'from_label': 'EC2KeyPair', 'type': 'SSH_LOGIN_TO'
        }
        json_utils.add_relationship(ec2_instance_relationship, ec2_dict, update_tag)


@timeit
def load_ec2_network_interfaces(
        neo4j_session: neo4j.Session,
        network_interface_list: List[Dict[str, Any]],
        region: str, current_aws_account_id: str,
        update_tag: int, ec2_dict: Dict) -> None:
    entities = ec2_dict['entities']

    for interface in network_interface_list:
        interface_id = interface['NetworkInterfaceId']
        json_utils.add_to_entities(entities, interface, interface_id, update_tag, ['NetworkInterface'], region)

        # add relationship to AWSAccount:RESOURCE
        aws_rel = {
            'to_id': interface_id, 'from_id': current_aws_account_id, 'to_label': 'NetworkInterface',
            'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(aws_rel, ec2_dict, update_tag)

        # add relationship with EC2Instance
        ec2_instance_rel = {
            'to_id': interface_id, 'from_id': interface['InstanceId'], 'to_label': 'NetworkInterface',
            'from_label': 'EC2Instance', 'type': 'NETWORK_INTERFACE'
        }
        json_utils.add_relationship(ec2_instance_rel, ec2_dict, update_tag)

        # add relationship with EC2Subnet:PART_OF_SUBNET
        ec2_subnet_rel = {
            'to_id': interface['SubnetId'], 'from_id': interface_id, 'to_label': 'EC2Subnet',
            'from_label': 'NetworkInterface', 'type': 'PART_OF_SUBNET'
        }
        json_utils.add_relationship(ec2_subnet_rel, ec2_dict, update_tag)

        # add relationship with EC2SecurityGroup:MEMBER_OF_EC2_SECURITY_GROUP
        ec2_sg_rel = {
            'to_id': interface['GroupId'], 'from_id': interface_id, 'to_label': 'NetworkInterface',
            'from_label': 'EC2SecurityGroup', 'type': 'MEMBER_OF_EC2_SECURITY_GROUP'
        }
        json_utils.add_relationship(ec2_sg_rel, ec2_dict, update_tag)


@timeit
def load_ec2_instance_ebs_volumes(
        neo4j_session: neo4j.Session,
        ebs_data: List[Dict[str, Any]],
        region: str, current_aws_account_id: str,
        update_tag: int, ec2_dict: Dict) -> None:

    entities = ec2_dict['entities']
    for ebs in ebs_data:
        ebs_id = ebs['VolumeId']
        json_utils.add_to_entities(entities, ebs, ebs_id, update_tag, ['EBSVolume'], region)

        # add relationship with AWSAccount:RESOURCE
        aws_rel = {
            'to_id': ebs_id, 'from_id': current_aws_account_id, 'to_label': 'EBSVolume',
            'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }
        json_utils.add_relationship(aws_rel, ec2_dict, update_tag)

        # add relationship with EC2Instance
        ec2_instance_rel = {
            'to_id': ebs['InstanceId'], 'from_id': ebs_id, 'to_label': 'EC2Instance',
            'from_label': 'EBSVolume', 'type': 'ATTACHED_TO'
        }
        json_utils.add_relationship(ec2_instance_rel, ec2_dict, update_tag)


def load_ec2_instance_data(
        neo4j_session: neo4j.Session,
        region: str,
        current_aws_account_id: str,
        update_tag: int,
        reservation_list: List[Dict[str, Any]],
        instance_list: List[Dict[str, Any]],
        subnet_list: List[Dict[str, Any]],
        sg_list: List[Dict[str, Any]],
        key_pair_list: List[Dict[str, Any]],
        nic_list: List[Dict[str, Any]],
        ebs_volumes_list: List[Dict[str, Any]],
        ec2_dict: Dict
) -> None:
    load_ec2_reservations(neo4j_session, reservation_list, region, current_aws_account_id, update_tag, ec2_dict)
    load_ec2_instance_nodes(neo4j_session, instance_list, region, current_aws_account_id, update_tag, ec2_dict)
    load_ec2_subnets(neo4j_session, subnet_list, region, current_aws_account_id, update_tag, ec2_dict)
    load_ec2_security_groups(neo4j_session, sg_list, region, current_aws_account_id, update_tag, ec2_dict)
    load_ec2_key_pairs(neo4j_session, key_pair_list, region, current_aws_account_id, update_tag, ec2_dict)
    load_ec2_network_interfaces(neo4j_session, nic_list, region, current_aws_account_id, update_tag, ec2_dict)
    load_ec2_instance_ebs_volumes(neo4j_session, ebs_volumes_list, region, current_aws_account_id, update_tag, ec2_dict)


def split_and_write_to_json(ec2_dict: Dict, aws_acc_id: str) -> None:
    entities: Dict = ec2_dict['entities']

    ec2_instances: list = []
    ec2_sgs: list = []
    ec2_subnets: list = []
    ec2_key_pairs: list = []
    network_interfaces: list = []
    ec2_reservations: list = []
    ec2_volumes: list = []

    for _, entity in entities.items():
        l_list = entity['labels']
        if 'EC2SecurityGroup' in l_list:
            ec2_sgs.append(entity)
        if 'EC2Subnet' in l_list:
            ec2_subnets.append(entity)
        if 'EC2KeyPair' in l_list:
            ec2_key_pairs.append(entity)
        if 'NetworkInterface' in l_list:
            network_interfaces.append(entity)
        if 'EC2Instance' in l_list:
            ec2_instances.append(entity)
        if 'EC2Reservation' in l_list:
            ec2_reservations.append(entity)
        if 'EBSVolume' in l_list:
            ec2_volumes.append(entity)

    s_tag = AWSServices.EC2_INSTANCES.value
    json_utils.write_to_json(ec2_instances, 'ec2_instances.json', s_tag, aws_acc_id)
    json_utils.write_to_json(ec2_sgs, 'ec2_security_groups.json', s_tag, aws_acc_id)
    json_utils.write_to_json(ec2_subnets, 'ec2_subnets.json', s_tag, aws_acc_id)
    json_utils.write_to_json(ec2_key_pairs, 'ec2_key_pairs.json', s_tag, aws_acc_id)
    json_utils.write_to_json(ec2_reservations, 'ec2_reservations.json', s_tag, aws_acc_id)
    json_utils.write_to_json(ec2_volumes, 'ebs_volumes.json', s_tag, aws_acc_id)
    json_utils.write_to_json(network_interfaces, 'network_interfaces.json', s_tag, aws_acc_id)



@timeit
def sync_ec2_instances_json(
        neo4j_session: neo4j.Session,
        boto3_session: boto3.session.Session,
        regions: List[str], current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict[str, Any]) -> None:
    ec2_instances_dict: Dict = {
        'entities': {},
        'relationships': []
    }

    for region in regions:
        logger.info("Syncing EC2 instances for region '%s' in account '%s'.", region, current_aws_account_id)
        reservations = get_ec2_instances(boto3_session, region)
        ec2_data = transform_ec2_instances(reservations, region, current_aws_account_id)
        load_ec2_instance_data(
            neo4j_session,
            region,
            current_aws_account_id,
            update_tag,
            ec2_data.reservation_list,
            ec2_data.instance_list,
            ec2_data.subnet_list,
            ec2_data.sg_list,
            ec2_data.keypair_list,
            ec2_data.network_interface_list,
            ec2_data.instance_ebs_volumes_list,
            ec2_instances_dict
        )

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
    json_utils.override_properties(ec2_instances_dict, properties={})
    remove_properties = {
        None: ['DeleteOnTermination']
    }
    json_utils.exclude_properties(ec2_instances_dict, remove_properties)

    json_utils.create_folder(AWSServices.EC2_INSTANCES.value, current_aws_account_id)

    # write relationships to file
    json_utils.write_relationship_to_json(ec2_instances_dict, AWSServices.EC2_INSTANCES.value, current_aws_account_id)

    # split and write nodes to file
    split_and_write_to_json(ec2_instances_dict, current_aws_account_id)
