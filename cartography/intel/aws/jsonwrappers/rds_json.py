import datetime
import logging
import time
import os
from typing import Dict
from typing import List

import boto3
import neo4j

from cartography.intel.aws.jsonwrappers.service_enum import AWSServices
from cartography.intel.aws.rds import get_rds_cluster_data, get_rds_instance_data, _validate_rds_endpoint, \
    _get_db_subnet_group_arn, get_rds_snapshot_data, transform_rds_snapshots

import cartography.intel.aws.jsonwrappers.json_utils as json_utils

from cartography.stats import get_stats_client
from cartography.util import dict_value_to_str
from cartography.util import timeit

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)

json_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))


@timeit
def load_rds_clusters(
        neo4j_session: neo4j.Session, data: List[Dict], region: str, current_aws_account_id: str,
        aws_update_tag: int, rds_dict: dict) -> None:
    entities = rds_dict['entities']
    for cluster in data:

        # add additional data
        cluster['EarliestRestorableTime'] = dict_value_to_str(cluster, 'EarliestRestorableTime')
        cluster['LatestRestorableTime'] = dict_value_to_str(cluster, 'LatestRestorableTime')
        cluster['ClusterCreateTime'] = dict_value_to_str(cluster, 'ClusterCreateTime')
        cluster['EarliestBacktrackTime'] = dict_value_to_str(cluster, 'EarliestBacktrackTime')
        cluster['ScalingConfigurationInfoMinCapacity'] = cluster.get('ScalingConfigurationInfo', {}).get('MinCapacity')
        cluster['ScalingConfigurationInfoMaxCapacity'] = cluster.get('ScalingConfigurationInfo', {}).get('MaxCapacity')
        cluster['ScalingConfigurationInfoAutoPause'] = cluster.get('ScalingConfigurationInfo', {}).get('AutoPause')

        for key, value in cluster.items():
            if isinstance(value, datetime.datetime):
                cluster[key] = str(value)

        # save the cluster to entities where DBInstanceArn is the id
        entities[cluster['DBClusterArn']] = {
            'identity': cluster['DBClusterArn'],
            'labels': ['RDSCluster'],
        }
        entities[cluster['DBClusterArn']].update(cluster)

        # add remaining data to the properties not present in cluster
        entities[cluster['DBClusterArn']].update(
            {
                'firstseen': int(time.time()),
                'lastupdated': aws_update_tag,
                'region': region
            })

        # add relationship with the AWSAccount
        relationship_details = {
            'to_id': cluster['DBClusterArn'],
            'from_id': current_aws_account_id,
            'to_label': 'RDSCluster',
            'from_label': 'AWSAccount',
            'type': 'RESOURCE'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)


@timeit
def _attach_ec2_security_groups(neo4j_session: neo4j.Session, instances: List[Dict],
                                aws_update_tag: int, rds_dict: dict) -> None:
    groups = []
    for instance in instances:
        for group in instance['VpcSecurityGroups']:
            groups.append({
                'arn': instance['DBInstanceArn'],
                'group_id': group['VpcSecurityGroupId'],
            })

    entities = rds_dict['entities']
    for ec2group in groups:
        entities[ec2group['group_id']] = {
            'identity': ec2group['group_id'],
            'labels': ['EC2SecurityGroup'],
            'id': ec2group['group_id']
        }

        relationship_details = {
            'to_id': ec2group['group_id'], 'from_id': ec2group['arn'],
            'to_label': 'EC2SecurityGroup', 'from_label': 'RDSInstance',
            'type': 'MEMBER_OF_EC2_SECURITY_GROUP'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)


@timeit
def _attach_ec2_subnets_to_subnetgroup(
        neo4j_session: neo4j.Session, db_subnet_groups: List[Dict], region: str,
        current_aws_account_id: str, aws_update_tag: int, rds_dict: dict) -> None:
    subnets = []
    for subnet_group in db_subnet_groups:
        for subnet in subnet_group.get('Subnets', []):
            sn_id = subnet.get('SubnetIdentifier')
            sng_arn = _get_db_subnet_group_arn(region, current_aws_account_id, subnet_group['DBSubnetGroupName'])
            az = subnet.get('SubnetAvailabilityZone', {}).get('Name')
            subnets.append({
                'sn_id': sn_id,
                'sng_arn': sng_arn,
                'az': az,
            })

    entities = rds_dict['entities']
    for subnet in subnets:
        entities[subnet['sn_id']] = {
            'identity': subnet['sn_id'],
            'labels': ['EC2Subnet'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
            'SubnetAvailabilityZone': subnet['az'],
            'SubnetIdentifier': subnet['sn_id']
        }

        relationship_details = {
            'to_id': subnet['sn_id'], 'from_id': subnet['sng_arn'],
            'to_label': 'EC2Subnet', 'from_label': 'DBSubnetGroup', 'type': 'RESOURCE'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)


@timeit
def _attach_ec2_subnet_groups(
        neo4j_session: neo4j.Session, instances: List[Dict], region: str, current_aws_account_id: str,
        aws_update_tag: int, rds_dict: dict) -> None:
    db_subnet_groups = []
    for instance in instances:
        db_sng = instance['DBSubnetGroup']
        db_sng['arn'] = _get_db_subnet_group_arn(region, current_aws_account_id, db_sng['DBSubnetGroupName'])
        db_sng['instance_arn'] = instance['DBInstanceArn']
        db_subnet_groups.append(db_sng)

    entities = rds_dict['entities']
    for grp in db_subnet_groups:
        entities[grp['arn']] = {
            'identity': grp['arn'],
            'labels': ['DBSubnetGroup'],
        }
        entities[grp['arn']].update(grp)

        entities[grp['arn']].update({
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
        })

        relationship_details = {
            'to_id': grp['arn'], 'from_id': grp['instance_arn'],
            'to_label': 'DBSubnetGroup', 'from_label': 'RDSInstance',
            'type': 'MEMBER_OF_DB_SUBNET_GROUP'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)
    _attach_ec2_subnets_to_subnetgroup(neo4j_session, db_subnet_groups, region, current_aws_account_id, aws_update_tag,
                                       rds_dict)


@timeit
def _attach_read_replicas(neo4j_session: neo4j.Session, read_replicas: List[Dict],
                          aws_update_tag: int, rds_dict: dict) -> None:
    for replica in read_replicas:
        relationship_details = {
            'to_id': replica['ReadReplicaSourceDBInstanceIdentifier'], 'from_id': replica['DBInstanceArn'],
            'to_label': 'RDSInstance', 'from_label': 'RDSInstance', 'type': 'IS_READ_REPLICA_OF'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)


@timeit
def _attach_clusters(neo4j_session: neo4j.Session, cluster_members: List[Dict],
                     aws_update_tag: int, rds_dict: dict) -> None:
    for cluster_member in cluster_members:
        relationship_details = {
            'to_id': cluster_member['DBClusterIdentifier'], 'from_id': cluster_member['DBInstanceArn'],
            'to_label': 'RDSCluster', 'from_label': 'RDSInstance', 'type': 'IS_CLUSTER_MEMBER_OF'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)


@timeit
def load_rds_instances(
        neo4j_session: neo4j.Session, data: List[Dict], region: str, current_aws_account_id: str,
        aws_update_tag: int, rds_dict: dict) -> None:
    entities = rds_dict['entities']

    read_replicas = []
    clusters = []
    secgroups = []
    subnets = []

    for rds in data:
        ep = _validate_rds_endpoint(rds)

        # Keep track of instances that are read replicas so we can attach them to their source instances later
        if rds.get("ReadReplicaSourceDBInstanceIdentifier"):
            read_replicas.append(rds)

        # Keep track of instances that are cluster members so we can attach them to their source clusters later
        if rds.get("DBClusterIdentifier"):
            clusters.append(rds)

        if rds.get('VpcSecurityGroups'):
            secgroups.append(rds)

        if rds.get('DBSubnetGroup'):
            subnets.append(rds)

        rds['InstanceCreateTime'] = dict_value_to_str(rds, 'InstanceCreateTime')
        rds['LatestRestorableTime'] = dict_value_to_str(rds, 'LatestRestorableTime')
        rds['EndpointAddress'] = ep.get('Address')
        rds['EndpointHostedZoneId'] = ep.get('HostedZoneId')
        rds['EndpointPort'] = ep.get('Port')

        entities[rds['DBInstanceArn']] = {
            'identity': rds['DBInstanceArn'],
            'labels': ['RDSInstance'],
        }
        entities[rds['DBInstanceArn']].update(rds)

        relationship_details = {
            'to_id': rds['DBInstanceArn'],
            'from_id': current_aws_account_id,
            'to_label': 'RDSInstance', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)

    _attach_ec2_security_groups(neo4j_session, secgroups, aws_update_tag, rds_dict)
    _attach_ec2_subnet_groups(neo4j_session, subnets, region, current_aws_account_id, aws_update_tag, rds_dict)
    _attach_read_replicas(neo4j_session, read_replicas, aws_update_tag, rds_dict)
    _attach_clusters(neo4j_session, clusters, aws_update_tag, rds_dict)


@timeit
def _attach_snapshots(neo4j_session: neo4j.Session, snapshots: List[Dict],
                      aws_update_tag: int, rds_dict: dict) -> None:
    """
    Attach snapshots to their source instance
    """
    attach_member_to_source = """
    UNWIND $Snapshots as snapshot
        MATCH (rdsInstance:RDSInstance {db_instance_identifier: snapshot.DBInstanceIdentifier}),
        (rdsSnapshot:RDSSnapshot {arn: snapshot.DBSnapshotArn})
        MERGE (rdsInstance)-[r:IS_SNAPSHOT_SOURCE]->(rdsSnapshot)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = $aws_update_tag
    """

    for snapshot in snapshots:
        relationship_details = {
            'to_id': snapshot['DBSnapshotArn'], 'from_id': snapshot['DBInstanceIdentifier'],
            'to_label': 'RDSSnapshot', 'from_label': 'RDSInstance', 'type': 'IS_SNAPSHOT_SOURCE'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)


@timeit
def load_rds_snapshots(
        neo4j_session: neo4j.Session, data: Dict, region: str, current_aws_account_id: str,
        aws_update_tag: int, rds_dict: dict) -> None:
    snapshots = transform_rds_snapshots(data)

    entities = rds_dict['entities']
    for snapshot in snapshots:
        entities[snapshot['DBSnapshotArn']] = {
            'identity': snapshot['DBSnapshotArn'],
            'label': ['RDSSnapshot'],
            'firstseen': int(time.time()),
            'lastupdated': aws_update_tag,
        }

        entities[snapshot['DBSnapshotArn']].update({snapshot})

        relationship_details = {
            'to_id': snapshot['DBSnapshotArn'], 'from_id': current_aws_account_id,
            'to_label': 'RDSSnapshot', 'from_label': 'AWSAccount', 'type': 'RESOURCE'
        }

        json_utils.add_relationship(relationship_details, rds_dict, aws_update_tag)

    _attach_snapshots(neo4j_session, snapshots, aws_update_tag, rds_dict)


def split_and_write_entities_to_json(data: dict, aws_acc_id: str) -> None:
    """
    Method to split the rds_dict into sub dictionaries of entities that are similar to each other
    """
    entities: dict = data['entities']

    subnet_groups = []
    cluster_and_instances = []

    for _, entity in entities.items():
        l_list = entity['labels']  # list of labels
        if 'RDSInstance' in l_list or 'RDSCluster' in l_list or 'RDSSnapshot' in l_list:
            cluster_and_instances.append(entity)
        if 'DBSubnetGroup' in l_list or 'EC2Subnet' in l_list or 'EC2SecurityGroup' in l_list or 'DBSubnetGroup' in l_list:
            subnet_groups.append(entity)

    json_utils.write_to_json(subnet_groups, 'subnets.json', AWSServices.RDS.value, aws_acc_id)
    json_utils.write_to_json(cluster_and_instances, 'rds_cluster_instance.json', AWSServices.RDS.value, aws_acc_id)


@timeit
def sync_rds_clusters(neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
                      current_aws_account_id: str, update_tag: int,
                      common_job_parameters: Dict, rds_dict: dict) -> None:
    for region in regions:
        logger.info("Syncing RDS for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_rds_cluster_data(boto3_session, region)
        load_rds_clusters(neo4j_session, data, region, current_aws_account_id, update_tag, rds_dict)  # type: ignore


@timeit
def sync_rds_instances(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str, update_tag: int, common_job_parameters: Dict, rds_dict: dict) -> None:
    """
        Grab RDS instance data from AWS, ingest to neo4j, and run the cleanup job.
        """
    for region in regions:
        logger.info("Syncing RDS for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_rds_instance_data(boto3_session, region)
        load_rds_instances(neo4j_session, data, region, current_aws_account_id, update_tag, rds_dict)


@timeit
def sync_rds_snapshots(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict, rds_dict: dict
) -> None:
    """
    Grab RDS snapshot data from AWS, ingest to neo4j, and run the cleanup job.
    """
    for region in regions:
        logger.info("Syncing RDS for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_rds_snapshot_data(boto3_session, region)
        load_rds_snapshots(neo4j_session, data, region, current_aws_account_id, update_tag, rds_dict)  # type: ignore


@timeit
def sync(
        neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str],
        current_aws_account_id: str,
        update_tag: int, common_job_parameters: Dict) -> None:
    rds_dict = {
        'entities': {},
        'relationships': []
    }

    sync_rds_clusters(
        neo4j_session, boto3_session, regions, current_aws_account_id, update_tag,
        common_job_parameters, rds_dict
    )
    sync_rds_instances(
        neo4j_session, boto3_session, regions, current_aws_account_id, update_tag,
        common_job_parameters, rds_dict
    )
    sync_rds_snapshots(
        neo4j_session, boto3_session, regions, current_aws_account_id, update_tag,
        common_job_parameters, rds_dict
    )

    """
    Method below to override any properties in the entities
    format:
    override_properties = {
        # overrides in only entities with labels having RDSInstance
        'RDSInstance': {
            'BackupRetentionPeriod': 1,
            ...
        }
        # overrides it in all entities
        None: {
            'SomePropertyName': 2
        }
    }
    label tag as the key and value being the dict that contains key value pairs of properties that need to be updated
    If the property is not found in the properties, it appends to it. To override the same property everywhere, use None
    as the label
    
    """
    json_utils.override_properties(rds_dict, properties={})

    """
    Method below removes properties that are not wanted in the entities. 
    remove_keys = {
        'RDSInstance': ['DBClusterMembers', ...] 
    }
    Multiple labels can be provided. Incase of just providing the properties and not any labels, keep the key
    as None and provide all the properties in a single list. Keys with the None tag will be removed from all 
    entities instead of label specific
    remove_keys = {
        None: ['DBClusterMembers', ...],
        'RDSInstance': ['DBClusterMembers']
    }
    """
    keys_to_remove = {
        None: [
            'DBClusterMembers', 'VpcSecurityGroups', 'MasterUserSecret', 'VpcSecurityGroups', 'DBSubnetGroup',
            'OptionGroupMemberships', 'CertificateDetails', 'DBParameterGroups', 'Subnets', 'StatusInfos'
        ]
    }
    json_utils.exclude_properties(rds_dict, keys_to_remove)

    json_utils.create_folder(AWSServices.RDS.value, current_aws_account_id)

    # write relationships to json file
    json_utils.write_relationship_to_json(rds_dict, AWSServices.RDS.value, current_aws_account_id)
    split_and_write_entities_to_json(rds_dict, current_aws_account_id)
