import datetime
import time
import json
import logging
from typing import Dict, Any, List
import os

logger = logging.getLogger(__name__)

out_directory = f'{os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))}/json_assets/'


def add_relationship(relationship_details: dict, source_dict: dict, aws_update_tag: int) -> None:
    """
    :param relationship_details: details of relationship between two entities
    :param source_dict: the s3_dict that holds all the entities and relationships
    :param aws_update_tag aws update tag to set the last updated field
    :return: None
    """
    relationships = source_dict['relationships']
    relationships.append(
        {
            'to_id': relationship_details['to_id'],
            'from_id': relationship_details['from_id'],
            'to_label': relationship_details['to_label'],
            'from_label': relationship_details['from_label'],
            'type': relationship_details['type'],
            'properties': {
                'firstseen': int(time.time()),
                'lastupdated': aws_update_tag
            }
        }
    )


def default_json_serializer(obj):
    """
    JSON serializer to handle datetime objects
    """
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def override_properties(data: dict, properties: dict) -> None:
    """
    :param data the data dictionary holding entities and relationships
    :param properties dict of properties that needs to be overriden
    """
    entities = data['entities'].values()
    for label, property_dict in properties.items():
        for entity in entities:
            if label is None:
                entity.update(property_dict)
            else:
                if label in entity['labels']:
                    entity.update(property_dict)


def exclude_properties(data: dict, properties: dict) -> None:
    """
    :param data the dictionary
    :param properties the list of keys that should be removed
    """
    entities = data['entities'].values()
    for label, property_list in properties.items():
        for entity in entities:
            if label is None:
                for prop in property_list:
                    entity.pop(prop, None)
            else:
                if label in entity['labels']:
                    for prop in property_list:
                        entity.pop(prop, None)


def write_relationship_to_json(data: dict, service_name: str, aws_acc_id: str) -> None:
    try:
        relationships = data['relationships']
        relationships_json = json.dumps(relationships, default=default_json_serializer, indent=4)
        with open(f'{out_directory}/{aws_acc_id}/{service_name}/relationships.json', 'w+') as relationship_file:
            relationship_file.write(relationships_json)
        relationship_file.close()
    except Exception as e:
        logger.warning("Error occured saving relationships to json")


def write_to_json(data: list[dict], filename: str, service_name: str, aws_acc_id: str) -> None:
    try:
        entities = {
            'entities': data
        }
        nodes_json_dump = json.dumps(entities, default=default_json_serializer, indent=4)
        with open(f'{out_directory}/{aws_acc_id}/{service_name}/{filename}', 'w+') as nodes_file:
            nodes_file.write(nodes_json_dump)
        nodes_file.close()

    except Exception as e:
        logger.warning("Error occurred while saving to JSON")


def create_folder(folder_name: str, current_aws_account_id: str) -> None:
    """
    Create folder if they do not exist for output json files
    :param folder_name: name of the folder
    :param current_aws_account_id: the aws account id
    :return: None
    """
    parent_common_path = f'{out_directory}/{current_aws_account_id}'
    dirs_to_create = [parent_common_path, f'{parent_common_path}/{folder_name}']
    for dir in dirs_to_create:
        if not os.path.exists(dir):
            os.makedirs(dir)


def get_out_folder_path(service_name: str, aws_account_id: str) -> str:
    return f'{out_directory}/{aws_account_id}/{service_name}'


def add_to_entities(entities: Dict, entity_to_add: Dict, entity_id: Any, update_tag: int, labels: List[str],
                    region: str = None):
    entities[entity_id] = {
        'identity': entity_id,
        'labels': labels,
        'firstseen': int(time.time()),
        'lastupdated': update_tag
    }
    entities[entity_id].update(entity_to_add)

    if region is not None:
        entities[entity_id].update({'Region': region})