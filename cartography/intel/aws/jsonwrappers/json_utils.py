import datetime
import time
import json
import logging
import os

logger = logging.getLogger(__name__)


def add_relationship(relationship_details: dict, source_dict: dict) -> None:
    """
    :param relationship_details: details of relationship between two entities
    :param source_dict: the s3_dict that holds all the entities and relationships
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
                'lastupdated': int(time.time())
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


def write_relationship_to_json(data: dict, folder_path: str) -> None:
    try:
        relationships = data['relationships']
        relationships_json = json.dumps(relationships, default=default_json_serializer, indent=4)
        with open(f'{folder_path}/relationships.json', 'w+') as relationship_file:
            relationship_file.write(relationships_json)
        relationship_file.close()
    except Exception as e:
        logger.warning("Error occured saving relationships to json")


def write_to_json(data: list[dict], filepath: str) -> None:
    try:
        entities = {
            'entities': data
        }
        nodes_json_dump = json.dumps(entities, default=default_json_serializer, indent=4)
        with open(f'{filepath}', 'w+') as nodes_file:
            nodes_file.write(nodes_json_dump)
        nodes_file.close()

    except Exception as e:
        logger.warning("Error occurred while saving to JSON")


def create_folder(folder_path: str, current_aws_account_id: str) -> None:
    """
    Create folder if they do not exist for output json files
    :param folder_path: the path for folder
    :param current_aws_account_id: the aws account id
    :return: None
    """
    parent_common_path = f'{folder_path}/jsonassets/{current_aws_account_id}'
    folders = [
        f'{folder_path}/jsonassets/', parent_common_path, f'{parent_common_path}/rds/', f'{parent_common_path}/s3/',
        f'{parent_common_path}/dynamodb/']
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)