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


def write_to_json(data: dict, folder_path: str, aws_account_id: str) -> None:
    try:
        # save entities to json
        entities = data['entities']
        list_of_nodes = list(entities.values())
        nodes_json_dump = json.dumps(list_of_nodes, default=default_json_serializer, indent=4)
        with open(f'{folder_path}/{aws_account_id}_nodes.json', 'w+') as nodes_file:
            nodes_file.write(nodes_json_dump)
        nodes_file.close()

        relationships = data['relationships']
        relationships_json = json.dumps(relationships, default=default_json_serializer, indent=4)
        with open(f'{folder_path}/{aws_account_id}_relationships.json', 'w+') as relationship_file:
            relationship_file.write(relationships_json)
        relationship_file.close()

    except Exception as e:
        logger.warning("Error occurred while saving to JSON")


def create_folder(folder_path: str) -> None:
    """
    Create folder if they do not exist for output json files
    :param folder_path: the path for folder
    :return: None
    """
    folders = [f'{folder_path}/jsonassets', f'{folder_path}/jsonassets/rds/', f'{folder_path}/jsonassets/s3/']
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)