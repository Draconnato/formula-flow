import os
import yaml

def create_folder(folder) -> None:

    os.makedirs(os.path.dirname(folder), exist_ok=True)

def load_yaml(yaml_path) -> dict:
    
    with open(yaml_path, 'r') as f:
        yaml_parse = yaml.safe_load(f)

    return yaml_parse 