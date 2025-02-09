from data_ingestion.ingestion import Ingestion
from utils.general import load_yaml 

config = load_yaml('config.yaml')

for end_point in config['bronze_layer']['end_points']:

    data_ingstion = Ingestion(
        url='https://api.openf1.org/v1/',
        end_point=end_point,
        path_to_save=config['bronze_layer']['path_to_save']
    )
    
    data_ingstion.extract_data()