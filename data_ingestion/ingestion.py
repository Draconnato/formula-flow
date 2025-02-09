import requests
import json
import time
from datetime import datetime, timedelta
from utils.general import create_folder

class Ingestion():

    def __init__(
        self, 
        url: str,
        end_point: str,
        path_to_save: str,
        parameter_query: dict = None, 
        logs: bool = True, 
        retry: int = 5
    ):
        self.url = url
        self.end_point = end_point
        self.parameter_query = parameter_query
        self.path_to_save = path_to_save
        self.retry = retry
        self.logs = logs
        self.file_name = f"{end_point}-{time.strftime('%Y%m%d-%H%M%S')}"
        self.full_file_path = f"{path_to_save}/{end_point}/{self.file_name}.json"

    def insert_audit_data(self) -> dict:
        # Audit data - ETL details

        audit = {
            '_run_id': time.time_ns(),
            '_file_name': self.file_name,
            '_load_at_utc': datetime.now().isoformat(),
            '_url': self.url,
            '_end_point': self.end_point,
            '_url_params': self.parameter_query
        }

        return audit

    def extract_data(self) -> None:

        attempt = 0
        
        while attempt < self.retry:
            
            response = requests.get(self.url + self.end_point, params=self.parameter_query)
            
            response_data = response.json() # Transforming response to json

            if response.status_code == 200:

                if len(response_data) == 0:
                    print('No data to capture, skipping')
                    
                    return
                
                create_folder(self.full_file_path)

                data = {
                    'content': response_data,
                    'audit': self.insert_audit_data()
                }

                with open(self.full_file_path, "w") as f:
                    f.write(json.dumps(data))
            
                return
            
            else:
            
                print(f"""
                    Error extracting {self.end_point} \n 
                    with query {self.parameter_query} \n
                    Attempt {attempt + 1} failed.
                    """
                )
            
                attempt += 1
            
                time.sleep(2.5)