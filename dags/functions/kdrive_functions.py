import tempfile
import os
import io
import http
import pandas as pd
import requests
import http
import csv
import json
from airflow.hooks.base import BaseHook

class kDrive():
    def __init__(self) -> None:
        self.conn = BaseHook.get_connection('kdrive')

    def create_header(self):
        kd_token = self.conn.password
        header = {
        'Authorization': f'Bearer {kd_token}',
        'Content-Type': 'application/json',
        }
        return header

    def upload_files(self, filetype, df, filename, directory_id):
        header = self.create_header()
        with tempfile.NamedTemporaryFile(mode='w', delete=True, prefix=filename) as temp:
            if filetype == 'parquet':
                df.to_parquet(temp.name, index=False)
            elif filetype == 'csv':
                df.to_csv(temp.name, index=False)

            with open(temp.name, 'rb') as f:
                file = f.read()
            file_size = os.path.getsize(temp.name)

            print('Filename: ' + temp.name)
            print('Filesize: ' + str(file_size))

            schema = self.conn.schema
            version = '3'
            extra = self.conn.extra
            kdrive_id = json.loads(extra)['kdrive_id']
            conn = http.client.HTTPSConnection(self.conn.host)
            conn.request("POST", f'{schema}{version}/drive/{kdrive_id}/upload?total_size={file_size}&directory_id={directory_id}&file_name={filename}.{filetype}&conflict=version', file, header)
            res = conn.getresponse()
            data = res.read()
            print(data.decode("utf-8"))

    def files_in_directory(self, folder_id):
        # Return Authentication Header
        header = self.create_header()

        # Define URL
        schema = self.conn.schema
        version = '3'
        extra = self.conn.extra
        kdrive_id = json.loads(extra)['kdrive_id']
        url=f'{schema}{version}/drive/{kdrive_id}/files/{folder_id}/files'

        id_list = []
        # Get data
        r = requests.get(url=url, headers=header)
        
        if r.status_code == 200:
            # content = json.loads(r.decode('utf-8'))
            content = r.json()
            for i in range(len(content['data'])):
                data = content['data'][i]
                id_list.append(data['id'])

            cursor = content['cursor']
            has_more = content['has_more']
            while has_more == True:
                loop_resp = requests.get(url + '?cursor=' + cursor, headers=header)
                loop_cont = loop_resp.json()
                # loop_cont = json.loads(loop_resp.decode('utf-8'))

                for j in range(len(loop_cont['data'])):
                    loop_data = loop_cont['data'][j]
                    id_list.append(loop_data['id'])

                has_more = loop_cont['has_more']
                cursor = loop_cont['cursor']
            return id_list
        else:
            return r.json()['error']['code']
        
    def return_filename(self, file_id):
        header = self.create_header()

        schema = self.conn.schema
        version = '2'
        extra = self.conn.extra
        kdrive_id = json.loads(extra)['kdrive_id']

        url=f'{schema}{version}/drive/{kdrive_id}/files/{file_id}'
        r = requests.get(url=url, headers=header)
        filename = r.json()['data']['name']

        return filename
            
    def read_files(self, filetype, file_id):
        header = self.create_header()

        def find_delimiter(text):
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(text).delimiter
            return delimiter

        schema = self.conn.schema
        version = '2'
        extra = self.conn.extra
        kdrive_id = json.loads(extra)['kdrive_id']
        url=f'{schema}{version}/drive/{kdrive_id}/files/{file_id}/download'
        r = requests.get(url=url, headers=header)
        if filetype == 'excel':
            df = pd.read_excel(io.BytesIO(r.content), engine='openpyxl')
        elif filetype == 'csv':
            text = r.text
            delimiter = find_delimiter(text)
            df = pd.read_csv(io.StringIO(r.text), sep=delimiter)
        elif filetype == 'parquet':
            df = pd.read_parquet(io.BytesIO(r.content))
        
        return df