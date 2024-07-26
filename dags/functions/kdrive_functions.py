import tempfile
import os
import http

class kDrive():
    def __init__(self, conn) -> None:
        self.conn = conn

    def upload_parquet(self, df, filename):
        with tempfile.NamedTemporaryFile(mode='w', delete=True, prefix=filename) as temp:
            df.to_parquet(temp.name, index=False)

            with open(temp.name, 'rb') as f:
                file = f.read()
            file_size = os.path.getsize(temp.name)

            print('Filename: ' + temp.name)
            print('Filesize: ' + str(file_size))

            headers = {
                'Authorization': 'Bearer ' + self.conn.password,
                'Content-Type': 'application/octet-stream',
                }

            conn = http.client.HTTPSConnection(self.conn.host)
            conn.request("POST", f'{self.conn.schema}/upload?total_size={file_size}&directory_id=5385&file_name={filename}&conflict=version', file, headers)
            res = conn.getresponse()
            data = res.read()
            print(data.decode("utf-8"))