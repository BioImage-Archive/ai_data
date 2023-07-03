import os
import yaml
import requests
from zipfile import ZipFile
from tqdm import tqdm
from io import BytesIO

def download_and_extract(dataset_url, dataset_name):
    if os.path.exists(f'./{dataset_name}'):
        print(f'{dataset_name} already exists. Skipping download.')
        return

    response = requests.get(dataset_url, stream=True)
    if response.status_code == 200:
        file_size = int(response.headers.get('content-length', 0))
        progress = tqdm(response.iter_content(1024), f'Downloading {dataset_name}', total=file_size, unit='B', unit_scale=True, unit_divisor=1024)
        content = b''
        for data in progress:
            content += data
        with ZipFile(BytesIO(content)) as zip_file:
            zip_file.extractall(f'./{dataset_name}')
    else:
        print(f'Failed to download {dataset_name}.')

def main():
    with open('zerocost.yaml', 'r') as file:
        datasets = yaml.safe_load(file)
    
    for dataset_name, zenodo_url in datasets.items():
        download_url = f'{zenodo_url}/files/{dataset_name}.zip?download=1'
        download_and_extract(download_url, dataset_name)
    print("All datasets downloaded and extracted successfully.")

if __name__ == '__main__':
    main()
