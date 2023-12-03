import pandas as pd
import urllib3
import json
import boto3
import os
import gzip
import zipfile
import io
from urllib.parse import urlparse

def read_data(input_mode, input_path, input_file_format):
    """
    Read data from either an HTTP/API endpoint or a local file.

    Parameters:
    - input_mode (str): Expected input mode ('local', 'http' or 'api)
    - input_path (str): The path or URL to the data source.
    - input_file_format (str): Expected input file format

    Returns:
    - pandas.DataFrame: The loaded data as a DataFrame.
    """
    # Handle for either local or api/http mode
    if input_mode == 'local':
        return read_data_from_local_file(input_path, input_file_format)
    
    elif input_mode in ['http', 'api']:
        parsed_url = urlparse(input_path)
        if parsed_url.scheme and parsed_url.netloc:
            return read_data_from_http_api(input_path)
        else:
            raise ValueError(f"URL was unable to be parsed properly. Please check {parsed_url}.")
    else:
        raise ValueError(f"Invalid input mode was supplied {input_mode}.")
    
def read_data_from_http_api(url):
    """
    Read data from an HTTP or API endpoint.

    Parameters:
    - url (str): The URL of the data source.

    Returns:
    - pandas.DataFrame: The loaded data as a DataFrame.
    """
    http = urllib3.PoolManager()
    response = http.request('GET', url, headers={'Content-Type': 'application/json'})

    if response.status == 200:
        content_type = response.headers.get('Content-Type', '')
        print(content_type)
        if 'application/json' in content_type:
            try:
                data = json.loads(response.data.decode('utf-8'))
                return pd.DataFrame(data)
            except json.JSONDecodeError:
                return pd.DataFrame({'response_text': [response.data.decode('utf-8')]})
        elif 'text/csv' in content_type or 'text/plain' in content_type:
            return pd.read_csv(io.StringIO(response.data.decode('utf-8')))
        elif 'application/gzip' in content_type:
            return read_data_from_gzip(response.data)
        elif 'application/zip' in content_type:
            return read_data_from_zip(response.data)
        else:
            raise ValueError(f"Unsupported Content-Type: {content_type}.")
    else:
        raise ValueError(f"Failed to fetch data from {url}. Status code: {response.status}")

def read_data_from_gzip(gzip_data):
    """
    Read data from a gzip-compressed stream.

    Parameters:
    - gzip_data (bytes): The binary data of the gzip-compressed stream.

    Returns:
    - pandas.DataFrame: The loaded data as a DataFrame.
    """
    with gzip.GzipFile(fileobj=io.BytesIO(gzip_data), mode='rb') as f:
        # Process the decompressed data
        return pd.read_csv(io.TextIOWrapper(f, 'utf-8'), delimiter=',')  # Adjust delimiter as needed

def read_data_from_zip(zip_data):
    """
    Read data from a ZIP archive.

    Parameters:
    - zip_data (bytes): The binary data of the ZIP archive.

    Returns:
    - pandas.DataFrame: The loaded data as a DataFrame.
    """
    # Assuming there is only one file in the ZIP archive
    with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as z:
        file_in_zip = z.namelist()[0]
        with z.open(file_in_zip) as f:
            # Process the file within the ZIP archive
            return pd.read_csv(io.TextIOWrapper(f, 'utf-8'), delimiter=',')  # Adjust delimiter as needed
        
def read_data_from_local_file(file_path, input_file_format):
    """
    Read data from a local file.

    Parameters:
    - file_path (str): The path to the local file.
    - input_file_format (str): Expected input file format 

    Returns:
    - pandas.DataFrame: The loaded data as a DataFrame.
    """
    # Pandas allows for interence of the compression type
    if input_file_format == 'csv':
        return pd.read_csv(file_path, header=0, index_col=False)
    elif input_file_format == 'tsv':
        return pd.read_csv(file_path, sep='\t', header=0, index_col=False)
    elif input_file_format == 'parquet':
        return pd.read_parquet(file_path, index=False)
    else:
        raise ValueError(f"No supported file types were found.")


def write_data(output_mode, output_path, output_format, output_data):
    """
    Writes the output data to the specified location based on the output mode.

    Parameters:
        output_mode (str): The output mode ('local' or 's3').
        output_path (str): The output path or destination.
        output_format (str): The output format ('csv', 'tsv', 'parquet', etc.).
        output_data (pd.DataFrame): The DataFrame containing the output data.
    """
    if output_mode == 'local':
        write_data_local(output_path, output_format, output_data)
    elif output_mode == 's3':
        write_data_to_s3(output_path, output_format, output_data)
        

def write_data_local(output_path, output_format, output_data):
    """
    Writes the output data locally.

    Parameters:
        output_path (str): The local file path or destination.
        output_data (pd.DataFrame): The DataFrame containing the output data.
        output_format (str): The output format ('csv', 'tsv', 'parquet', etc.).
    """
    # Write data locally
    if output_format == 'csv':
        output_data.to_csv(output_path, header=True, index=False)
    elif output_format == 'tsv':
        # Example: Convert a Pandas DataFrame to Parquet format
        output_data.to_csv(output_path, header=True, index=False, sep='\t')
    elif output_format == 'parquet':
        # Example: Convert a Pandas DataFrame to Parquet format
        output_data.to_parquet(output_path, engine='pyarrow', index=False)
        pass
    else:
        print(f'Unsupported output format was supplied: {output_format}\nProceeding to use default csv.')
        output_data.to_csv(output_path, header=True, index=False)


def write_data_to_s3(output_path, output_format, output_data):
    """
    Writes the output data to an S3 bucket.

    Parameters:
        output_path (str): The S3 path (e.g., 's3://bucket_name/key').
        output_data (pd.DataFrame): The DataFrame containing the output data.
        output_format (str): The output format ('csv', 'tsv', 'parquet', etc.).
    """
    # Extract bucket name and key from the S3 path
    bucket_name, object_key = output_path.split('//')[1].split('/', 1)
    
    # Create a session using your AWS credentials
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
        region_name = os.getenv('AWS_REGION_NAME', 'ap-southeast-1')
    )
    
    # Create an S3 client using the session
    s3 = session.client('s3')

    # Write data to S3
    with io.BytesIO() as output_buffer:
        # Convert the data to bytes
        if output_format == 'csv':
            output_data.to_csv(output_buffer, header=True, index=False)
        elif output_format == 'tsv':
            output_data.to_csv(output_buffer, header=True, index=False, sep='\t')
        elif output_format == 'parquet':
            output_data.to_parquet(output_buffer, engine='pyarrow', index=False)
        else:
            print(f'Unsupported output format was supplied: {output_format}\nProceeding to use default csv.')
            output_data.to_csv(output_buffer, header=True, index=False)
        # Upload the data to S3
        s3.upload_fileobj(output_buffer, bucket_name, object_key)
