import argparse
import time
import pandas as pd
import data_io_handler_v2


DTYPE_MAPPING = {
    'tconst': 'str',
    'titleType': 'str',
    'primaryTitle': 'str',
    'originalTitle': 'str',
    'isAdult': 'bool',
    'startYear': 'int',
    'endYear': 'int',
    'runtimeMinutes': 'int',
    'genres': 'str'
}

def main():
    # Record start time
    start_time = time.time()

    parser = argparse.ArgumentParser(description="This script serves to do some cleaning of the provided data.")

    parser.add_argument("--input_mode", type=str, choices=["local", "http", "api"], default="local", help="Input mode (local, http, or api)")
    parser.add_argument("--input_file_path", type=str, default="./data/input/title.basics.tsv.gz", help="Path to the input file (local, http, or api)")
    parser.add_argument("--input_file_format", type=str, choices=["tsv", "csv", "parquet"], default="tsv", help="Input file format. For now this supports tsv, csv and parquet")

    parser.add_argument("--output_mode", type=str, choices=["local", "s3"], default="local", help="Output mode (local, s3)")
    parser.add_argument("--output_file_path", type=str, default="./data/output/sample.csv", help="Name for the output file")
    parser.add_argument("--output_file_format", type=str, choices=['tsv', 'csv', 'parquet'], default="csv" , help="Output file format. For now this supports tsv, csv and parquet")

    args = parser.parse_args()

    # In case debugging is needed
    # print("Input Mode:", args.input_mode)
    # print("Input File Path:", args.input_file_path)
    # print("Input File Format:", args.input_file_format)
    # print("Output Mode:", args.output_mode)
    # print("Output File Path:", args.output_file_path)
    # print("Output File Format:", args.output_file_format)

    # # Read data from different sources
    df = data_io_handler_v2.read_data(args.input_mode, args.input_file_path, args.input_file_format)

    # Clean and transform data
    # Assumption made is that the schema to follow is the one from https://datasets.imdbws.com/title.basics.tsv.gz dataset
    df = df.replace('\\N', pd.NA)
    df= df.dropna().reset_index(drop=True)
    df = df.astype(DTYPE_MAPPING)
   

    # # Write data to different destinations
    data_io_handler_v2.write_data(args.output_mode, args.output_file_path, args.output_file_format, df)
    # Calculate and print duration
    duration = time.time() - start_time
    print(f"Time elapsed: {round(duration, 2)} seconds.")

if __name__ == "__main__":
    main()
