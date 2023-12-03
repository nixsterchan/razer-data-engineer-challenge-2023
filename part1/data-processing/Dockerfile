FROM python:3.11-slim

RUN apt-get update

# Set work directory
WORKDIR /usr/local/app

# Install dependencies from requirements.txt
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy code over to the working directory
COPY . .

ARG local_input_mode 
ARG local_input_file_path 
ARG local_input_file_format
ARG local_output_mode
ARG local_output_file_path
ARG local_output_file_format

# Set environment variables
ENV INPUT_MODE $local_input_mode
ENV INPUT_FILE_PATH $local_input_file_path
ENV INPUT_FILE_FORMAT $local_input_file_format
ENV OUTPUT_MODE $local_output_mode
ENV OUTPUT_FILE_PATH $local_output_file_path
ENV OUTPUT_FILE_FORMAT $local_output_file_format

ENTRYPOINT python ./process-data_v2.py --input_mode $INPUT_MODE --input_file_path $INPUT_FILE_PATH --input_file_format $INPUT_FILE_FORMAT --output_mode $OUTPUT_MODE --output_file_path $OUTPUT_FILE_PATH --output_file_format $OUTPUT_FILE_FORMAT 