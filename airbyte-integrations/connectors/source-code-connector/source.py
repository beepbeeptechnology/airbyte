# source.py
import argparse  # helps parse commandline arguments
import json
import sys
import os
import requests

def read_json(filepath):
    with open(filepath, "r") as f:
        return json.loads(f.read())

def log(message):
    log_json = {"type": "LOG", "log": message}
    print(json.dumps(log_json))

# **********************  
# ********************** Implement the connector in line with the Airbyte Specification ***************************
# **********************
# Implement the spec operation
def spec():
    # Read the file named spec.json from the module directory as a JSON file
    current_script_directory = os.path.dirname(os.path.realpath(__file__))
    spec_path = os.path.join(current_script_directory, "spec.json")
    specification = read_json(spec_path)
    
    # form an Airbyte Message containing the spec and print it to stdout
    airbyte_message = {"type": "SPEC", "spec": specification}
    # json.dumps converts the JSON (python dict) to a string
    print(json.dumps(airbyte_message))
# *******************  END - Implement the connector in line with the Airbyte Specification **********************


# ********************** 
# ********************** Implementing check connection *******************************
# **********************
def _api_call(endpoint, token, headers):
    # get owned docs
    coda_token = token
    headers = headers
    #docs_uri = 'https://coda.io/apis/v1/docs'
    docs_uri = endpoint
    docs_params = {'isOwner': True}
    return requests.get(docs_uri, headers=headers, params=docs_params)

# helper method for reading input
def get_input_file_path(path):
    if os.path.isabs(path):
        return path
    else:
        return os.path.join(os.getcwd(), path)

def check(config):
    # Validate input configuration by attempting to get the data response
    # get owned docs
    coda_token = config["api_key"]
    headers = {'Authorization': f'Bearer {config["api_key"]}'}
    docs_uri = 'https://coda.io/apis/v1/docs'
    docs_params = {'isOwner': True}

    #doc_response = _api_call(endpoint="https://coda.io/apis/v1/docs", token=config["api_key"], headers=headers)
    doc_response = requests.get(docs_uri, headers=headers, params=docs_params)

    if doc_response.status_code == 200:
        result = {"status": "SUCCEEDED"}
        # get doc_ids
        doc_response_json = doc_response.json()
        for doc in doc_response_json['items']:
            doc_id = doc['id']
            tables_uri = f'https://coda.io/apis/v1/docs/{doc_id}/tables'
            tables_response = requests.get(tables_uri, headers=headers).json()
            
            # get table_ids in docs and get rows 
            for table in tables_response['items']:
                table_id = table['id']

                rows_params = {'useColumnNames': 'true' , 'valueFormat': 'simpleWithArrays'} 
                rows_uri = f'https://coda.io/apis/v1/docs/{doc_id}/tables/{table_id}/rows'
                rows_response = requests.get(rows_uri, headers=headers, params=rows_params).json()

                #print(rows_response)
                
    elif doc_response.status_code == 403:
        # HTTP code 403 means authorization failed so the API key is incorrect
        result = {"status": "FAILED", "message": "API Key is incorrect"}
    else:
        result = {"status": "FAILED", "message": "Input configuration is incorrect. Please verify the input stock ticker and API key."}
    
    output_message = {"type": "CONNECTION_STATUS", "connectionStatus": result}
    print(json.dumps(output_message))


# ********************** END - Implementing check connection *************************



def run(args):
    parent_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser()
    subparsers = main_parser.add_subparsers(title="commands", dest="command")

    # Accept the spec command
    subparsers.add_parser("spec", help="outputs the json configuration specification", parents=[parent_parser])

    # Accept the check command
    check_parser = subparsers.add_parser("check", help="checks the config used to connect", parents=[parent_parser])
    required_check_parser = check_parser.add_argument_group("required named arguments")
    required_check_parser.add_argument("--config", type=str, required=True, help="path to the json configuration file")

    parsed_args = main_parser.parse_args(args)
    command = parsed_args.command

    if command == "spec":
        spec()
    elif command == "check":
        config_file_path = get_input_file_path(parsed_args.config)
        config = read_json(config_file_path)
        check(config)
    else:
        # If we don't recognize the command log the problem and exit with an error code greater than 0 to indicate the process
        # # had a failure
        log("Invalid command. Allowable commands: [spec]")
        sys.exit(1)
    
    # A zero exit code means the process successfully completed
    sys.exit(0)

def main():
    arguments = sys.argv[1:]
    run(arguments)

if __name__ == "__main__":
    main()

