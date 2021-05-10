"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
import requests

from datetime import datetime
from typing import Dict, Generator

from airbyte_protocol import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from base_python import AirbyteLogger, Source


class SourceCodeConnector(Source):

    def _api_call(self, endpoint, token, headers):
        # get owned docs
        coda_token = token
        headers = headers
        #docs_uri = 'https://coda.io/apis/v1/docs'
        docs_uri = endpoint
        docs_params = {'isOwner': True}

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
                    
                    return rows_response

    # ********************** 
    # ********************** Implementing check connection *******************************
    # **********************

    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            Provided Coda API token can be used to connect to the Coda API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        # Validate input configuration by attempting to get the data response
        # get owned docs
        coda_token = config["api_key"]
        headers = {'Authorization': f'Bearer {config["api_key"]}'}
        docs_uri = 'https://coda.io/apis/v1/docs'
        docs_params = {'isOwner': True}
        doc_response = requests.get(docs_uri, headers=headers, params=docs_params)

        try:
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
                    #print("rows_response", json.dumps(rows_response))
            return AirbyteConnectionStatus(status=Status.SUCCEEDED, message="Here we go! Check succeeded")
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    # ********************** END - Implementing check connection *************************


    # ********************** 
    # ********************** Implementing discover connection *******************************
    # **********************
    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []

        stream_name = "coda_connector"  # Example
        json_schema = {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "array",
                    "items": {
                    "type": "object",
                    "properties": {
                        "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                            "id": {
                                "type": "string"
                            },
                            "type": {
                                "type": "string"
                            },
                            "href": {
                                "type": "string"
                            },
                            "name": {
                                "type": "string"
                            },
                            "index": {
                                "type": "number"
                            },
                            "createdAt": {
                                "type": "string"
                            },
                            "updatedAt": {
                                "type": "string"
                            },
                            "browserLink": {
                                "type": "string"
                            },
                            "values": {
                                "type": "object",
                                "properties": {
                                "dataset_id": {
                                    "type": "string"
                                },
                                "table_name": {
                                    "type": "string"
                                },
                                "client": {
                                    "type": "string"
                                },
                                "client_default_project": {
                                    "type": "string"
                                },
                                "project_id": {
                                    "type": "string"
                                },
                                "table_ref": {
                                    "type": "string"
                                },
                                "description": {
                                    "type": "string"
                                }
                                }
                            }
                            },
                            "required": [
                            "id",
                            "type",
                            "href",
                            "name",
                            "index",
                            "createdAt",
                            "updatedAt",
                            "browserLink",
                            "values"
                            ]
                        }
                        },
                        "href": {
                        "type": "string"
                        },
                        "nextSyncToken": {
                        "type": "string"
                        }
                    }
                    }
                }

        # Not Implemented

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema))
        return AirbyteCatalog(streams=streams)
    # ********************** END - Implementing discover connection *************************


    # ********************** 
    # ********************** Implementing read connection *******************************
    # **********************
    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.json file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """
        coda_token = config["api_key"]
        headers = {'Authorization': f'Bearer {config["api_key"]}'}
        docs_uri = 'https://coda.io/apis/v1/docs'
        docs_params = {'isOwner': True}

        stream_name = "CodaRows"  # Example
        #data = {"columnName": {"Hello World": "hi"}}
        data_res = self._api_call(docs_uri, coda_token, headers)
        data = data_res

        yield AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=int(datetime.now().timestamp()) * 1000),
        )
    # ********************** END - Implementing read connection *************************


# from airbyte-integrations/connectors/source-<source-name>
# python main_dev.py spec
# python main_dev.py check --config secrets/config.json
# python main_dev.py discover --config secrets/config.json
# python main_dev.py read --config secrets/config.json --catalog sample_files/configured_catalog.json
# python main_dev.py read --config secrets/config.json --catalog source_code_connector/schema/configured_catalog.json