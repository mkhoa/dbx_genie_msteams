# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import json
import logging
import asyncio
from typing import Dict, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
from config import DefaultConfig

# Load environment variables
CONFIG = DefaultConfig()
# Log
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class GenieClient:
    def __init__(self, host: str = None, token: str = None):
    
        self.host = CONFIG.DATABRICKS_HOST
        self.token = CONFIG.DATABRICKS_TOKEN

    def get_genie_api(self) -> GenieAPI:
        """
        Returns the GenieAPI instance.
        """

        client = WorkspaceClient(
            host=self.host,
            token=self.token
        )

        genie_api = GenieAPI(client.api_client)

        return genie_api
    
    def get_space_id(self) -> str:
        """
        Returns the space ID.
        """

        space_id = CONFIG.DATABRICKS_SPACE_ID

        return space_id
    
    async def ask_genie(self, question: str, conversation_id: Optional[str] = None) -> tuple[str, str]:
        """Ask a question to the Genie API and return the response.
        
        """
        
        self.genie_api = self.get_genie_api()
        self.space_id = self.get_space_id()

        try:
            loop = asyncio.get_running_loop()
            if conversation_id is None:
                initial_message = await loop.run_in_executor(None, self.genie_api.start_conversation_and_wait, self.space_id, question)
                conversation_id = initial_message.conversation_id
            else:
                initial_message = await loop.run_in_executor(None, self.genie_api.create_message_and_wait, self.space_id, conversation_id, question)

            message_content = await loop.run_in_executor(None, self.genie_api.get_message,
                self.space_id, initial_message.conversation_id, initial_message.id)

            logger.info(f"Raw message content: {message_content}")

            if message_content.attachments:
                for attachment in message_content.attachments:
                    attachment_id = getattr(attachment, "attachment_id", None)
                    query_obj = getattr(attachment, "query", None)
                    if attachment_id and query_obj:
                        # Use the new endpoint to get query results
                        query_result = await loop.run_in_executor(
                            None,
                            self.genie_api.get_message_attachment_query_result,
                            self.space_id,
                            initial_message.conversation_id,
                            initial_message.message_id,
                            attachment_id
                        )
                        logger.info(f"Raw query result: {query_result}")
                        
                        query_description = getattr(query_obj, "description", "")
                        query_result_metadata = getattr(query_obj, "query_result_metadata", {})
                        statement_id = getattr(query_obj, "statement_id", "")
                        
                        if hasattr(query_result_metadata, "__dict__"):
                            query_result_metadata = query_result_metadata.__dict__
                        
                        logger.info(f"Query result metadata: {query_result_metadata}")
                        logger.info(f"Statement ID: {statement_id}")

                        response_data = {
                            "query_description": query_description,
                            "query_result_metadata": query_result_metadata,
                            "statement_id": statement_id
                        }

                        if query_result.statement_response:
                            response_data["statement_response"] = query_result.as_dict()['statement_response']
                            logger.info(f"Added statement_response to response: {response_data['statement_response']}")
                        else:
                            logger.error(f"Missing statement_response in query_result: {query_result}")

                        return json.dumps(response_data), conversation_id

                    text_obj = getattr(attachment, "text", None)
                    if text_obj and hasattr(text_obj, "content"):
                        return json.dumps({"message": text_obj.content}), conversation_id
            
            return json.dumps({"message": message_content.content}), conversation_id
        except Exception as e:
            logger.error(f"Error in ask_genie: {str(e)}")
            return json.dumps({"error": "An error occurred while processing your request."}), conversation_id    

    def process_query_results(self, answer_json: Dict) -> str:
        """Process the JSON response from Genie API and format to Markdown format it for display.
        
        
        """
        response = ""
        
        logger.info(f"Processing answer JSON: {answer_json}")
        
        if "query_description" in answer_json and answer_json["query_description"]:
            response += f"## Query Description\n\n{answer_json['query_description']}\n\n"

        if "query_result_metadata" in answer_json:
            metadata = answer_json["query_result_metadata"]
            if isinstance(metadata, dict):
                if "row_count" in metadata:
                    response += f"**Row Count:** {metadata['row_count']}\n\n"
                if "execution_time_ms" in metadata:
                    response += f"**Execution Time:** {metadata['execution_time_ms']}ms\n\n"

        if "statement_response" in answer_json:
            statement_response = answer_json["statement_response"]
            logger.info(f"Found statement_response: {statement_response}")
            
            if "result" in statement_response and "data_array" in statement_response["result"]:
                response += "## Query Results\n\n"
                
                schema = statement_response.get("manifest", {}).get("schema", {})
                columns = schema.get("columns", [])
                logger.info(f"Schema columns: {columns}")
                
                header = "| " + " | ".join(col["name"] for col in columns) + " |"
                separator = "|" + "|".join(["---" for _ in columns]) + "|"
                response += header + "\n" + separator + "\n"
                
                data_array = statement_response["result"]["data_array"]
                logger.info(f"Data array: {data_array}")
                
                for row in data_array:
                    formatted_row = []
                    for value, col in zip(row, columns):
                        if value is None:
                            formatted_value = "NULL"
                        elif col["type_name"] in ["DECIMAL", "DOUBLE", "FLOAT"]:
                            formatted_value = f"{float(value):,.2f}"
                        elif col["type_name"] in ["INT", "BIGINT", "LONG"]:
                            formatted_value = f"{int(value):,}"
                        else:
                            formatted_value = str(value)
                        formatted_row.append(formatted_value)
                    response += "| " + " | ".join(formatted_row) + " |\n"
            else:
                logger.error(f"Missing result or data_array in statement_response: {statement_response}")
        elif "message" in answer_json:
            response += f"{answer_json['message']}\n\n"
        else:
            response += "No data available.\n\n"
            logger.error("No statement_response or message found in answer_json")
        
        return response