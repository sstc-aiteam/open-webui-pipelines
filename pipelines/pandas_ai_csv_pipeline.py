"""
title: PandasAI CSV Query
author: open-webui
date: 2024-05-31
version: 1.0
license: MIT
description: A pipeline for querying CSV files using PandasAI.
requirements: pandasai, pandas, pandasai_openai
"""

import os
import glob
import json
import requests
import hashlib

import pandas as pd

from logging import getLogger
from typing import List, Union, Generator, Iterator, Any, Callable

from pydantic import BaseModel, Field

from utils.pipelines.main import get_last_user_message

logger = getLogger(__name__)


class Pipeline:
    """
    A pipeline for querying CSV files using PandasAI.
    This pipeline loads all CSV files from a specified directory and uses the PandasAI library
    to answer questions about the data in those files.
    """

    class Valves(BaseModel):
        """
        Configuration valves for the pipeline.
        These can be overridden by setting environment variables.
        """

        CSV_DIR: str = Field(
            default="csv",
            description="Directory containing CSV files, relative to the 'pipelines' directory.",
        )
        OPEN_WebUI_Host: str = Field(
            default="http://localhost:3000", 
            description="Open WebUI host URL. (use docker internal host when you host OpenWebui within docker)"
        )
        OPEN_WebUI_API_KEY: str = Field(
            default="", 
            description="Open WebUI API key for the file API api/v1/files/\{id\}/content access."
        )
        OPENAI_API_KEY: str = Field(
            default="", 
            description="OpenAI API key. Can be set via OPENAI_API_KEY environment variable."
        )
        OPENAI_MODEL: str = Field(
             default="gpt-5.1", description="OpenAI model to use for PandasAI."
#            default="gpt-5-mini", description="OpenAI model to use for PandasAI."
#            default="gpt-4.1-mini", description="OpenAI model to use for PandasAI."
        )

    def __init__(self):
        self.name = "PandasAI CSV Pipeline"
        self.valves = self.Valves(
            **{k: os.getenv(k, v.default) for k, v in self.Valves.model_fields.items()}
        )
        self.pai_agent = None

    def setup_pandasai_agent(self):
        import pandasai as pai
        from pandasai import Agent
        from pandasai_litellm.litellm import LiteLLM

        if not self.valves.OPENAI_API_KEY:
            logger.warning(
                "OpenAI API key is not set. Please set the OPENAI_API_KEY environment variable. The pipeline will not be able to answer questions."
            )
            return

        # The CSV_DIR is relative to the `pipelines` directory
        csv_dir_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), self.valves.CSV_DIR
        )
        csv_files = glob.glob(os.path.join(csv_dir_path, "*.csv"))

        if not csv_files:
            logger.warning(
                f"No CSV files found in {csv_dir_path}. The pipeline will not be able to answer questions."
            )
            return

        dataframes = [pai.read_csv(file) for file in csv_files]

        # Provide names to dataframes for the agent to identify them
        for df, filename in zip(dataframes, csv_files):
            df.name = os.path.splitext(os.path.basename(filename))[0]

        ### llm = OpenAI(
        ###     api_token=self.valves.OPENAI_API_KEY, 
        ###     model=self.valves.OPENAI_MODEL
        ### )
        llm = LiteLLM(
            model=self.valves.OPENAI_MODEL,
            api_key=self.valves.OPENAI_API_KEY
        )
        self.pai_agent = Agent(dataframes, config={"llm": llm, "verbose": True})
        logger.info(
            f"PandasAI agent initialized with {len(dataframes)} dataframes from {csv_dir_path}."
        )

    async def on_startup(self):
        """
        Initializes the PandasAI agent on server startup.
        It loads CSV files from the specified directory and sets up the agent with an OpenAI LLM.
        """
        logger.debug(f"on_startup:{self.name}")
        self.setup_pandasai_agent()

    async def on_shutdown(self):
        logger.debug(f"on_shutdown:{self.name}")
        pass

    def check_duplicate(self, csv_dir_path: str, content: bytes) -> bool:
        file_hash = hashlib.sha256(content).hexdigest()
        for existing_file in glob.glob(os.path.join(csv_dir_path, "*.csv")):
            if os.path.getsize(existing_file) == len(content):
                with open(existing_file, "rb") as f:
                    if hashlib.sha256(f.read()).hexdigest() == file_hash:
                        logger.info(f"Duplicate file content found in {os.path.basename(existing_file)}, skipping download.")
                        return True
        return False

    async def inlet(self, body: dict, user: dict) -> dict:
        # Get received files if no specific task is set in metadata
        if body.get("metadata", {}).get("task") is None:
            # logger.info(f"inlet body: {json.dumps(body, ensure_ascii=False)}")

            files_added = False
            files = body.get("files", [])
            for file in files:
                id = file.get("id")
                url_path = file.get("url")
                name = file.get("name")
                logger.info(f"get uploaded file: {name} {id} with access URL and ID: {url_path}")

                if url_path and name.endswith(".csv"):
                    csv_dir_path = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)), self.valves.CSV_DIR
                    )
                    if not os.path.exists(csv_dir_path):
                        os.makedirs(csv_dir_path)

                    try:
                        headers = {}
                        if self.valves.OPEN_WebUI_API_KEY:
                            headers["Authorization"] = f"Bearer {self.valves.OPEN_WebUI_API_KEY}"
                        url = f"{self.valves.OPEN_WebUI_Host}{url_path}/content"
                        response = requests.get(url, headers=headers)
                        response.raise_for_status()

                        content = response.content
                        if not self.check_duplicate(csv_dir_path, content):
                            save_name = "_".join([id, os.path.basename(name)])
                            with open(os.path.join(csv_dir_path, save_name), "wb") as f:
                                f.write(content)
                            logger.info(f"Downloaded {save_name} to {csv_dir_path}")
                            files_added = True
                    except Exception as e:
                        logger.error(f"Error downloading file: {e}")
            
            if files_added:
                self.setup_pandasai_agent()
            
            last_user_message = get_last_user_message(body.get("messages", []))
            if not last_user_message:
                return body
            
            rs = ""
            if not self.pai_agent:
                rs = "PandasAI agent is not initialized. Please ensure CSV files are present and your OPENAI_API_KEY is set, then restart the pipeline."
            
            try:
                logger.info(f"user message for PandasAI: {last_user_message}")
                response = self.pai_agent.chat(last_user_message)

                # The response can be a string, a number, a pandas DataFrame, or a plot object.
                # We convert it to a string to be sent back to the user.
                if isinstance(response, (pd.DataFrame, pd.Series)):
                    rs = response.to_markdown()
                else:
                    rs = str(response)
            except Exception as e:
                logger.error(f"Error during PandasAI chat: {e}")
                rs = "Sorry, I encountered an error while processing your request with PandasAI."
            
            body["response"] = rs

        return body

    def pipe(
        self, 
        user_message: str, 
        model_id: str, 
        messages: List[dict], 
        body: dict,
    ) -> Union[str, Generator, Iterator]:
        """
        This method is called for each user message.
        It uses the PandasAI agent to answer the user's question based on the loaded CSV files.
        """

        return body.get("response", "")

        # if not self.pai_agent:
        #     return "PandasAI agent is not initialized. Please ensure CSV files are present and your OPENAI_API_KEY is set, then restart the pipeline."

        # try:
        #     response = self.pai_agent.chat(user_message)

        #     # The response can be a string, a number, a pandas DataFrame, or a plot object.
        #     # We convert it to a string to be sent back to the user.
        #     if isinstance(response, (pd.DataFrame, pd.Series)):
        #         return response.to_markdown()
        #     else:
        #         return str(response)
        # except Exception as e:
        #     logger.error(f"Error during PandasAI chat: {e}")
        #     return "Sorry, I encountered an error while processing your request with PandasAI."
