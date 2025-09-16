"""
title: PandasAI CSV Query
author: open-webui
date: 2024-05-31
version: 1.0
license: MIT
description: A pipeline for querying CSV files using PandasAI.
requirements: pandasai, pandas, pandasai_openai
"""

from typing import List, Union, Generator, Iterator
from pydantic import BaseModel, Field
import pandas as pd
import os
import glob

from logging import getLogger

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
        OPENAI_API_KEY: str = Field(
            default="", 
            description="OpenAI API key. Can be set via OPENAI_API_KEY environment variable."
        )
        OPENAI_MODEL: str = Field(
            default="gpt-4.1", description="OpenAI model to use for PandasAI."
        )

    def __init__(self):
        self.name = "PandasAI CSV Pipeline"
        self.valves = self.Valves(
            **{k: os.getenv(k, v.default) for k, v in self.Valves.model_fields.items()}
        )
        self.pai_agent = None

    async def on_startup(self):
        """
        Initializes the PandasAI agent on server startup.
        It loads CSV files from the specified directory and sets up the agent with an OpenAI LLM.
        """
        import pandasai as pai
        from pandasai import Agent
        from pandasai_openai import OpenAI

        if not self.valves.OPENAI_API_KEY:
            logger.warning(
                "OpenAI API key is not set. Please set the OPENAI_API_KEY environment variable. The pipeline will not be able to answer questions."
            )
            return

        logger.debug(f"on_startup:{self.name}")

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

        llm = OpenAI(
            api_token=self.valves.OPENAI_API_KEY, 
            model=self.valves.OPENAI_MODEL
        )
        self.pai_agent = Agent(dataframes, config={"llm": llm, "verbose": True})
        logger.info(
            f"PandasAI agent initialized with {len(dataframes)} dataframes from {csv_dir_path}."
        )

    async def on_shutdown(self):
        logger.debug(f"on_shutdown:{self.name}")
        pass

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        """
        This method is called for each user message.
        It uses the PandasAI agent to answer the user's question based on the loaded CSV files.
        """
        if not self.pai_agent:
            return "PandasAI agent is not initialized. Please ensure CSV files are present and your OPENAI_API_KEY is set, then restart the pipeline."

        logger.info(f"User Message: {user_message}")

        try:
            response = self.pai_agent.chat(user_message)

            # The response can be a string, a number, a pandas DataFrame, or a plot object.
            # We convert it to a string to be sent back to the user.
            if isinstance(response, (pd.DataFrame, pd.Series)):
                return response.to_markdown()
            else:
                return str(response)
        except Exception as e:
            logger.error(f"Error during PandasAI chat: {e}")
            return "Sorry, I encountered an error while processing your request with PandasAI."
