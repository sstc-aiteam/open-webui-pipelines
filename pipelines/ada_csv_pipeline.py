"""
title: OpenAI Advanced Data Analysis CSV Query
author: open-webui
date: 2024-05-31
version: 1.0
license: MIT
description: A pipeline for querying CSV files using OpenAI Advanced Data Analysis.
requirements: openai, python-docx, pandas
"""

import os
import glob
import json
import requests
import hashlib
import asyncio
import io
import pandas as pd

from logging import getLogger
from typing import List, Union, Generator, Iterator, Optional, Any, Callable

from pydantic import BaseModel, Field
from openai import AsyncOpenAI
from docx import Document

from utils.pipelines.main import pop_system_message, get_last_user_message

logger = getLogger(__name__)


class Pipeline:
    """
    A pipeline for querying CSV files using OpenAI Advanced Data Analysis.
    This pipeline loads all CSV files from a specified directory and uses the OpenAI Assistants API
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
        DOC_DIR: str = Field(
            default="docs",
            description="Directory containing DOCX files, relative to the 'pipelines' directory.",
        )
        OPEN_WebUI_Host: str = Field(
            default="http://localhost:3000", 
            description="Open WebUI host URL. (use docker internal host when you host OpenWebui within docker)"
        )
        OPEN_WebUI_API_KEY: str = Field(
            default="", 
            description="Open WebUI API key for the file API api/v1/files/{id}/content access."
        )
        OPENAI_API_KEY: str = Field(
            default="", 
            description="OpenAI API key. Can be set via OPENAI_API_KEY environment variable."
        )
        OPENAI_MODEL: str = Field(
             default="gpt-5.1", description="OpenAI model to use for Code Interpreter."
        )
        SYSTEM_PROMPT_KEYWORD: str = Field(
            default="""
你是一位專精於智慧運輸系統（Intelligent Transportation Systems, ITS）領域的專業分析師，擅長從政策報告、技術文件與研究資料中抽取能支撐分類工作的關鍵資訊。
你的任務是：
針對使用者提供的 ITS 文件內容或知識庫文本，輸出可用於文件自動分類與檢索索引建立的結構化分析結果。

【語言與風格規範】

語言： 必須使用繁體中文，禁止出現簡體字或大陸用語。

風格： 採正式、嚴謹、邏輯清晰的分析報告口吻。

取詞準則： 關鍵詞應具備「語意指向性」與「分類可判斷性」，避免過於籠統或修辭性詞彙。

⚙️ 【任務流程與輸出邏輯】
🧠 Step 1｜模式判定與執行

1️⃣ 預設模式（不摘要）
　直接根據全文內容進行主題分類與關鍵詞擷取。
　此模式適合文件結構完整、主題集中者。

2️⃣ 摘要模式（使用者指定時採用）
　若使用者明確指令（如「請先摘要再抽取」或「採摘要模式」），則：
　- 先生成一份約500字 的專業摘要。
　- 再根據摘要進行關鍵詞擷取與分類分析。

💡 若未指定模式，完成初步擷取後請主動詢問：
「是否要改採 ‘先摘要再擷取關鍵詞’ 的模式，以獲得更概括的主題關鍵詞？」

🧩 Step 2｜輸出標準 JSON 格式結構化內容，不添加額外說明或建議

請一律輸出下列表格所示的四個主要區塊，並確保內容可支撐後續分類任務。

區塊編號	區塊名稱	輸出內容要求	範例參考
1️⃣	【主題分類】	以 2–4 組明確主題標籤表示文件核心範疇；建議使用既有 ITS 分類體系。	智慧號誌、車聯網(V2X)、MaaS、自駕車、交通管理政策、國際合作
2️⃣	【文件屬性】	指出文件性質，用以判斷資料來源與用途。	政策報告、技術規劃、試驗案例、研究成果、標準規範
3️⃣	【核心關鍵詞組】	抽取 5–10 個具代表性的多詞名詞組，並涵蓋「技術項目」、「政策面向」與「應用場域」三類資訊。	動態號誌控制系統、車流偵測設備、緊急車輛優先權、路側單元(RSU)、資料交換標準
4️⃣	【搜尋輔助詞】	針對關鍵詞組補充同義詞、縮寫、英文術語或常用變體，方便跨文件檢索。	動態號誌：智慧號誌控制、Adaptive Signal Control；EVP：緊急車輛優先通行、Emergency Vehicle Priority

🔍 提示： 關鍵詞組的選取應兼顧「主題層級」（政策、技術、服務）與「語意層級」（系統、設備、應用、成效），以利後續文件自動分群。
            """,
            description="System prompt for keyword extraction from document files."
        )

    def __init__(self):
        self.name = "OpenAI Advanced Data Analysis CSV Pipeline"
        self.valves = self.Valves(
            **{k: os.getenv(k, v.default) for k, v in self.Valves.model_fields.items()}
        )
        self.client = None
        self.file_ids = {}  # Map file_path to file_id
        self.retry_attempts = 3

    async def setup_assistant(self):
        if not self.valves.OPENAI_API_KEY:
            logger.warning(
                "OpenAI API key is not set. Please set the OPENAI_API_KEY environment variable. The pipeline will not be able to answer questions."
            )
            return
        
        if not self.client:
            self.client = AsyncOpenAI(api_key=self.valves.OPENAI_API_KEY)

        # The CSV_DIR is relative to the `pipelines` directory
        csv_dir_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), self.valves.CSV_DIR
        )
        if not os.path.exists(csv_dir_path):
            os.makedirs(csv_dir_path)
            
        # Combine all CSVs into one
        all_csv_path = os.path.join(csv_dir_path, "all.csv")

        csv_files = glob.glob(os.path.join(csv_dir_path, "*.csv"))
        if not csv_files:
            logger.warning(
                f"No CSV files found in {csv_dir_path}. The pipeline will not be able to answer questions."
            )
            # We still proceed to setup assistant if needed, but without files it might be useless for data analysis

        dfs = []
        for file_path in csv_files:
            if os.path.basename(file_path) != "all.csv":
                try:
                    dfs.append(pd.read_csv(file_path))
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
        
        # Enrich all.csv with keywords from docs/*.json files
        if dfs:
            df = pd.concat(dfs, ignore_index=True)

            # Load keywords from docx json files and fill to the corresponding cell in dataframe
            doc_dir_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), self.valves.DOC_DIR
            )
            if os.path.exists(doc_dir_path):
                json_files = glob.glob(os.path.join(doc_dir_path, "*.json"))
                json_files.sort(key=os.path.getctime)
                if json_files:
                    if "核心關鍵詞組" not in df.columns:
                        df["核心關鍵詞組"] = ""
                    
                    for json_file in json_files:
                        try:
                            # Filename format: $GUID_$編號.docx/pdf.json
                            # e.g., 123e4567-e89b-12d3-a456-426614174000_42.docx.json
                            filename = os.path.basename(json_file)
                            if filename.endswith(".json"):
                                num_str = filename[-20:].split("_")[-1].split(".")[0]
                                logger.info(f"Extracted keywords from JSON file {filename} for 編號 {num_str} 計畫")
                                if num_str.isdigit():
                                    num = int(num_str)
                                    if "編號" in df.columns:
                                        row_masks = df["編號"] == num
                                        if row_masks.any():
                                            with open(json_file, "r", encoding="utf-8") as f:
                                                data = json.load(f)
                                            keywords = data.get("核心關鍵詞組")
                                            if keywords:
                                                df.loc[row_masks, "核心關鍵詞組"] = "、".join(keywords) if isinstance(keywords, list) else str(keywords)
                        except Exception as e:
                            logger.error(f"Error processing JSON file {json_file}: {e}")

            df.to_csv(all_csv_path, index=False)

        
        if os.path.exists(all_csv_path):
            # Upload all.csv to OpenAI
            try:
                logger.info(f"Uploading {all_csv_path} to OpenAI...")
                with open(all_csv_path, "rb") as f:
                    file_obj = await self.client.files.create(
                        file=f,
                        purpose="assistants"
                    )
                    self.file_ids[all_csv_path] = file_obj.id
                    logger.info(f"Uploaded file {all_csv_path} as id = {file_obj.id} is ready for ADA API")
            except Exception as e:
                logger.error(f"Failed to upload {all_csv_path}: {e}")

    async def on_startup(self):
        """
        Initializes the OpenAI Assistant on server startup.
        """
        logger.debug(f"on_startup:{self.name}")
        if self.valves.OPENAI_API_KEY:
            self.client = AsyncOpenAI(api_key=self.valves.OPENAI_API_KEY)
            await self.setup_assistant()

    async def on_shutdown(self):
        logger.debug(f"on_shutdown:{self.name}")
        pass

    def check_duplicate_file(self, dir_path: str, content: bytes, pattern: str = "*.csv") -> tuple[bool, str]:
        file_hash = hashlib.sha256(content).hexdigest()
        for existing_file in glob.glob(os.path.join(dir_path, pattern)):
            if os.path.getsize(existing_file) == len(content):
                with open(existing_file, "rb") as f:
                    if hashlib.sha256(f.read()).hexdigest() == file_hash:
                        file_name = os.path.basename(existing_file)
                        logger.info(f"Duplicate file content found in {file_name}, skipping download.")
                        return True, file_name
        return False, ""

    async def process_csv_file(self, file_id: str, file_name: str, url_path: str) -> bool:
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
            is_duplicate, _ = self.check_duplicate_file(csv_dir_path, content, "*.csv")
            if not is_duplicate:
                save_name = "_".join([file_id, os.path.basename(file_name)])
                with open(os.path.join(csv_dir_path, save_name), "wb") as f:
                    f.write(content)
                logger.info(f"Downloaded {url} to {os.path.join(csv_dir_path, save_name)}")
                
                return True
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
        
        return False


    async def process_doc_file(self, file_id: str, file_name: str, url_path: str) -> bool:
        doc_dir_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), self.valves.DOC_DIR
        )
        if not os.path.exists(doc_dir_path):
            os.makedirs(doc_dir_path)

        try:
            headers = {}
            if self.valves.OPEN_WebUI_API_KEY:
                headers["Authorization"] = f"Bearer {self.valves.OPEN_WebUI_API_KEY}"
            # OpenWebUI API for the uploaded file raw content
            url = f"{self.valves.OPEN_WebUI_Host}{url_path}/content"
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            content = response.content
            ptn_ext_fname = "*.docx" if file_name.endswith(".docx") else "*.pdf"
            is_duplicate, save_name = self.check_duplicate_file(doc_dir_path, content, ptn_ext_fname)
            if not is_duplicate:
                save_name = "_".join([file_id, os.path.basename(file_name)])
                file_path = os.path.join(doc_dir_path, save_name)
                with open(file_path, "wb") as f:
                    f.write(content)
                logger.info(f"Downloaded {url} to {file_path}")

            if not self.client and self.valves.OPENAI_API_KEY:
                self.client = AsyncOpenAI(api_key=self.valves.OPENAI_API_KEY)

            if self.client:
                json_path = os.path.join(doc_dir_path, f"{save_name}.json")
                if os.path.exists(json_path):
                    logger.info(f"Keywords JSON already exists at {json_path}, skipping generation.")
                    return False
                
                # OpenWebUI API for the uploaded file metadata and processed content in JSON
                # The response JSON structure:
                # {
                #   "id": "", "user_id": "", "hash": "", "filename": "", "path": "",
                #   "data": {
                #     "status": "",
                #     "content": ""
                #   },
                #   "meta": { "name": "", "content_type": "", "size": "", "data": {}, "collection_name": "" },
                #   "access_control": "", "created_at": "", "updated_at": ""
                # }
                url = f"{self.valves.OPEN_WebUI_Host}{url_path}"
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                raw_json = json.loads(response.content)
                text_content = raw_json.get("data", {}).get("content", "")
                snippet = text_content[:50].replace('\n', '\\n')
                logger.info(f"Extracting text content from {save_name}: {snippet}")

                logger.info(f"Extracting keywords for file {save_name} via OpenAI API ...")
                keywords_json = "{}"
                for attempt in range(self.retry_attempts):
                    try:
                        response = await self.client.responses.create(
                            model=self.valves.OPENAI_MODEL,
                            instructions=self.valves.SYSTEM_PROMPT_KEYWORD,
                            input=[{"role": "user", 
                                    "content": text_content[:20000]}],
                            timeout=60
                        )
                        keywords_json = response.output_text
                        logger.info(f"Received keywords JSON of {save_name}: {keywords_json[:100]}")

                        break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt+1}/{self.retry_attempts} failed for {save_name}: {e}")
                        if attempt == self.retry_attempts - 1:
                            raise e
                        await asyncio.sleep(2)
                
                with open(json_path, "w") as f:
                    f.write(keywords_json)
                logger.info(f"Keywords saved to {json_path}")
                return True

        except Exception as e:
            logger.error(f"Error processing doc file: {e}")
        
        return False

    async def inlet(self,  body: dict, __user__: Optional[dict] = None) -> dict:
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
                    if await self.process_csv_file(id, name, url_path):
                        files_added = True

                elif url_path and name.endswith((".docx", ".pdf")):
                    if await self.process_doc_file(id, name, url_path):
                        files_added = True
            
            if files_added:
                await self.setup_assistant()
            
            messages = body.get("messages", [])
            _, nosys_messages = pop_system_message(messages)
            logger.info(f"messages without system part: {nosys_messages}")
            
            if not get_last_user_message(messages):
                return body
            
            rs = ""
            try:
                if not self.client:
                    rs = "Assistant not initialized. Please ensure OPENAI_API_KEY is set."
                else:
                    # Use Responses API
                    csv_dir_path = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)), self.valves.CSV_DIR
                    )
                    csv_files = glob.glob(os.path.join(csv_dir_path, "all.csv"))

                    csv_file_ids = []
                    all_csv_path = os.path.join(csv_dir_path, "all.csv")
                    fid = self.file_ids.get(all_csv_path)
                    if fid:
                        csv_file_ids.append(fid)
                        logger.info(f"Using file IDs for analysis: {fid} from {all_csv_path}")
                        
                    msgs = [{"role": m["role"], "content": m["content"]} for m in nosys_messages]
                    logger.info(f"messages: {msgs}")

                    response = await self.client.responses.create(
                        model=self.valves.OPENAI_MODEL,
                        tools=[{
                            "type": "code_interpreter",
                            "container": {
                                "type": "auto",
                                "file_ids": csv_file_ids
                            }
                        }],
                        instructions="You are a helpful assistant proficient in data analysis. You have access to CSV files. Use the code_interpreter tool to analyze the data and answer user questions.",
                        input=msgs,
                    )
                    rs = response.output_text
                        
            except Exception as e:
                logger.error(f"Error during OpenAI chat: {e}")
                rs = f"Sorry, I encountered an error while processing your request: {e}"
            
            body["response"] = rs

        return body

    def pipe(
        self, 
        user_message: str, 
        model_id: str, 
        messages: List[dict], 
        body: dict,
    ) -> Union[str, Generator, Iterator]:
        return body.get("response", "")
