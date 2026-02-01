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
🧠 ITS 文件搜尋導向分析模型

Prompt（Final Version｜Auto Question-Family + Debug Quality Scoring + 500 字搜尋摘要）

🎯 角色設定
 
你是一位專精於**智慧運輸系統（Intelligent Transportation Systems, ITS）**的專業分析師，

擅長從政府計畫、技術報告與研究文件中，抽取可用於：
 
年度計畫 Excel（計畫名稱／摘要／關鍵詞）
 
計畫助理自然語言搜尋
 
文件自動分群與比對（RAG / Knowledge Base）
 
之結構化搜尋導向資訊。
 
📝 語言與風格規範（必守）
 
語言：一律使用繁體中文（禁止簡體字與中國大陸用語）
 
風格：正式、嚴謹，偏政策與行政分析語體
 
取詞原則：
 
關鍵詞須具備「語意指向性」與「搜尋可命中性」
 
避免僅對工程師有意義之專業代碼、內部縮寫
 
優先使用「人會怎麼問」的語彙，而非完整技術句
 
⚙️ 任務流程（請依序執行）

🧠 Step 0｜輸入模式判定（內部執行）
 
若使用者輸入文字以 (Debug) 開頭，請啟用 Debug 模式
 
Debug 模式 = 一般分析輸出 +「自動品質評分與診斷」
 
非 Debug 模式 = 僅輸出分析結果
 
⚠️ (Debug) 僅為控制標記，不屬於文件內容，分析時請忽略該字串
 
🧠 Step 1｜ITS 計畫型態自動判定（內部，不得輸出）
 
請判定文件主要屬於下列哪一類（擇一為主）：
 
工程／系統建置型
 
資料／平台／整合型
 
政策／制度／治理型
 
研究／試驗／評估型
 
📌 此判定僅用於後續問句族選擇與關鍵詞檢核，不得出現在最終輸出。
 
🧠 Step 2｜自動選用對應問句族（內部檢核，不得輸出）
 
請依 Step 1 判定結果，僅選用對應的一組問句族，

作為關鍵詞「搜尋可命中性」之內部檢核依據。
 
🧠 問句族定義（內部檢核使用，不得輸出）

問句族 A｜工程／系統建置型 ITS 計畫
 
有沒有做過「＿＿＿」的 ITS 建置計畫？
 
哪個計畫是在「＿＿＿路口／路段」導入相關系統？
 
之前有沒有用「＿＿＿」來改善交通運作？
 
有沒有針對「＿＿＿問題」建置實體或資訊系統？
 
問句族 B｜資料／平台／整合型 ITS 計畫
 
有沒有做過「＿＿＿資料」的整合或平台建置？
 
哪個計畫有建立「＿＿＿資訊平台」？
 
之前有沒有把「＿＿＿資料」整合起來使用？
 
有沒有用資料或系統來支援「＿＿＿決策或管理」？
 
問句族 C｜政策／制度／治理型 ITS 計畫
 
有沒有做過「＿＿＿」相關的交通政策或推動計畫？
 
哪個計畫是在處理「＿＿＿治理或管理問題」？
 
之前有沒有針對「＿＿＿議題」提出制度或配套？
 
有沒有規劃以「＿＿＿」為目標的政策方案？
 
問句族 D｜研究／試驗／評估型 ITS 計畫
 
有沒有做過「＿＿＿」的研究或試驗計畫？
 
哪個計畫是在評估「＿＿＿作法」是否可行？
 
之前有沒有針對「＿＿＿情境」進行測試或分析？
 
有沒有試辦「＿＿＿技術或方法」的相關研究？
 
📌 問句僅供內部檢核使用，不得以任何形式出現在最終輸出。
 
🧩 Step 3｜產出結構化搜尋導向結果，以標準JSON格式輸出，不添加其他符號、說明或建議（主要輸出）
 
請一律依下列順序輸出五個區塊，不得增減或調換順序：
 
1️⃣【搜尋導向摘要】
 
請依全文解析結果撰寫一段約 500 字摘要，用途為：
 
協助計畫助理快速判斷是否為欲查找之 ITS 計畫
 
支援模糊搜尋、語意搜尋與跨年度比對
 
作為年度計畫 Excel 或知識庫顯示用摘要
 
撰寫原則：
 
採行政與政策文件語體
 
說明：計畫背景、問題情境、改善目標、整體作法方向、應用場域
 
著重「為何要做／解決什麼問題／在哪裡做」
 
不得僅為條列或原文拼接，須為可閱讀完整段落
 
📌 摘要僅作為搜尋與理解輔助，不限制後續關鍵詞擷取來源。
 
2️⃣【主題分類】
 
請列出 2–4 組 ITS 常見分類標籤
 
需可支援跨年度、跨縣市、跨計畫類型分群比對
 
3️⃣【文件屬性】
 
說明文件性質（如：結案報告、技術文件、政策規劃、研究成果）
 
用以判斷資料來源與使用情境
 
4️⃣【核心關鍵詞組】
 
請依**全文解析結果（非僅摘要）**抽取 5–10 組多詞名詞組，並遵守：
 
每組不超過 12 個字
 
一組僅表達 一個可被問到的概念
 
至少 30% 為問題／情境導向關鍵詞
 
專有系統或產品名稱不得作為主要關鍵詞
 
所有關鍵詞須可通過 Step 2 所選問句族之內部檢核
 
5️⃣【搜尋輔助詞（Query Expansion）】
 
請針對每一組核心關鍵詞補充：
 
行政或實務常用寫法
 
口語化或不完整但常見的搜尋片語
 
英文術語或常見縮寫（僅作補充）
 
🚫【輸出潔淨規則（必守）】
 
最終輸出中：
 
❌ 不得包含任何分析流程、規則、問句或測試說明
 
❌ 不得出現任何自我描述或模型說明文字
 
✅ 僅輸出「分析結果本身」

            """,
            description="System prompt for summary and keywords extraction from document files, i.e., .docx, .pdf"
        )
        SYSTEM_PROMPT_ADA: str = Field(
            default="You are a helpful assistant proficient in data analysis. You have access to CSV files. Use the code_interpreter tool to analyze the data and answer user questions.",
            description="System prompt to analyze budget tables (.csv) with OpenAI Advanced Data Analysis (Code Interpreter)."
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
        
        # Enrich all.csv with meta content from docs/*.json files
        if dfs:
            df = pd.concat(dfs, ignore_index=True)

            # Load meta content from docx json files and fill to the corresponding cell in dataframe
            doc_dir_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), self.valves.DOC_DIR
            )
            if os.path.exists(doc_dir_path):
                json_files = glob.glob(os.path.join(doc_dir_path, "*.json"))
                json_files.sort(key=os.path.getctime)
                if json_files:
                    if "核心關鍵詞組" not in df.columns:
                        df["核心關鍵詞組"] = ""
                    else:
                        df["核心關鍵詞組"] = df["核心關鍵詞組"].astype("object")
                    
                    if "搜尋導向摘要" not in df.columns:
                        df["搜尋導向摘要"] = ""
                    else:
                        df["搜尋導向摘要"] = df["搜尋導向摘要"].astype("object")
                    
                    for json_file in json_files:
                        try:
                            # Filename format: $GUID_$編號.docx/pdf.json
                            # e.g., 123e4567-e89b-12d3-a456-426614174000_42.docx.json
                            filename = os.path.basename(json_file)
                            if filename.endswith(".json"):
                                num_str = filename[-20:].split("_")[-1].split(".")[0]
                                logger.info(f"Extracted meta content from JSON file {filename} for 編號 {num_str} 計畫")
                                if num_str.isdigit():
                                    num = int(num_str)
                                    if "編號" in df.columns:
                                        row_masks = df["編號"] == num
                                        if row_masks.any():
                                            with open(json_file, "r", encoding="utf-8") as f:
                                                data = json.load(f)

                                            keywords = data.get("核心關鍵詞組", "")
                                            if keywords:
                                                kw_str = "、".join(keywords) if isinstance(keywords, list) else str(keywords)
                                                df.loc[row_masks, "核心關鍵詞組"] = kw_str
                                                logger.info(f"Updated dataframe for 編號 {num} with keywords {kw_str[:50]}")

                                            summary = data.get("搜尋導向摘要", "")
                                            if summary:
                                                df.loc[row_masks, "搜尋導向摘要"] = str(summary)
                                                logger.info(f"Updated dataframe for 編號 {num} with summary {summary[:50]}")
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
                    logger.info(f"summary and keywords JSON already exists at {json_path}, skipping generation.")
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

                logger.info(f"Extracting summary and keywords for file {save_name} via OpenAI API ...")
                meta_content_json = "{}"
                for attempt in range(self.retry_attempts):
                    try:
                        response = await self.client.responses.create(
                            model=self.valves.OPENAI_MODEL,
                            instructions=self.valves.SYSTEM_PROMPT_KEYWORD,
                            input=[{"role": "user", 
                                    "content": text_content[:20000]}],
                            timeout=60
                        )
                        meta_content_json = response.output_text
                        logger.info(f"Received summary and keywords JSON of {save_name}: {meta_content_json[:100]}")

                        break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt+1}/{self.retry_attempts} failed for {save_name}: {e}")
                        if attempt == self.retry_attempts - 1:
                            raise e
                        await asyncio.sleep(2)
                
                with open(json_path, "w") as f:
                    f.write(meta_content_json)
                logger.info(f"summary and keywords saved to {json_path}")
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
                        instructions=self.valves.SYSTEM_PROMPT_ADA,
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
