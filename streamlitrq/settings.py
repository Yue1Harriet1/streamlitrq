import os
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chat_models import AzureChatOpenAI
import openai


HOME_DIR = ""
BASE_LOG_DIR = os.path.join(HOME_DIR, "logs")
DEFAULT_LOG_DIR_OUT = f"{BASE_LOG_DIR}/stdout.txt"
APP_ENGINE_PATH = f"sqlite:///{HOME_DIR}/process_data.db"

#Execution frequencies
IMMEDIATE_FREQUENCY = "Once"
INTERVAL_FREQUENCY = "Interval"
DAILY_FREQUENCY = "Daily"

OPENAI_EMBEDDING_MODEL = "text-embedding-ada-002"
OPENAI_GPT_MODEL = "gpt-3.5-turbo"
ENGINE = ""
OPENAI_API_BASE = ""
OPENAI_API_TYPE = ""
OPENAI_API_KEY = ""
openai.api_version = "2023-03-15-preview"
openai.api_key = OPENAI_API_KEY

#LLM = AzureChatOpenAI(deployment_name=ENGINE, 
#					model_name=OPENAI_GPT_MODEL,
#					openai_api_base=OPENAI_API_BASE,
#					openai_api_version=openai.api_version,
#					openai_api_key=OPENAI_API_KEY)
#EMBEDDINGS = OpenAIEmbeddings(openai_api_key=openai.api_key, model=OPENAI_EMBEDDING_MODEL, chunk_size=1)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FORMAT = {
    "id": [],
    "created": [],
    "process_id": [],
    "job_name": [],
    "job_id": [],
    "task": [],
    "last_updated_time": [],
    "status": [],
}
WEEK_DAYS = {
    0: "Mon",
    1: "Tue",
    2: "Wed",
    3: "Thu",
    4: "Fri",
    5: "Sat",
    6: "Sun"
}

IMMEDIATE_FREQUENCY = 'Once'
INTERVAL_FREQUENCY = "Interval"
DAILY_FREQUENCY = "Daily"