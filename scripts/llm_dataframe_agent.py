import pandas as pd
import sys
import json
from pandasai import SmartDataframe
from pandasai.llm import Ollama

csv_path = sys.argv[1]
user_question = sys.argv[2]

df = pd.read_csv(csv_path)
llm = Ollama(model="llama3:8b")
sdf = SmartDataframe(df, config={"llm": llm})

try:
    result = sdf.chat(user_question)
    print(json.dumps({"answer": result}))
except Exception as e:
    print(json.dumps({"error": str(e)}))

