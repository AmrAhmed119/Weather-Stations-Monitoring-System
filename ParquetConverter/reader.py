import os
import pandas as pd
import json
import time

# === CONFIGURATION ===
BASE_DIR = "/app/parquet/"
INDEX_NAME = "weather"

def process_parquet_file(filepath):
    try:
        df = pd.read_parquet(filepath)
        
        # Flatten 'weather' column if it exists
        if 'weather' in df.columns:
            weather_df = df['weather'].apply(pd.Series)
            df = pd.concat([df.drop(columns=['weather']), weather_df], axis=1)

        # Convert to NDJSON format
        ndjson_lines = []
        for _, row in df.iterrows():
            meta = {"index": {"_index": INDEX_NAME}}
            doc = row.to_dict()
            ndjson_lines.append(json.dumps(meta))
            ndjson_lines.append(json.dumps(doc))
        
        ndjson_payload = "\n".join(ndjson_lines) + "\n"
        
        # === Replace actual sending with print ===
        print(f"[{filepath}] NDJSON payload:\n{ndjson_payload}")

        # === Commented out Elasticsearch logic ===
        # response = requests.post(
        #     ES_URL,
        #     headers={"Content-Type": "application/x-ndjson"},
        #     data=ndjson_payload.encode('utf-8')
        # )
        #
        # if response.status_code == 200:
        #     result = response.json()
        #     if result.get("errors"):
        #         print(f"[{filepath}] Some documents failed to index.")
        #     else:
        #         print(f"[{filepath}] All documents indexed successfully.")
        # else:
        #     print(f"[{filepath}] Failed to index. Status code: {response.status_code}")
        #     print(response.text)

    except Exception as e:
        print(f"Error processing {filepath}: {e}")

def traverse_and_process():
    for root, dirs, files in os.walk(BASE_DIR):
        for file in files:
            if file.endswith(".parquet"):
                filepath = os.path.join(root, file)
                process_parquet_file(filepath)

if __name__ == "__main__":
    while True:
        traverse_and_process()
        print("Sleeping for 30 seconds...")
        time.sleep(30)
