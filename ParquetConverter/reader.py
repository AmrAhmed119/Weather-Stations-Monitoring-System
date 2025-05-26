import os
import pandas as pd
import json
import time
import requests

# === CONFIGURATION ===
BASE_DIR = "/app/parquet/"
INDEX_NAME = "weather"
ES_URL = f"http://elasticsearch:9200/{INDEX_NAME}/_bulk"

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
            meta = {"index": {}}
            doc = row.to_dict()
            ndjson_lines.append(json.dumps(meta))
            ndjson_lines.append(json.dumps(doc))
        
        ndjson_payload = "\n".join(ndjson_lines) + "\n"

        # Send to Elasticsearch
        response = requests.post(
            ES_URL,
            headers={"Content-Type": "application/x-ndjson"},
            data=ndjson_payload.encode('utf-8')
        )

        if response.status_code == 200:
            result = response.json()
            if result.get("errors"):
                print(f"[{filepath}] Some documents failed to index. File NOT deleted.")
                return False
            else:
                print(f"[{filepath}] All documents indexed successfully.")
                try:
                    os.remove(filepath)
                    print(f"[{filepath}] File deleted successfully.")
                    return True
                except OSError as e:
                    print(f"[{filepath}] Error deleting file: {e}")
                    return False
        else:
            print(f"[{filepath}] Failed to index. Status code: {response.status_code}")
            print(response.text)
            return False

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return False

def traverse_and_process():
    for root, dirs, files in os.walk(BASE_DIR):
        for file in files:
            if file.endswith(".parquet"):
                filepath = os.path.join(root, file)
                success = process_parquet_file(filepath)
                if not success:
                    # Optionally move failed files to a different directory
                    pass

if __name__ == "__main__":
    while True:
        traverse_and_process()
        print("Sleeping for 30 seconds...")
        time.sleep(30)