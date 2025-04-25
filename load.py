import pickle
import json
import requests
import time
import os
from urllib.parse import urljoin
import base64

# REST API configuration for Elasticsearch
es_host = os.environ.get("ES_HOST", "chat.lexin.cs.ui.ac.id")
# es_port = os.environ.get("ES_PORT", "80")
es_base_url = f"https://{es_host}"
es_auth = base64.b64encode(b"elastic:password").decode('ascii')
es_headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {es_auth}"
}

print(f"Connecting to Elasticsearch REST API at {es_base_url}...")

# Try connecting with retry logic
max_retries = 5
retry_delay = 5  # seconds

for attempt in range(max_retries):
    try:
        # Simple ping request to check if Elasticsearch is available
        response = requests.get(
            urljoin(es_base_url, "/"),
            headers=es_headers,
            timeout=30
        )
        
        if response.status_code == 200:
            print("Successfully connected to Elasticsearch REST API")
            print(f"Elasticsearch info: {response.json()['version']['number']}")
            break
        else:
            print(f"Elasticsearch connection failed with status code {response.status_code}.")
            print(f"Response: {response.text}")
            print(f"Retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
    except Exception as e:
        print(f"Connection error: {e}. Retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
    
    if attempt < max_retries - 1:
        time.sleep(retry_delay)
else:
    print("Could not connect to Elasticsearch after multiple attempts. Make sure it's running.")
    print("If using Docker, ensure Docker container is running and ports are properly mapped.")
    print("Try setting environment variables: ES_HOST=<docker-container-name> ES_PORT=<exposed-port>")
    exit(1)

# Load data
items = []
with open("items.pkl", "rb") as f:
    items = pickle.load(f)

# Define a function to preprocess documents before indexing
def preprocess_document(doc):
    """Preprocess document to ensure all fields are properly formatted for Elasticsearch"""
    processed_doc = doc.copy()
    
    # Handle dates in metadata
    date_fields = ["Tanggal Penetapan", "Tanggal Pengundangan", "Tanggal Berlaku"]
    id_month_mapping = {
        "Januari": "January",
        "Februari": "February",
        "Maret": "March",
        "April": "April",
        "Mei": "May",
        "Juni": "June",
        "Juli": "July",
        "Agustus": "August",
        "September": "September",
        "Oktober": "October",
        "November": "November",
        "Desember": "December"
    }
    
    if "metadata" in processed_doc:
        for date_field in date_fields:
            # If the field exists but is empty, null, or not a string, remove it
            if date_field in processed_doc["metadata"]:
                date_value = processed_doc["metadata"][date_field]
                if date_value is None or date_value == "" or not isinstance(date_value, str):
                    processed_doc["metadata"].pop(date_field, None)
                    continue
                
                try:
                    # Convert Indonesian month names to English for parsing
                    date_str = date_value.strip()  # Remove whitespace
                    for id_month, en_month in id_month_mapping.items():
                        if id_month in date_str:
                            date_str = date_str.replace(id_month, en_month)
                    processed_doc["metadata"][date_field] = date_str
                except Exception as e:
                    # If date conversion fails, remove the field to prevent indexing errors
                    print(f"Warning: Could not parse date in field '{date_field}': {date_value}. Error: {e}")
                    processed_doc["metadata"].pop(date_field, None)
    
    return processed_doc

# Define Elasticsearch mapping for better search capabilities
es_mapping = {
    "settings": {
        "analysis": {
            "analyzer": {
                "indonesian_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "indonesian_stop", "indonesian_stemmer"]
                }
            },
            "filter": {
                "indonesian_stop": {
                    "type": "stop",
                    "stopwords": "_indonesian_"
                },
                "indonesian_stemmer": {
                    "type": "stemmer",
                    "language": "indonesian"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "metadata": {
                "properties": {
                    "Tipe Dokumen": {"type": "keyword"},
                    "Judul": {"type": "text", "analyzer": "indonesian_analyzer"},
                    "T.E.U.": {"type": "text"},
                    "Nomor": {"type": "keyword"},
                    "Bentuk": {"type": "keyword"},
                    "Bentuk Singkat": {"type": "keyword"},
                    "Tahun": {"type": "keyword"},
                    "Tempat Penetapan": {"type": "keyword"},
                    "Tanggal Penetapan": {"type": "date", "format": "d MMMM yyyy||dd MMMM yyyy||yyyy-MM-dd||strict_date_optional_time"},
                    "Tanggal Pengundangan": {"type": "date", "format": "d MMMM yyyy||dd MMMM yyyy||yyyy-MM-dd||strict_date_optional_time"},
                    "Tanggal Berlaku": {"type": "date", "format": "d MMMM yyyy||dd MMMM yyyy||yyyy-MM-dd||strict_date_optional_time"},
                    "Sumber": {"type": "text"},
                    "Subjek": {"type": "keyword"},
                    "Status": {"type": "keyword"},
                    "Bahasa": {"type": "keyword"},
                    "Lokasi": {"type": "keyword"},
                    "Bidang": {"type": "keyword"}
                }
            },
            "relations": {"type": "object"},
            "files": {
                "type": "nested",
                "properties": {
                    "file_id": {"type": "keyword"},
                    "filename": {"type": "text"},
                    "download_url": {"type": "text"},
                    "content": {"type": "text", "analyzer": "indonesian_analyzer"}
                }
            },
            "abstrak": {
                "type": "text",
                "analyzer": "indonesian_analyzer",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "catatan": {
                "type": "text",
                "analyzer": "indonesian_analyzer",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            }
        }
    }
}

# Check if index exists and delete if necessary
index_name = "peraturan_indonesia"
try:
    response = requests.head(
        urljoin(es_base_url, f"/{index_name}"),
        headers=es_headers
    )
    
    if response.status_code == 200:
        print(f"Index '{index_name}' already exists. Deleting...")
        delete_response = requests.delete(
            urljoin(es_base_url, f"/{index_name}"),
            headers=es_headers
        )
        if delete_response.status_code not in (200, 404):
            print(f"Failed to delete index: {delete_response.text}")
            exit(1)
except Exception as e:
    print(f"Error checking index existence: {e}")
    exit(1)

# Create index with mapping
print(f"Creating index '{index_name}' with custom mapping...")
try:
    create_response = requests.put(
        urljoin(es_base_url, f"/{index_name}"),
        headers=es_headers,
        json=es_mapping
    )
    
    if create_response.status_code != 200:
        print(f"Failed to create index: {create_response.text}")
        exit(1)
    else:
        print("Index created successfully")
except Exception as e:
    print(f"Error creating index: {e}")
    exit(1)

# Bulk indexing using the _bulk API
def prepare_bulk_data(batch):
    bulk_data = []
    for item in batch:
        # Preprocess the document to fix date formats and other issues
        processed_item = preprocess_document(item)
        
        # Create a document ID
        doc_id = f"{processed_item['metadata'].get('Bentuk Singkat', 'doc')}_{processed_item['metadata'].get('Nomor', '')}_{processed_item['metadata'].get('Tahun', '')}"
        
        # Add the index action and document data
        bulk_data.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}))
        bulk_data.append(json.dumps(processed_item))
    
    # Join with newlines and add final newline
    return "\n".join(bulk_data) + "\n"

# Bulk index the documents in batches
total_docs = len(items)
batch_size = 50  # Smaller batch for REST API to avoid request size limits
start_time = time.time()

print(f"Indexing {total_docs} documents in batches of {batch_size}...")

success_count = 0
for i in range(0, total_docs, batch_size):
    batch = items[i:i+batch_size]
    try:
        bulk_data = prepare_bulk_data(batch)
        
        # Add Content-Type header specifically for bulk API
        bulk_headers = es_headers.copy()
        bulk_headers["Content-Type"] = "application/x-ndjson"
        
        response = requests.post(
            urljoin(es_base_url, "/_bulk"),
            headers=bulk_headers,
            data=bulk_data,
            timeout=60  # Longer timeout for bulk operations
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Improved error reporting
            if result.get("errors", False):
                # Get detailed error information
                error_items = [item for item in result["items"] if item.get("index", {}).get("status", 500) >= 400]
                success_items = len(batch) - len(error_items)
                
                print(f"Indexed batch {i//batch_size + 1}/{(total_docs+batch_size-1)//batch_size}: {success_items}/{len(batch)} documents (with {len(error_items)} errors)")
                
                # Print detailed error information for up to 3 errors
                for idx, error_item in enumerate(error_items[:3]):
                    error_details = error_item.get("index", {})
                    error_reason = error_details.get("error", {})
                    doc_id = error_details.get("_id", "unknown")
                    
                    if isinstance(error_reason, dict):
                        error_type = error_reason.get("type", "unknown")
                        error_reason_msg = error_reason.get("reason", "No reason provided")
                        caused_by = error_reason.get("caused_by", {}).get("reason", "")
                        print(f"  Error {idx+1}: Doc ID: {doc_id}, Type: {error_type}, Reason: {error_reason_msg}")
                        if caused_by:
                            print(f"    Caused by: {caused_by}")
                    else:
                        print(f"  Error {idx+1}: Doc ID: {doc_id}, Details: {error_reason}")
                
                if len(error_items) > 3:
                    print(f"  ... and {len(error_items) - 3} more errors not shown")
                
                # Try to identify common error patterns
                error_types = {}
                for item in error_items:
                    error_type = item.get("index", {}).get("error", {}).get("type", "unknown")
                    error_types[error_type] = error_types.get(error_type, 0) + 1
                
                print("  Error summary:")
                for error_type, count in error_types.items():
                    print(f"    - {error_type}: {count} occurrences")
                
                success_count += success_items
            else:
                print(f"Indexed batch {i//batch_size + 1}/{(total_docs+batch_size-1)//batch_size}: {len(batch)} documents")
                success_count += len(batch)
        else:
            print(f"Error in batch {i//batch_size + 1}: HTTP Status {response.status_code}")
            print(f"Response: {response.text[:500]}...")  # Show first 500 chars of error
    except Exception as e:
        print(f"Error in batch {i//batch_size + 1}: {e}")
        # Add exception traceback for better debugging
        import traceback
        print(traceback.format_exc())

end_time = time.time()
elapsed_time = end_time - start_time

print(f"\nIndexing complete!")
print(f"Successfully indexed {success_count} out of {total_docs} documents")
print(f"Time taken: {elapsed_time:.2f} seconds")

# Example search query to verify the indexing
try:
    sample_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"abstrak": "peraturan"}},
                    {"match": {"files.content": "peraturan"}},
                    {"match": {"catatan": "peraturan"}}
                ]
            }
        },
        "size": 5
    }
    
    print("\nPerforming a test search for 'peraturan'...")
    search_response = requests.post(
        urljoin(es_base_url, f"/{index_name}/_search"),
        headers=es_headers,
        json=sample_query
    )
    
    if search_response.status_code == 200:
        results = search_response.json()
        print(f"Found {results['hits']['total']['value']} matching documents")
        for hit in results['hits']['hits']:
            print(f"- {hit['_source']['metadata'].get('Judul', 'No title')} (Score: {hit['_score']})")
    else:
        print(f"Search failed: {search_response.text}")
except Exception as e:
    print(f"Error during test search: {e}")