import json

file_path = '/home/ubuntu/data/msmarco_passage_embedding.txt'

def ms_marco_v2_generator():
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def doc_generator():
    for idx, record in enumerate(ms_marco_v2_generator()):
        # Use the index as the document ID
        # Transform the record into the document format expected by OpenSearch
        doc = {"passage_embedding": record.get("passage_sparse", ""), "id": idx}
        yield (idx, doc)