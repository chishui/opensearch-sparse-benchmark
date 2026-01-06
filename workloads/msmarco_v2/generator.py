import json

file_path = '/Users/xiliyun/projects/opensearch-sparse-benchmark/data/msmarco_passage_69'

def ms_marco_v2_generator():
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def doc_generator(**kwargs):
    total_count = kwargs.get('total_count')
    for idx, record in enumerate(ms_marco_v2_generator()):
        if total_count and idx >= total_count:
            break
        # Use the index as the document ID
        # Transform the record into the document format expected by OpenSearch
        doc = {"text": record.get("passage", "")}
        yield (idx, doc)