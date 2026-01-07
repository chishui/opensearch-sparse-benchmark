# OpenSearch Sparse Benchmark

A multi-threaded benchmarking tool for OpenSearch with support for bulk ingestion, search workloads, and neural sparse search evaluation with recall metrics.

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Set up your OpenSearch connection by creating a `.env` file in `benchmark/basic/`:

```env
OPENSEARCH_HOST=localhost
OPENSEARCH_PORT=9200
OPENSEARCH_USER=admin
OPENSEARCH_PASSWORD=admin
```

## Usage

### Run a workload

```bash
python -m benchmark.main run -w workloads/msmarco_v1_100k
```

### With parameters

```bash
python -m benchmark.main run -w workloads/msmarco_v1_100k -p index=my-index -p clients=8
```

### Skip specific tasks

```bash
python -m benchmark.main run -w workloads/msmarco_v1_100k -s create-index -s ingest
```

## Workload Structure

Each workload is a directory containing:

- `config.yml` - Task definitions and parameters
- `index.json` - Index mapping (optional)
- `generator.py` - Document/query generator script (optional)

### Example config.yml

```yaml
parameters:
  index: test-index

tasks:
  - name: create-index
    payload: index.json
    delete: true
  - name: ingest
    script: generator.py
  - name: search
    script: search_generator.py
    recall:
      truth_file: /path/to/ground_truth.txt
      size: 10
```

## Task Types

- `create-index` - Create an OpenSearch index
- `create-ingest-pipeline` - Create an ingest pipeline
- `create-search-pipeline` - Create a search pipeline
- `ingest` - Bulk ingest documents (multi-threaded)
- `search` - Execute search queries (multi-threaded) with optional recall evaluation
- `request` - Generic HTTP request

## Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `index` | Target index name | required |
| `clients` | Number of concurrent workers | 4 |
| `bulk_size` | Documents per bulk request | 100 |
| `total_count` | Total documents to process | all |
| `queue_size` | Internal queue size | 100 |
| `max_retries` | Max retry attempts | 3 |

## Search Recall Evaluation

The benchmark tool supports automatic recall evaluation for search tasks. When enabled, it compares search results against ground truth data to calculate recall metrics.

### Configuration

Add a `recall` section to your search task configuration:

```yaml
- name: search
  script: search_generator.py
  recall:
    truth_file: /path/to/ground_truth.txt
    size: 10
```

### Parameters

- `truth_file` - Path to ground truth file containing relevant document IDs (comma-separated format)
- `size` - Number of top results to evaluate (e.g., recall@10)

### Output

When recall evaluation is enabled, the benchmark will output:

```
🎯 Search Recall:
   0.85
```

The recall score represents the fraction of relevant documents found in the top-k results, averaged across all queries.

## Neural Sparse Search Support

The benchmark includes built-in support for neural sparse search queries with CSR (Compressed Sparse Row) matrix format:

- Reads sparse query vectors from `.csr` files
- Converts sparse vectors to OpenSearch neural_sparse query format
- Supports configurable search parameters (top_n, heap_factor, k)

### Example Neural Sparse Query

The tool automatically generates queries in this format:

```json
{
    "_source": false,
    "query": {
        "neural_sparse": {
            "passage_embedding": {
                "query_tokens": {"123": 0.5, "456": 0.8},
                "method_parameters": {
                    "top_n": 3,
                    "heap_factor": 1.2,
                    "k": 10
                }
            }
        }
    },
    "size": 10
}
```
