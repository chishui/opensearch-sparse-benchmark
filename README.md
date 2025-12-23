# OpenSearch Sparse Benchmark

A multi-threaded benchmarking tool for OpenSearch with support for bulk ingestion and search workloads.

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
```

## Task Types

- `create-index` - Create an OpenSearch index
- `create-ingest-pipeline` - Create an ingest pipeline
- `create-search-pipeline` - Create a search pipeline
- `ingest` - Bulk ingest documents (multi-threaded)
- `search` - Execute search queries (multi-threaded)
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
