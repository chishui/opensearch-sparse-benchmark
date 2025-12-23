import json
from benchmark.basic import client
from benchmark.basic.util import wait_task, trace, swallow_exceptions

class Index:
    def __init__(self, index):
        self.index = index

    @trace
    @swallow_exceptions
    def delete(self):
        return client.http.delete(f"/{self.index}")
    
    @trace
    def forcemerge(self, max_segment_count=1):
        return client.http.post(f"/{self.index}/_forcemerge?max_num_segments={max_segment_count}")

    @trace
    def count(self):
        return client.http.get(f"/_cat/count/{self.index}")
    
    @trace
    def segments(self):
        return client.http.get(f"/{self.index}/_segments")

    @trace
    def bind_ingest_pipeline(self, pipeline_name):
        """Bind an ingest pipeline to this index."""
        body = {
            "index.default_pipeline": pipeline_name
        }
        return client.http.put(f"/{self.index}/_settings", body=body)

    @trace
    def bind_search_pipeline(self, pipeline_name):
        """Bind a search pipeline to this index."""
        body = {
            "index.search.default_pipeline": pipeline_name
        }
        return client.http.put(f"/{self.index}/_settings", body=body)

    @trace
    def create(self, payload):
        return client.http.put(f"/{self.index}", body=payload)
