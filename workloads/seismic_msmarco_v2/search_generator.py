import json
from scipy.sparse import csr_matrix, vstack
import numpy as np

file_path = '/home/ubuntu/data/queries.dev.csr'

def read_sparse_matrix_fields(fname):
    """read the fields of a CSR matrix without instanciating it"""
    with open(fname, "rb") as f:
        sizes = np.fromfile(f, dtype="int64", count=3)
        nrow, ncol, nnz = sizes
        indptr = np.fromfile(f, dtype="int64", count=nrow + 1)
        assert nnz == indptr[-1]
        indices = np.fromfile(f, dtype="int32", count=nnz)
        assert np.all(indices >= 0) and np.all(indices < ncol)
        data = np.fromfile(f, dtype="float32", count=nnz)
        return data, indices, indptr, ncol


def read_sparse_matrix(fname):
    """read a CSR matrix in spmat format, optionally mmapping it instead"""
    data, indices, indptr, ncol = read_sparse_matrix_fields(fname)

    return csr_matrix((data, indices, indptr), shape=(len(indptr) - 1, ncol))

def sparse_vector_to_json(csr_matrix, row_idx=0):

    # Get the first row as a sparse vector
    vector = csr_matrix[row_idx]
    
    # Get nonzero elements and their indices
    indices = vector.indices
    data = vector.data
    
    # Create dictionary with string keys
    result = {str(int(idx)): float(val) for idx, val in zip(indices, data)}
    
    # Convert to JSON
    return json.dumps(result)

template = """
{
    "_source": false,
    "query": {
        "neural_sparse": {
            "passage_embedding": {
                "query_tokens": {{embedding}},
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
"""

def doc_generator(**kwargs):
    global template
    X = read_sparse_matrix(file_path)
    size = kwargs.get('total_count', X.shape[0])
    size = min(size, X.shape[0])
    for i in range(0, size):
        vec = sparse_vector_to_json(X[i % X.shape[0]])
        payload = template.replace("{{embedding}}", vec)
        yield (i, payload)
