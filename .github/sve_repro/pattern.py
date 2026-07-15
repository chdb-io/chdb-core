import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import sys

# For each dictionary-index bit width, write a known sequence and diff the read-back.
print("pyarrow", pa.__version__, flush=True)
try:
    import ctypes
    PR_SVE_GET_VL = 51
    vl = ctypes.CDLL(None, use_errno=True).prctl(PR_SVE_GET_VL, 0, 0, 0, 0)
    print(f"SVE vector length: {vl & 0xFFFF} bytes ({(vl & 0xFFFF) * 8} bits)", flush=True)
except Exception as e:
    print("SVE VL probe failed:", e, flush=True)
N = 4096
any_bad = False
for width in range(1, 21):
    card = 1 << width  # cardinality forcing 'width'-bit indices (max index = card-1)
    idx = np.arange(N, dtype=np.int64) % card
    dict_vals = pa.array([f"v{i:06d}" for i in range(card)])
    col = pa.DictionaryArray.from_arrays(pa.array(idx, type=pa.int32()), dict_vals)
    t = pa.table({"c": col})
    path = f"/tmp/w{width}.parquet"
    pq.write_table(t, path, use_dictionary=True, compression="none")
    try:
        back = pq.read_table(path, use_threads=False)
        got = back["c"].combine_chunks().indices.to_numpy()
        exp = idx.astype(got.dtype)
        bad = np.nonzero(got != exp)[0]
        if len(bad):
            any_bad = True
            first = bad[:8]
            print(f"width={width:2d}: {len(bad):5d}/{N} wrong; first bad pos={list(first)}; "
                  f"got={list(got[first])} exp={list(exp[first])}", flush=True)
        else:
            print(f"width={width:2d}: OK", flush=True)
    except Exception as e:
        any_bad = True
        print(f"width={width:2d}: EXCEPTION {type(e).__name__}: {e}", flush=True)
sys.exit(2 if any_bad else 0)
