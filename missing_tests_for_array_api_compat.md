# Missing tests for array-API compatibility in hdmf

PR #1414 (collection detection) and PR #1415 (0-d ndarray handling) introduced `_is_collection`, `_get_length`, and `_unwrap_scalar` to replace duck-typing patterns that break with zarr v3. However, when tested against hdmf-zarr's full test suite, four additional call sites failed that were not covered by the original PRs.

## What failed and where

All four failures came from hdmf-zarr tests that exercise read-back and export paths, where `data` is a `zarr.Array`:

| File | Line | Call | Error |
|------|------|------|-------|
| `container.py` | 980 | `len(self.__data)` in `Data.__len__` | `TypeError: object of type 'Array' has no len()` |
| `h5tools.py` | 1403, 1405 | `len(data)` when sizing datasets during export | same |
| `objectmapper.py` | 977 | `len(data)` for compound dtype shape | same |
| `objectmapper.py` | 1069 | `list(row)` iterating compound dataset rows | `TypeError: iteration over a 0-d array` |

These were fixed in commits `7ee7ed64` (table.py), and `ec5ece4e` (container.py, h5tools.py, objectmapper.py) on the `remove_duck_typing_for_type` branch.

## Why these were missed

The original PRs found call sites by searching for `hasattr(data, "__len__")` patterns. But these four sites used `len()` directly without a `hasattr` guard, so they were invisible to that search. They also don't fail in hdmf's own test suite because hdmf tests never pass zarr Arrays as data -- they use lists, numpy arrays, and h5py datasets, all of which have `__len__` and return scalars from indexing.

## Tests that should be added

hdmf currently has no unit tests that exercise its core APIs with array-like objects that lack `__len__` or that return 0-d ndarrays from scalar indexing. The following tests would catch regressions and cover the patterns we fixed:

### 1. `Data.__len__` with a `__len__`-less array-like

Test that `Data(name, data)` where `data` has `shape` but no `__len__` still returns the correct length via `shape[0]`.

```python
class ArrayWithoutLen:
    """Minimal array-like that has shape and ndim but no __len__, like zarr v3."""
    def __init__(self, shape, dtype):
        self._data = np.zeros(shape, dtype=dtype)
        self.shape = shape
        self.ndim = len(shape)
        self.dtype = dtype

    def __getitem__(self, idx):
        return self._data[idx]

def test_data_len_without_dunder_len():
    arr = ArrayWithoutLen((5,), "f8")
    d = Data(name="test", data=arr)
    assert len(d) == 5
```

### 2. `_is_collection` and `_get_length` with array-like without `__len__`

```python
def test_is_collection_array_without_len():
    arr = ArrayWithoutLen((3,), "i4")
    assert _is_collection(arr) is True
    assert _get_length(arr) == 3

def test_is_collection_scalar_array():
    arr = ArrayWithoutLen((), "f8")
    assert _is_collection(arr) is False
```

### 3. `_unwrap_scalar` in `ElementIdentifiers._validate_new_data_element`

Test that `ElementIdentifiers` accepts data whose `__getitem__` returns 0-d ndarrays:

```python
class ArrayReturning0dNdarray(ArrayWithoutLen):
    """Array-like where __getitem__ returns 0-d ndarray (like zarr v3)."""
    def __getitem__(self, idx):
        val = self._data[idx]
        if np.ndim(val) == 0 and not isinstance(val, np.ndarray):
            return np.array(val)  # wrap scalar as 0-d ndarray
        return val

def test_element_identifiers_with_0d_ndarray_elements():
    arr = ArrayReturning0dNdarray((3,), "i8")
    arr._data[:] = [10, 20, 30]
    # should not raise "ElementIdentifiers must contain integers"
    eids = ElementIdentifiers(name="ids", data=arr)
```

### 4. `ObjectMapper` compound dtype handling with 0-d rows

Test that building a compound dataset where each row is a 0-d ndarray (structured void) works. This is harder to unit-test in isolation because it goes through the build pipeline, but a minimal test could mock `container.data` as an array whose iteration yields 0-d structured ndarrays.

### 5. `HDF5IO.__write_dataset` with `__len__`-less data

Test that writing a dataset from a `__len__`-less array-like correctly determines data shape via `shape[0]`.

## Broader pattern

The root issue is that hdmf implicitly assumes all array-like data supports `len()` and returns scalars from `__getitem__`. The Python array API standard does not require either. A shared mock class (like `ArrayWithoutLen` above) that mimics array-API-only behavior could be reused across the test suite to systematically verify all data paths.
