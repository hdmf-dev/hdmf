# Rename `_get_length` to reflect actual semantics: first-dimension size, not `len()`

The `_get_length` utility was introduced in PR #1414 to replace direct `len()` calls that break with array-API-conforming libraries (e.g., zarr v3, which does not implement `__len__` on arrays). The function works correctly, but its name implies `len()` semantics when what hdmf actually needs in every call site is the size of the first dimension.

## Current implementation

```python
def _get_length(data):
    if hasattr(data, "shape") and data.shape is not None:
        return data.shape[0]
    return len(data)
```

## What every call site actually does

Every use of `_get_length` in the codebase falls into one of three patterns, all of which are asking "how many rows / elements along the first axis?":

**Bounds checking / slice resolution**
- `table.py`: `arg.indices(_get_length(self.data))`, `arg >= _get_length(self.data)`
- `table.py`: `DynamicTableRegion.shape` returns `(_get_length(self.data), ...)`

**Empty check**
- `table.py`: `_get_length(data) > 0`
- `validator.py`: `_get_length(data) > 0`
- `h5tools.py`: `_get_length(data) == 0`
- `objectmapper.py`: `_get_length(t) > 0`, `_get_length(tmp) == 0`

**Sizing / iteration**
- `container.py`: `Data.__len__` returns `_get_length(self.__data)`
- `h5tools.py`: `_get_length(data) > dset.shape[0]`, `new_shape[0] = _get_length(data)`
- `objectmapper.py`: `data_shape = (_get_length(data),)`
- `data_utils.py`: `self.__maxshape[0] = _get_length(self.data)`

None of these are asking for "the length of this object" in the Python `__len__` sense. They are all asking for `shape[0]` -- the number of elements along the first axis.

## Why the name matters

1. The array API standard deliberately excludes `__len__` from array objects. This was not an oversight: for multi-dimensional arrays, `len()` is ambiguous (does it mean total elements? first axis? something else?). The standard uses `.shape` and `.size` instead.

2. Naming this `_get_length` suggests that hdmf is working around the missing `__len__` by reimplementing it. A name like `_num_rows` or `_first_dim_size` would make clear that we are using the right semantic: first-axis size via `.shape[0]`, with a `len()` fallback for plain containers that don't have `.shape`.

3. Future contributors will see `_get_length` and think "this is just `len()` with extra steps" rather than understanding the intent.

## Suggested rename

`_get_length` -> `_num_rows` or `_first_dim_size`

The fallback to `len()` for plain containers (lists, tuples, dicts) should stay since these don't have `.shape`, and for 1-d containers `len()` does give the first-axis size. But the name should reflect what we are actually measuring.

This is a low-priority naming issue, not a bug. All existing call sites work correctly with the current implementation.
