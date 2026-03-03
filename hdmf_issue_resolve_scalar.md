# Handle 0-d ndarrays in scalar isinstance checks

Chained after #1414 (`_is_collection`). Motivated by hdmf-dev/hdmf-zarr#325 (zarr v2 to v3 migration).

hdmf's `get_type` functions infer element dtype by recursively indexing with `data[0]` until reaching a scalar, then calling `type()` on it. With numpy and zarr v2, `data[0]` on a 1-d float array returns a numpy scalar (e.g., `numpy.float64`), which passes `isinstance(val, float)` and has no `__len__`, so all downstream checks work. With zarr v3 (following the Python array API standard), `data[0]` returns a 0-d ndarray instead. A 0-d ndarray fails `isinstance(val, (int, float, str, bool))` and `type()` returns `numpy.ndarray` rather than the element dtype. PR #1414 fixed the crash path (`__len__` heuristic), but `isinstance` checks in other parts of the codebase still silently take the wrong branch when they encounter a 0-d ndarray.

This PR adds a `_unwrap_scalar` helper in `hdmf.utils` that converts 0-d ndarrays to numpy scalars via `.item()`, and applies it at the remaining `isinstance` checks that compare against Python scalar types. Together with #1414, this eliminates the need for the `__getitem__` monkey-patch in hdmf-zarr PR #325.
