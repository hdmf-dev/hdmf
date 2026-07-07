"""Extraction of per-argument documentation from Google-style docstrings."""

from docstring_parser import DocstringStyle, parse


def parse_docstring(func):
    """Return (param_docs, returns_doc, rtype) parsed from ``func.__doc__``.

    ``param_docs`` maps argument name to its description from the ``Args:`` section
    (missing arguments simply have no entry). ``returns_doc`` and ``rtype`` come from
    the ``Returns:`` section, or are None if absent.
    """
    doc = getattr(func, '__doc__', None)
    if not doc:
        return {}, None, None
    try:
        parsed = parse(doc, style=DocstringStyle.GOOGLE)
    except Exception:
        return {}, None, None
    param_docs = {}
    for param in parsed.params:
        if param.description:
            # collapse continuation-line whitespace into single spaces
            param_docs[param.arg_name] = ' '.join(param.description.split())
    returns_doc = None
    rtype = None
    if parsed.returns is not None:
        if parsed.returns.description:
            returns_doc = ' '.join(parsed.returns.description.split())
        rtype = parsed.returns.type_name
    return param_docs, returns_doc, rtype
