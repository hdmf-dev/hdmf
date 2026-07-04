"""AST-based migration tool for converting ``@docval`` functions to type hints.

Usage::

    python -m hdmf.typing.migrate path/to/file.py            # print migrated source
    python -m hdmf.typing.migrate path/to/file.py --diff     # show unified diff
    python -m hdmf.typing.migrate path/to/file.py --in-place # rewrite the file

The tool rewrites each ``@docval``-decorated function whose argument specs are
literal dicts: the decorator becomes ``@validated``, the ``**kwargs`` signature
becomes a real type-hinted signature, a Google-style docstring is generated from the
docval ``doc`` strings, and mechanical ``getargs``/``popargs`` body lines are
removed. Anything it cannot convert safely is marked with a ``# TODO(migrate):``
comment for human review:

- ``@docval(*get_docval(...))`` splice decorators (composition sites)
- mutable default values (converted to ``= None``; the body needs a None-guard)
- bodies that still reference ``kwargs`` after conversion (e.g.
  ``super().__init__(**kwargs)``)
"""

from __future__ import annotations

import argparse
import ast
import difflib
import sys
from dataclasses import dataclass, field
from pathlib import Path

# docval type strings -> hdmf.typing alias names
MACRO_MAP = {
    'array_data': 'ArrayData',
    'scalar_data': 'ScalarData',
    'data': 'AnyData',
    'int': 'Int',
    'uint': 'UInt',
    'float': 'Float',
    'bool': 'Bool',
}

# bare python numeric types are widened by docval's check_type; the aliases preserve that
NAME_MAP = {
    'int': 'Int',
    'float': 'Float',
    'bool': 'Bool',
}

MAX_SIGNATURE_WIDTH = 115


@dataclass
class MigratedArg:
    name: str
    hint: str
    doc: str = ''
    default_src: str | None = None  # source text of the default, None if required
    todos: list[str] = field(default_factory=list)


@dataclass
class MigratedFunction:
    node: ast.FunctionDef
    decorator: ast.Call
    args: list[MigratedArg]
    options: dict[str, str]  # remaining docval options (source text), e.g. allow_positional
    returns_doc: str | None
    rtype_hint: str | None
    allow_extra: bool
    todos: list[str] = field(default_factory=list)


class _AliasTracker:
    """Track which hdmf.typing names the migrated code needs to import."""

    def __init__(self):
        self.names = set()

    def use(self, name):
        self.names.add(name)
        return name


class DocvalMigrator:
    """Migrates ``@docval``-decorated functions in a source file to ``@validated``."""

    def __init__(self):
        self.aliases = _AliasTracker()
        self.typing_names = set()  # names needed from the stdlib typing module

    # ---------------------------------------------------------------- parsing

    def _find_docval_functions(self, tree):
        found = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for dec in node.decorator_list:
                if (isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name)
                        and dec.func.id == 'docval'):
                    found.append((node, dec))
                    break
        return found

    def _convert_type(self, node, arg):
        """Convert a docval type expression AST node to a type hint source string."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in MACRO_MAP:
                return self.aliases.use(MACRO_MAP[node.value])
            self.aliases.use('TypeName')
            return f"TypeName[{node.value!r}]"
        if isinstance(node, ast.Constant) and node.value is None:
            return 'None'
        if isinstance(node, ast.Name):
            if node.id in NAME_MAP:
                return self.aliases.use(NAME_MAP[node.id])
            return node.id
        if isinstance(node, (ast.Tuple, ast.List)):
            members = [self._convert_type(elt, arg) for elt in node.elts]
            # dedupe (e.g. ('array_data', list) -> ArrayData already covers list at runtime,
            # but keep both: the union is what the author wrote)
            seen = []
            for m in members:
                if m not in seen:
                    seen.append(m)
            return ' | '.join(seen)
        arg.todos.append(f"could not convert type expression: {ast.unparse(node)}")
        return ast.unparse(node)

    def _convert_arg(self, spec_node):
        """Convert one literal docval spec dict AST node to a MigratedArg, or None."""
        if not isinstance(spec_node, ast.Dict):
            return None
        keys = {}
        for k, v in zip(spec_node.keys, spec_node.values):
            if not (isinstance(k, ast.Constant) and isinstance(k.value, str)):
                return None
            keys[k.value] = v
        if 'name' not in keys or not isinstance(keys['name'], ast.Constant):
            return None
        arg = MigratedArg(name=keys['name'].value, hint='')

        if 'doc' in keys:
            doc_node = keys['doc']
            if isinstance(doc_node, ast.Constant) and isinstance(doc_node.value, str):
                arg.doc = ' '.join(doc_node.value.split())
            else:
                arg.doc = ''
                arg.todos.append(f"non-literal doc: {ast.unparse(doc_node)}")

        if 'enum' in keys:
            enum_src = ast.unparse(keys['enum']).strip('()[]')
            self.typing_names.add('Literal')
            arg.hint = f"Literal[{enum_src}]"
        else:
            arg.hint = self._convert_type(keys.get('type', ast.Constant(value=None)), arg)

        if 'shape' in keys:
            self.aliases.use('Shaped')
            arg.hint = f"Shaped[{arg.hint}, {ast.unparse(keys['shape'])}]"

        if 'default' in keys:
            default_node = keys['default']
            default_src = ast.unparse(default_node)
            is_mutable = (isinstance(default_node, (ast.List, ast.Dict, ast.Set))
                          or (isinstance(default_node, ast.Call)
                              and isinstance(default_node.func, ast.Name)
                              and default_node.func.id in ('list', 'dict', 'set')))
            allow_none = ('allow_none' in keys
                          and isinstance(keys['allow_none'], ast.Constant)
                          and keys['allow_none'].value)
            default_is_none = isinstance(default_node, ast.Constant) and default_node.value is None
            if is_mutable:
                arg.todos.append(f"default was {default_src}; docval deepcopied it per call — "
                                 "add a None-guard in the body")
                default_src = 'None'
                default_is_none = True
            if (default_is_none or allow_none) and 'None' not in arg.hint.split(' | '):
                arg.hint = f"{arg.hint} | None"
            arg.default_src = default_src
        return arg

    def _convert_function(self, node, dec):
        """Convert one @docval function. Returns a MigratedFunction, or None to skip."""
        func = MigratedFunction(node=node, decorator=dec, args=[], options={},
                                returns_doc=None, rtype_hint=None, allow_extra=False)
        for spec_node in dec.args:
            if isinstance(spec_node, ast.Starred):
                func.todos.append(
                    f"decorator splices other functions' specs ({ast.unparse(spec_node)}); "
                    "convert by hand")
                return None
            arg = self._convert_arg(spec_node)
            if arg is None:
                func.todos.append(f"non-literal argument spec: {ast.unparse(spec_node)}")
                return None
            func.args.append(arg)

        for kw in dec.keywords:
            if kw.arg == 'returns' and isinstance(kw.value, ast.Constant):
                func.returns_doc = kw.value.value
            elif kw.arg == 'rtype':
                if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    func.rtype_hint = f"{kw.value.value!r}"
                else:
                    func.rtype_hint = ast.unparse(kw.value)
            elif kw.arg == 'is_method':
                pass  # real signatures make this obsolete
            elif kw.arg == 'allow_extra':
                if isinstance(kw.value, ast.Constant) and kw.value.value:
                    func.allow_extra = True
            elif kw.arg in ('enforce_type', 'enforce_shape', 'allow_positional'):
                func.options[kw.arg] = ast.unparse(kw.value)
            elif kw.arg in ('func_name', 'doc'):
                func.todos.append(f"docval option {kw.arg}={ast.unparse(kw.value)} has no "
                                  "@validated equivalent; convert by hand")
                return None
        return func

    # --------------------------------------------------------------- emitting

    def _existing_description(self, node):
        doc = ast.get_docstring(node)
        return doc.strip() if doc else ''

    def _build_docstring(self, func, indent):
        lines = []
        description = self._existing_description(func.node)
        body_indent = indent + '    '
        lines.append(f'{body_indent}"""{description.splitlines()[0] if description else func.node.name}')
        for extra in (description.splitlines()[1:] if description else []):
            lines.append(f'{body_indent}{extra}' if extra.strip() else '')
        if func.args:
            lines.append('')
            lines.append(f'{body_indent}Args:')
            for arg in func.args:
                lines.append(f'{body_indent}    {arg.name}: {arg.doc}')
        if func.returns_doc:
            lines.append('')
            lines.append(f'{body_indent}Returns:')
            lines.append(f'{body_indent}    {func.returns_doc}')
        lines.append(f'{body_indent}"""')
        return lines

    def _build_signature(self, func, indent):
        node = func.node
        params = []
        existing = [a.arg for a in node.args.args]
        if existing and existing[0] in ('self', 'cls'):
            params.append(existing[0])
        required = [a for a in func.args if a.default_src is None]
        optional = [a for a in func.args if a.default_src is not None]
        for arg in required:
            params.append(f"{arg.name}: {arg.hint}")
        for arg in optional:
            params.append(f"{arg.name}: {arg.hint} = {arg.default_src}")
        if func.allow_extra:
            params.append('**kwargs')
        ret = f" -> {func.rtype_hint}" if func.rtype_hint and node.name != '__init__' else ''
        one_line = f"{indent}def {node.name}({', '.join(params)}){ret}:"
        if len(one_line) <= MAX_SIGNATURE_WIDTH:
            return [one_line]
        arg_indent = ' ' * (len(indent) + len(f"def {node.name}(") )
        lines = [f"{indent}def {node.name}({params[0]},"]
        for p in params[1:-1]:
            lines.append(f"{arg_indent}{p},")
        lines.append(f"{arg_indent}{params[-1]}){ret}:")
        return lines

    def _build_decorator(self, func, indent):
        if func.options:
            opts = ', '.join(f"{k}={v}" for k, v in sorted(func.options.items()))
            return f"{indent}@validated({opts})"
        return f"{indent}@validated"

    def _rewrite_body(self, func, body_lines, indent):
        """Remove mechanical getargs/popargs lines; flag remaining kwargs references."""
        import re
        argnames = {a.name for a in func.args}
        out = []
        removed_any = False
        pattern = re.compile(
            r"^\s*(?P<targets>\w+(?:\s*,\s*\w+)*)\s*=\s*(?:getargs|popargs)\(\s*"
            r"(?P<names>(?:'[^']+'|\"[^\"]+\")(?:\s*,\s*(?:'[^']+'|\"[^\"]+\"))*)\s*,\s*kwargs\s*\)\s*$")
        for line in body_lines:
            m = pattern.match(line)
            if m:
                targets = [t.strip() for t in m.group('targets').split(',')]
                names = [n.strip().strip('\'"') for n in m.group('names').split(',')]
                if targets == names and all(n in argnames for n in names):
                    removed_any = True
                    continue  # parameters are now real names; the line is redundant
            out.append(line)
        leftover_kwargs = any('kwargs' in line for line in out) and not func.allow_extra
        todos = list(func.todos)
        for arg in func.args:
            todos.extend(f"{arg.name}: {t}" for t in arg.todos)
        if leftover_kwargs and removed_any:
            todos.append("body still references `kwargs`; rewrite remaining uses "
                         "(e.g. super().__init__(**kwargs) -> explicit keywords)")
        todo_lines = [f"{indent}    # TODO(migrate): {t}" for t in todos]
        return todo_lines + out

    # ------------------------------------------------------------------ driver

    def migrate_source(self, source):
        """Return (migrated_source, n_converted, n_skipped)."""
        tree = ast.parse(source)
        targets = self._find_docval_functions(tree)
        if not targets:
            return source, 0, 0
        lines = source.splitlines()
        n_converted = 0
        n_skipped = 0
        # bottom-up so earlier line numbers stay valid
        for node, dec in sorted(targets, key=lambda t: t[0].lineno, reverse=True):
            func = self._convert_function(node, dec)
            if func is None:
                n_skipped += 1
                dec_line = dec.lineno - 1
                indent = ' ' * (len(lines[dec_line]) - len(lines[dec_line].lstrip()))
                lines.insert(dec_line, f"{indent}# TODO(migrate): docval decorator could not be "
                                       "converted automatically; convert by hand")
                continue
            n_converted += 1
            dec_start = min(d.lineno for d in node.decorator_list
                            if d is dec or getattr(getattr(d, 'func', None), 'id', None) == 'docval') - 1
            # skip an existing docstring; it is folded into the generated one
            body_start = node.body[0].lineno - 1
            body_end = node.end_lineno - 1
            first_stmt = node.body[0]
            if (isinstance(first_stmt, ast.Expr) and isinstance(first_stmt.value, ast.Constant)
                    and isinstance(first_stmt.value.value, str)):
                body_start = first_stmt.end_lineno  # first line after the docstring
            indent = ' ' * node.col_offset

            other_decorators = [ast.unparse(d) for d in node.decorator_list if d is not dec]
            new_block = [f"{indent}@{d}" for d in other_decorators]
            new_block.append(self._build_decorator(func, indent))
            new_block.extend(self._build_signature(func, indent))
            new_block.extend(self._build_docstring(func, indent))
            body_lines = lines[body_start:body_end + 1] if body_start <= body_end else []
            new_block.extend(self._rewrite_body(func, body_lines, indent))

            lines[dec_start:body_end + 1] = new_block
        migrated = '\n'.join(lines)
        if source.endswith('\n') and not migrated.endswith('\n'):
            migrated += '\n'
        migrated = self._add_imports(migrated)
        return migrated, n_converted, n_skipped

    def _add_imports(self, source):
        stmts = []
        if self.typing_names and not any(
                'from typing import' in line and name in line
                for name in self.typing_names for line in source.splitlines()):
            stmts.append(f"from typing import {', '.join(sorted(self.typing_names))}")
        if 'from hdmf.typing import' not in source:
            import_names = sorted(self.aliases.names) + ['validated']
            stmts.append(f"from hdmf.typing import {', '.join(import_names)}")
        if not stmts:
            return source
        lines = source.splitlines()
        insert_at = 0
        for i, line in enumerate(lines):
            if line.startswith(('import ', 'from ')):
                insert_at = i + 1
        lines[insert_at:insert_at] = stmts
        out = '\n'.join(lines)
        return out + '\n' if source.endswith('\n') else out

    def migrate_file(self, filepath, in_place=False):
        filepath = Path(filepath)
        source = filepath.read_text()
        migrated, n_converted, n_skipped = self.migrate_source(source)
        if in_place and migrated != source:
            filepath.write_text(migrated)
        return migrated, n_converted, n_skipped


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='python -m hdmf.typing.migrate',
        description="Migrate @docval decorators to @validated with type hints")
    parser.add_argument('files', nargs='+', help="Python files to migrate")
    parser.add_argument('--diff', action='store_true', help="show a unified diff of the changes")
    parser.add_argument('--in-place', '-i', action='store_true', help="rewrite files in place")
    args = parser.parse_args(argv)

    migrator = DocvalMigrator()
    for filepath in args.files:
        filepath = Path(filepath)
        if not filepath.exists():
            print(f"error: file not found: {filepath}", file=sys.stderr)  # noqa: T201
            return 1
        original = filepath.read_text()
        migrated, n_converted, n_skipped = migrator.migrate_file(filepath, in_place=args.in_place)
        print(f"{filepath}: converted {n_converted}, needs manual attention {n_skipped}",  # noqa: T201
              file=sys.stderr)
        if args.diff:
            sys.stdout.writelines(difflib.unified_diff(
                original.splitlines(keepends=True), migrated.splitlines(keepends=True),
                fromfile=f"a/{filepath.name}", tofile=f"b/{filepath.name}"))
        elif not args.in_place:
            print(migrated)  # noqa: T201
    return 0


if __name__ == '__main__':
    sys.exit(main())
