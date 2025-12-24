"""
Migration tool for converting @docval to @validated with type hints.

This module provides utilities to help migrate code from the legacy @docval
decorator to the new @validated decorator with Python type hints.

Usage:
    python -m hdmf.validation.migrate path/to/file.py [--dry-run] [--in-place]
    
    Or programmatically:
    
    from hdmf.validation.migrate import DocvalMigrator
    migrator = DocvalMigrator()
    new_code = migrator.migrate_file("path/to/file.py")
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# Mapping from docval type strings to Python type hints
DOCVAL_TYPE_MAP: dict[str, str] = {
    # Scalar types
    "str": "str",
    "int": "int",
    "float": "float",
    "bool": "bool",
    "bytes": "bytes",
    "None": "None",
    
    # Array types - map to our custom type aliases
    "array_data": "ArrayData",
    "scalar_data": "ScalarData",
    "data": "AnyData",
    
    # NumPy types
    "ndarray": "np.ndarray",
    
    # Container types
    "list": "list",
    "tuple": "tuple",
    "dict": "dict",
    "set": "set",
    
    # HDMF types (will be kept as strings for forward references)
    "Container": "Container",
    "Data": "Data",
    "AbstractContainer": "AbstractContainer",
}


@dataclass
class DocvalArg:
    """Represents a single docval argument specification."""
    name: str
    type_: str | tuple[str, ...] | None = None
    doc: str = ""
    default: Any = None
    has_default: bool = False
    required: bool = True
    shape: tuple | None = None
    
    def to_type_hint(self) -> str:
        """Convert this docval arg to a Python type hint string."""
        if self.type_ is None:
            return "Any"
        
        if isinstance(self.type_, tuple):
            # Multiple types - create Union
            types = [self._single_type_to_hint(t) for t in self.type_]
            if len(types) == 1:
                type_hint = types[0]
            else:
                type_hint = f"Union[{', '.join(types)}]"
        else:
            type_hint = self._single_type_to_hint(self.type_)
        
        # Handle optional (has default of None)
        if self.has_default and self.default is None and not self.required:
            if not type_hint.startswith("Optional[") and "None" not in type_hint:
                type_hint = f"Optional[{type_hint}]"
        
        return type_hint
    
    def _single_type_to_hint(self, type_str: str) -> str:
        """Convert a single type string to a type hint."""
        # Check if it's a known type
        if type_str in DOCVAL_TYPE_MAP:
            return DOCVAL_TYPE_MAP[type_str]
        
        # Check if it's a forward reference (string with dots)
        if "." in type_str:
            # Keep as quoted string for forward reference
            return f'"{type_str}"'
        
        # Otherwise, return as-is (might be a class name)
        return type_str
    
    def to_parameter(self) -> str:
        """Generate the parameter string for a function signature."""
        type_hint = self.to_type_hint()
        
        if self.has_default:
            if self.default is None:
                default_str = "None"
            elif isinstance(self.default, str):
                default_str = repr(self.default)
            elif isinstance(self.default, (list, dict)):
                # Use None as default for mutable types (Python best practice)
                if type_hint.startswith("Optional["):
                    default_str = "None"
                else:
                    type_hint = f"Optional[{type_hint}]"
                    default_str = "None"
            else:
                default_str = repr(self.default)
            return f"{self.name}: {type_hint} = {default_str}"
        else:
            return f"{self.name}: {type_hint}"


@dataclass
class DocvalFunction:
    """Represents a function decorated with @docval."""
    name: str
    args: list[DocvalArg] = field(default_factory=list)
    returns: str | None = None
    is_method: bool = False
    docstring: str | None = None
    original_code: str = ""
    line_start: int = 0
    line_end: int = 0
    
    def to_validated_signature(self) -> str:
        """Generate the new function signature with @validated and type hints."""
        # Build parameter list
        params = []
        
        # Add self/cls for methods
        if self.is_method:
            params.append("self")
        
        # Add all arguments
        for arg in self.args:
            params.append(arg.to_parameter())
        
        # Build return type
        return_hint = f" -> {self.returns}" if self.returns else ""
        
        # Build the signature
        params_str = ", ".join(params)
        return f"def {self.name}({params_str}){return_hint}:"
    
    def to_validated_decorator(self) -> str:
        """Generate the @validated decorator."""
        return "@validated"


class DocvalParser:
    """Parser for extracting @docval specifications from Python code."""
    
    def __init__(self):
        self.functions: list[DocvalFunction] = []
    
    def parse_file(self, filepath: str | Path) -> list[DocvalFunction]:
        """Parse a Python file and extract all @docval decorated functions."""
        filepath = Path(filepath)
        code = filepath.read_text()
        return self.parse_code(code)
    
    def parse_code(self, code: str) -> list[DocvalFunction]:
        """Parse Python code and extract all @docval decorated functions."""
        self.functions = []
        
        # Find all @docval decorators using regex
        # This is more robust than AST for extracting the original decorator arguments
        docval_pattern = re.compile(
            r'^(\s*)@docval\s*\((.*?)\)\s*\n\s*def\s+(\w+)\s*\(',
            re.MULTILINE | re.DOTALL
        )
        
        for match in docval_pattern.finditer(code):
            indent = match.group(1)
            docval_args = match.group(2)
            func_name = match.group(3)
            
            # Parse the docval arguments
            args = self._parse_docval_args(docval_args)
            
            # Determine if it's a method (has 'self' or 'cls' as first param)
            is_method = self._check_if_method(code, match.end())
            
            func = DocvalFunction(
                name=func_name,
                args=args,
                is_method=is_method,
                line_start=code[:match.start()].count('\n') + 1,
            )
            
            self.functions.append(func)
        
        return self.functions
    
    def _parse_docval_args(self, docval_str: str) -> list[DocvalArg]:
        """Parse docval argument dictionaries from a string."""
        args = []
        
        # Find all dictionary patterns
        dict_pattern = re.compile(r'\{([^{}]*)\}', re.DOTALL)
        
        for match in dict_pattern.finditer(docval_str):
            dict_content = match.group(1)
            arg = self._parse_single_arg(dict_content)
            if arg:
                args.append(arg)
        
        return args
    
    def _parse_single_arg(self, dict_content: str) -> DocvalArg | None:
        """Parse a single docval argument dictionary."""
        # Extract name
        name_match = re.search(r"'name'\s*:\s*'([^']+)'", dict_content)
        if not name_match:
            name_match = re.search(r'"name"\s*:\s*"([^"]+)"', dict_content)
        if not name_match:
            return None
        
        name = name_match.group(1)
        
        # Extract type - need to handle tuples like ('array_data', list)
        type_ = None
        type_start = re.search(r"'type'\s*:\s*", dict_content)
        if not type_start:
            type_start = re.search(r'"type"\s*:\s*', dict_content)
        
        if type_start:
            # Start after the 'type': part
            start_pos = type_start.end()
            type_str = self._extract_value(dict_content[start_pos:])
            type_ = self._parse_type(type_str)
        
        # Extract doc
        doc_match = re.search(r"'doc'\s*:\s*['\"]([^'\"]+)['\"]", dict_content)
        if not doc_match:
            doc_match = re.search(r'"doc"\s*:\s*[\'"]([^"\']+)[\'"]', dict_content)
        doc = doc_match.group(1) if doc_match else ""
        
        # Extract default
        has_default = "'default'" in dict_content or '"default"' in dict_content
        default = None
        if has_default:
            default_match = re.search(r"'default'\s*:\s*([^,}]+)", dict_content)
            if not default_match:
                default_match = re.search(r'"default"\s*:\s*([^,}]+)', dict_content)
            if default_match:
                default_str = default_match.group(1).strip()
                default = self._parse_default(default_str)
        
        # Check if required
        required = not has_default
        
        return DocvalArg(
            name=name,
            type_=type_,
            doc=doc,
            default=default,
            has_default=has_default,
            required=required,
        )
    
    def _extract_value(self, text: str) -> str:
        """Extract a complete value from text, handling parentheses and brackets.
        
        This properly handles tuple types like ('array_data', list) by tracking
        parenthesis depth to find where the value ends.
        """
        text = text.strip()
        if not text:
            return ""
        
        # If it starts with a paren, find the matching close paren
        if text.startswith('('):
            paren_count = 0
            for i, char in enumerate(text):
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        return text[:i + 1]
            # No matching paren found, return up to comma or end
            return text.split(',')[0].strip()
        
        # If it starts with a bracket or brace, handle similarly
        if text.startswith('['):
            bracket_count = 0
            for i, char in enumerate(text):
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        return text[:i + 1]
        
        if text.startswith('{'):
            brace_count = 0
            for i, char in enumerate(text):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        return text[:i + 1]
        
        # Otherwise, return up to the next comma or end of dict
        # But be careful of nested structures
        result = ""
        nested = 0
        for char in text:
            if char in '([{':
                nested += 1
                result += char
            elif char in ')]}':
                nested -= 1
                result += char
            elif char == ',' and nested == 0:
                break
            elif char == '}' and nested == 0:
                break
            else:
                result += char
        
        return result.strip()
    
    def _parse_type(self, type_str: str) -> str | tuple[str, ...]:
        """Parse a type specification from docval."""
        type_str = type_str.strip()
        
        # Handle tuple of types - need to find matching parentheses
        if type_str.startswith('('):
            # Find the matching closing paren
            paren_count = 0
            end_idx = 0
            for i, char in enumerate(type_str):
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        end_idx = i
                        break
            
            if end_idx > 0:
                inner = type_str[1:end_idx]
                types = []
                # Split by comma but be careful of nested structures
                current = ""
                nested = 0
                for char in inner:
                    if char in '([{':
                        nested += 1
                        current += char
                    elif char in ')]}':
                        nested -= 1
                        current += char
                    elif char == ',' and nested == 0:
                        t = current.strip().strip("'\"")
                        if t:
                            types.append(t)
                        current = ""
                    else:
                        current += char
                # Don't forget the last item
                t = current.strip().strip("'\"")
                if t:
                    types.append(t)
                
                return tuple(types) if len(types) > 1 else types[0] if types else "Any"
        
        # Single type
        return type_str.strip("'\"")
    
    def _parse_default(self, default_str: str) -> Any:
        """Parse a default value from docval."""
        default_str = default_str.strip()
        
        if default_str == "None":
            return None
        if default_str == "True":
            return True
        if default_str == "False":
            return False
        if default_str.startswith("'") or default_str.startswith('"'):
            return default_str[1:-1]
        if default_str.isdigit():
            return int(default_str)
        try:
            return float(default_str)
        except ValueError:
            pass
        
        # Return as-is for complex expressions
        return default_str
    
    def _check_if_method(self, code: str, pos: int) -> bool:
        """Check if the function at the given position is a method (has self/cls)."""
        # Look for self or cls after the opening paren
        remaining = code[pos:pos + 100]
        return remaining.strip().startswith('self') or remaining.strip().startswith('cls')


class DocvalMigrator:
    """Migrates @docval decorated code to @validated with type hints."""
    
    def __init__(self):
        self.parser = DocvalParser()
    
    def migrate_file(self, filepath: str | Path, dry_run: bool = True) -> str:
        """Migrate a file from @docval to @validated.
        
        Args:
            filepath: Path to the Python file to migrate
            dry_run: If True, return the migrated code without writing to file
            
        Returns:
            The migrated code as a string
        """
        filepath = Path(filepath)
        original_code = filepath.read_text()
        
        functions = self.parser.parse_file(filepath)
        
        if not functions:
            return original_code
        
        migrated_code = self._apply_migrations(original_code, functions)
        
        if not dry_run:
            filepath.write_text(migrated_code)
        
        return migrated_code
    
    def migrate_code(self, code: str) -> str:
        """Migrate code from @docval to @validated.
        
        Args:
            code: Python source code as a string
            
        Returns:
            The migrated code as a string
        """
        functions = self.parser.parse_code(code)
        
        if not functions:
            return code
        
        return self._apply_migrations(code, functions)
    
    def _apply_migrations(self, code: str, functions: list[DocvalFunction]) -> str:
        """Apply migrations to the code."""
        lines = code.split('\n')
        
        # Add necessary imports at the top
        imports_to_add = self._get_required_imports(functions)
        
        # Find where to insert imports (after existing imports)
        import_insert_line = self._find_import_insert_position(lines)
        
        # Process functions in reverse order to maintain line numbers
        for func in reversed(functions):
            lines = self._migrate_function(lines, func)
        
        # Insert imports
        if imports_to_add:
            for i, imp in enumerate(imports_to_add):
                lines.insert(import_insert_line + i, imp)
        
        return '\n'.join(lines)
    
    def _get_required_imports(self, functions: list[DocvalFunction]) -> list[str]:
        """Determine what imports are needed for the migrated code."""
        imports = []
        
        # Check if we need typing imports
        needs_optional = False
        needs_union = False
        needs_any = False
        
        for func in functions:
            for arg in func.args:
                hint = arg.to_type_hint()
                if "Optional[" in hint:
                    needs_optional = True
                if "Union[" in hint:
                    needs_union = True
                if hint == "Any":
                    needs_any = True
        
        typing_imports = []
        if needs_optional:
            typing_imports.append("Optional")
        if needs_union:
            typing_imports.append("Union")
        if needs_any:
            typing_imports.append("Any")
        
        if typing_imports:
            imports.append(f"from typing import {', '.join(typing_imports)}")
        
        # Add validated import
        imports.append("from hdmf.validation import validated, ArrayData, ScalarData, AnyData")
        
        return imports
    
    def _find_import_insert_position(self, lines: list[str]) -> int:
        """Find the line number where imports should be inserted."""
        last_import_line = 0
        in_docstring = False
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # Skip docstrings
            if '"""' in stripped or "'''" in stripped:
                in_docstring = not in_docstring
                continue
            
            if in_docstring:
                continue
            
            # Check for import statements
            if stripped.startswith('import ') or stripped.startswith('from '):
                last_import_line = i + 1
        
        return last_import_line
    
    def _migrate_function(self, lines: list[str], func: DocvalFunction) -> list[str]:
        """Migrate a single function in the code."""
        # Find the @docval decorator line using the stored line_start
        # line_start is 1-indexed, so we convert to 0-indexed
        target_line = func.line_start - 1
        
        # Search around the target line for the @docval decorator
        docval_start = None
        docval_end = None
        def_line = None
        
        # Look for @docval within a few lines of where we expect it
        search_start = max(0, target_line - 5)
        search_end = min(len(lines), target_line + 50)
        
        for i in range(search_start, search_end):
            line = lines[i]
            if '@docval' in line and docval_start is None:
                # Check if this is close to our expected location
                if abs(i - target_line) <= 5:
                    docval_start = i
            if docval_start is not None and 'def ' + func.name in line:
                def_line = i
                # Find where the decorator ends (might span multiple lines)
                for j in range(docval_start, i):
                    if ')' in lines[j]:
                        docval_end = j
                if docval_end is None:
                    docval_end = i - 1
                break
        
        if docval_start is None or def_line is None:
            return lines
        
        # Get the indentation
        indent = len(lines[docval_start]) - len(lines[docval_start].lstrip())
        indent_str = ' ' * indent
        
        # Create the new decorator and signature
        new_decorator = indent_str + func.to_validated_decorator()
        new_signature = indent_str + func.to_validated_signature()
        
        # Find where the original def line ends (might have multi-line params)
        def_end = def_line
        paren_count = 0
        for i in range(def_line, len(lines)):
            paren_count += lines[i].count('(') - lines[i].count(')')
            if paren_count <= 0 and ':' in lines[i]:
                def_end = i
                break
        
        # Replace the decorator and function definition
        new_lines = lines[:docval_start]
        new_lines.append(new_decorator)
        new_lines.append(new_signature)
        new_lines.extend(lines[def_end + 1:])
        
        return new_lines
    
    def generate_diff(self, filepath: str | Path) -> str:
        """Generate a diff showing the proposed changes."""
        import difflib
        
        filepath = Path(filepath)
        original = filepath.read_text()
        migrated = self.migrate_file(filepath, dry_run=True)
        
        original_lines = original.splitlines(keepends=True)
        migrated_lines = migrated.splitlines(keepends=True)
        
        diff = difflib.unified_diff(
            original_lines,
            migrated_lines,
            fromfile=f"a/{filepath.name}",
            tofile=f"b/{filepath.name}",
        )
        
        return ''.join(diff)


def main():
    """Command-line interface for the migration tool."""
    parser = argparse.ArgumentParser(
        description="Migrate @docval decorators to @validated with type hints"
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="Python files to migrate"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be changed without modifying files (default)"
    )
    parser.add_argument(
        "--in-place", "-i",
        action="store_true",
        help="Modify files in place"
    )
    parser.add_argument(
        "--diff",
        action="store_true",
        help="Show unified diff of changes"
    )
    
    args = parser.parse_args()
    
    migrator = DocvalMigrator()
    
    for filepath in args.files:
        filepath = Path(filepath)
        
        if not filepath.exists():
            print(f"Error: File not found: {filepath}", file=sys.stderr)
            continue
        
        if args.diff:
            diff = migrator.generate_diff(filepath)
            if diff:
                print(f"=== Changes for {filepath} ===")
                print(diff)
            else:
                print(f"No @docval decorators found in {filepath}")
        elif args.in_place:
            result = migrator.migrate_file(filepath, dry_run=False)
            print(f"Migrated: {filepath}")
        else:
            result = migrator.migrate_file(filepath, dry_run=True)
            print(f"=== Migrated {filepath} (dry-run) ===")
            print(result)


if __name__ == "__main__":
    main()
