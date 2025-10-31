"""
Python Version Support Checker

This script analyzes Python package dependencies listed in pyproject.toml to check their
compatibility with a specified Python version (default: 3.13). It examines both regular
and optional dependencies, checking their trove classifiers for explicit version support.

The script provides:
- Grouped output of supported and unsupported packages
- Latest supported Python version for packages without explicit support
- Error reporting for packages that cannot be checked
- Summary statistics of compatibility status

Usage:
    # Run this command from the root of the repo
    python scripts/check_py_support.py

Requirements:
    - Python 3.11+
    - packaging
    - colorama

Input:
    - pyproject.toml file in the current directory

Output format:
    - Supported packages (green) with their versions
    - Unsupported packages (red) with their versions and latest supported Python version
    - Packages with errors (yellow)
    - Summary statistics

Note:
    The absence of explicit version support in trove classifiers doesn't necessarily
    indicate incompatibility, just that the package hasn't declared support.
"""

import tomllib
import importlib.metadata
from pathlib import Path
from packaging.requirements import Requirement
from colorama import init, Fore, Style
from typing import NamedTuple
import re

# Initialize colorama
init()

# Global configuration
PYTHON_VERSION = "3.13"

class PackageSupport(NamedTuple):
    name: str
    spec: str
    version: str | None
    latest_python: str | None
    error: str | None

def parse_dependencies(pyproject_path: Path) -> list[str]:
    """Parse dependencies from pyproject.toml, including optional dependencies."""
    with pyproject_path.open("rb") as f:
        pyproject = tomllib.load(f)

    # Get main dependencies
    dependencies = pyproject.get("project", {}).get("dependencies", [])

    # Get optional dependencies and flatten them
    optional_deps = pyproject.get("project", {}).get("optional-dependencies", {})
    for group_deps in optional_deps.values():
        dependencies.extend(group_deps)

    return dependencies

def get_package_name(dependency_spec: str) -> str:
    """Extract package name from dependency specification."""
    return Requirement(dependency_spec).name

def get_latest_python_version(classifiers: list[str]) -> str | None:
    """Extract the latest supported Python version from classifiers."""
    python_versions = []
    pattern = r"Programming Language :: Python :: (\d+\.\d+)"

    for classifier in classifiers:
        match = re.match(pattern, classifier)
        if match:
            version = match.group(1)
            try:
                major, minor = map(int, version.split('.'))
                python_versions.append((major, minor))
            except ValueError:
                continue

    if not python_versions:
        return None

    # Sort by major and minor version
    latest = sorted(python_versions, key=lambda x: (x[0], x[1]), reverse=True)[0]
    return f"{latest[0]}.{latest[1]}"

def check_python_version_support(package_name: str) -> dict[str, str | bool | None]:
    """Check if installed package supports Python 3.13."""
    try:
        dist = importlib.metadata.distribution(package_name)
        classifiers = dist.metadata.get_all('Classifier')
        version_classifier = f"Programming Language :: Python :: {PYTHON_VERSION}"

        return {
            'installed_version': dist.version,
            'has_support': version_classifier in classifiers,
            'latest_python': get_latest_python_version(classifiers),
            'error': None
        }
    except importlib.metadata.PackageNotFoundError:
        return {
            'installed_version': None,
            'has_support': False,
            'latest_python': None,
            'error': 'Package not installed'
        }
    except Exception as e:
        return {
            'installed_version': None,
            'has_support': False,
            'latest_python': None,
            'error': str(e)
        }

def print_section_header(title: str, count: int) -> None:
    """Print a formatted section header with count."""
    print(f"\n{Fore.CYAN}{title} ({count} packages){Style.RESET_ALL}")
    print(f"{Fore.BLUE}{'-' * 100}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{'Package':<25} {'Specification':<30} {'Version':<20} {'Latest Python'}{Style.RESET_ALL}")
    print(f"{Fore.BLUE}{'-' * 100}{Style.RESET_ALL}")

def main() -> None:
    pyproject_path = Path("pyproject.toml")

    if not pyproject_path.exists():
        print(f"{Fore.RED}Error: pyproject.toml not found{Style.RESET_ALL}")
        return

    try:
        dependencies = parse_dependencies(pyproject_path)
    except Exception as e:
        print(f"{Fore.RED}Error parsing pyproject.toml: {e}{Style.RESET_ALL}")
        return

    # Check each dependency
    supported: list[PackageSupport] = []
    unsupported: list[PackageSupport] = []
    errors: list[PackageSupport] = []

    for dep in dependencies:
        package_name = get_package_name(dep)
        result = check_python_version_support(package_name)

        package_info = PackageSupport(
            name=package_name,
            spec=dep,
            version=result['installed_version'],
            latest_python=result['latest_python'],
            error=result['error']
        )

        if result['error']:
            errors.append(package_info)
        elif result['has_support']:
            supported.append(package_info)
        else:
            unsupported.append(package_info)

    # Print results
    print(f"\n{Fore.CYAN}Python {PYTHON_VERSION} Explicit Support Check Results{Style.RESET_ALL}")
    print(f"{Fore.BLUE}{'=' * 100}{Style.RESET_ALL}")

    # Print supported packages
    if supported:
        print_section_header("Supported Packages", len(supported))
        for pkg in supported:
            print(f"{Fore.GREEN}{pkg.name:<25} {pkg.spec:<30} {pkg.version:<20} {PYTHON_VERSION}{Style.RESET_ALL}")

    # Print unsupported packages
    if unsupported:
        print_section_header("Unsupported Packages", len(unsupported))
        for pkg in unsupported:
            latest = f"→ {pkg.latest_python}" if pkg.latest_python else "unknown"
            print(f"{Fore.RED}{pkg.name:<25} {pkg.spec:<30} {pkg.version:<20} {latest}{Style.RESET_ALL}")

    # Print packages with errors
    if errors:
        print_section_header("Packages with Errors", len(errors))
        for pkg in errors:
            print(f"{Fore.YELLOW}{pkg.name:<25} {pkg.spec:<30} {pkg.error:<20} N/A{Style.RESET_ALL}")

    # Print summary
    print(f"\n{Fore.CYAN}Summary:{Style.RESET_ALL}")
    print(f"{Fore.BLUE}{'-' * 100}{Style.RESET_ALL}")
    total = len(supported) + len(unsupported) + len(errors)
    print(f"{Fore.GREEN}Supported:   {len(supported):3d} ({len(supported)/total*100:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.RED}Unsupported: {len(unsupported):3d} ({len(unsupported)/total*100:.1f}%){Style.RESET_ALL}")
    if errors:
        print(f"{Fore.YELLOW}Errors:      {len(errors):3d} ({len(errors)/total*100:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.CYAN}Total:       {total:3d}{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
