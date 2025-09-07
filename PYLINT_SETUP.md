# Pylint Configuration for Eventy

This document describes the pylint setup for the Eventy project.

## Overview

Pylint is configured to maintain code quality standards while being practical for the project's development workflow. The configuration is stored in `pyproject.toml` under the `[tool.pylint.*]` sections.

## Configuration Highlights

### Score Threshold
- **Minimum score**: 8.0/10
- **Current score**: 9.12/10

### Disabled Rules

The following rules have been disabled to match the project's coding style:

- `C0114` - missing-module-docstring
- `C0116` - missing-function-docstring  
- `R0913` - too-many-arguments
- `R0917` - too-many-positional-arguments
- `R0902` - too-many-instance-attributes
- `W0603` - global-statement
- `W0718` - broad-exception-caught
- `C0415` - import-outside-toplevel

### Key Settings

- **Line length**: 120 characters (increased from default 100)
- **Max arguments**: 8 (increased from default 5)
- **Max attributes**: 12 (increased from default 7)
- **Python version**: 3.12
- **Multiprocessing**: Enabled for faster linting

## Usage

### Using Poetry (Recommended)

```bash
# Run pylint on the entire eventy package
poetry run pylint eventy

# Run with specific output format
poetry run pylint eventy --output-format=text --reports=no
```

### Using the Lint Script

A convenience script is provided for easy linting:

```bash
# Make the script executable (first time only)
chmod +x scripts/lint.py

# Run the linter
python scripts/lint.py
```

### Integration with IDEs

Most IDEs can be configured to use the pylint configuration from `pyproject.toml`:

#### VS Code
Install the Python extension and pylint will automatically use the configuration.

#### PyCharm
1. Go to Settings → Tools → External Tools
2. Add pylint with the command: `poetry run pylint`
3. Set arguments to: `$FilePath$`

## Remaining Issues

The current pylint run shows some legitimate issues that should be addressed by developers:

- **E1120/E1123**: Constructor argument mismatches in filesystem modules
- **E1101**: Missing member attributes (likely due to dynamic attribute assignment)
- **C0304/C0328**: File formatting issues (missing newlines, line endings)

These issues represent actual code problems rather than style preferences and should be fixed rather than suppressed.

## Customization

To modify the pylint configuration:

1. Edit the `[tool.pylint.*]` sections in `pyproject.toml`
2. Test the changes: `poetry run pylint eventy`
3. Adjust the `fail-under` score if needed

## CI/CD Integration

To integrate pylint into your CI/CD pipeline:

```yaml
# Example GitHub Actions step
- name: Run Pylint
  run: |
    poetry install
    poetry run pylint eventy --fail-under=8.0
```

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure all dependencies are installed with `poetry install`
2. **Module not found**: Run pylint from the project root directory
3. **Configuration not loaded**: Verify `pyproject.toml` is in the current directory

### Getting Help

- Pylint documentation: https://pylint.pycqa.org/
- Configuration reference: https://pylint.pycqa.org/en/latest/user_guide/configuration/all-options.html