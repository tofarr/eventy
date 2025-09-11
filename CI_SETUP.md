# CI/CD Setup

This repository is configured with GitHub Actions to automatically run tests and enforce code coverage requirements.

## Continuous Integration

### Workflow Triggers
- **Push to main branch**: Runs full test suite with coverage
- **Pull requests to main**: Runs full test suite with coverage

### Requirements
- **Python Version**: 3.12
- **Test Framework**: pytest
- **Minimum Code Coverage**: 66%
- **Coverage Tool**: pytest-cov

### Running Tests Locally

```bash
# Run tests without coverage
make test

# Run tests with coverage report
make test-cov

# Run tests with coverage requirement (will fail if < 66%)
make test-cov-fail
```

### Coverage Configuration

The coverage configuration is defined in `pyproject.toml`:

- **Source**: `eventy` package
- **Minimum Coverage**: 66%
- **Excluded Files**: 
  - Test files
  - `watchdog_file_event_queue.py` (optional dependency)
  - Cache and build directories

### CI Workflow

The GitHub Actions workflow (`.github/workflows/ci.yml`) performs the following steps:

1. **Setup**: Checkout code and setup Python 3.12
2. **Dependencies**: Install Poetry and project dependencies with caching
3. **Testing**: Run pytest with coverage requirements
4. **Coverage**: Upload coverage reports to Codecov (optional)

### Coverage Failure

If the test coverage falls below 66%, the CI will fail with an error message:
```
ERROR: Coverage failure: total of XX is less than fail-under=66
```

This ensures that all code contributions maintain the minimum quality standards.