# Contributing to CC Open Source

Thank you for your interest in contributing to CC Open Source! This document is
a set of guidelines to help you contribute to this project.

## Code of Conduct

By participating in this project, you are expected to uphold our [Code of
Conduct][code_of_conduct].

[code_of_conduct]:https://creativecommons.github.io/community/code-of-conduct/

## Project Documentation

The `README` in the root of the repository should contain or link to
project documentation. If you cannot find the documentation you're
looking for, please file a GitHub issue with details of what
you'd like to see documented.

## How to Contribute

Please follow the processes in our general [Contributing Code][contributing]
guidelines on the Creative Common Open Source website.

[contributing]:https://creativecommons.github.io/contributing-code/

### Image Provider API Integration Guidelines

- The script should be in the `src/cc_catalog_airflow/dags/provider_api_scripts/` directory.
  The script should have a test suite in the same directory.
- The script must use the `ImageStore` class.
  (Import this from `src/cc_catalog_airflow/dags/provider_api_scripts/common/storage/image.py`.)
- The script should use the `DelayedRequester` class.
  (Import this from `src/cc_catalog_airflow/dags/provider_api_scripts/common/requester.py`.)
- The script must not use anything from `src/cc_catalog_airflow/dags/provider_api_scripts/modules/etlMods.py`.
  That module is deprecated.
- If the provider API has can be queried by 'upload date' or something similar,
  the script should take a `--date` parameter when run as a script, giving the
  date for which we should collect images. The form should be `YYYY-MM-DD` (so,
  the script can be run via `python my_favorite_provider.py --date 2018-01-01`).
- The script must provide a main function that takes the same parameters as from
  the CLI. In our example from above, we'd then have a main function
  `my_favorite_provider.main(date)`. The main should do the same thing calling
  from the CLI would do.
- The script *must* conform to [PEP8](https://www.python.org/dev/peps/pep-0008/).
  Please use `pycodestyle` (available via `pip install pycodestyle`) to check for compliance.
- The script should use small, testable functions.
- The test suite for the script may break PEP8 rules regarding long lines where
  appropriate (e.g., long strings for testing).

#### Examples
<!-- Do not modify this section. -->

For example provider API scripts and accompanying test suites, please see any
of the following pairs.

- `src/cc_catalog_airflow/dags/provider_api_scripts/flickr.py` and
  `src/cc_catalog_airflow/dags/provider_api_scripts/test_flickr.py`
- `src/cc_catalog_airflow/dags/provider_api_scripts/wikimedia_commons.py` and
  `src/cc_catalog_airflow/dags/provider_api_scripts/test_wikimedia_commons.py`

## Questions or Thoughts?

Talk to us on [one of our community forums][community].

[community]:https://creativecommons.github.io/community/
