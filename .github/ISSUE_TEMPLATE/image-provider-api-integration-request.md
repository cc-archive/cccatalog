---
name: Image provider API integration request
about: Tell us about an API providing CC-licensed images
labels: "🛠 goal: addition, 🚦 status: awaiting triage, 💻 aspect: code, 🟩 priority: low"
title: "[API integration] <Replace this with actual title>"
---

## Description
<!-- Concisely describe the image provider. -->

## API endpoint / documentation
<!-- Provide links to the API endpoint, and associated documentation. -->

## Licenses provided
<!-- Which CC licenses or Public Domain tools are in use by the source, if known? -->

## Technical details
<!-- Please provide any technical details that might be useful for -->
<!-- implementation, e.g., rate limits, filtering options, overall volume, -->
<!-- etc. -->

## Checklist
<!-- Do not modify this section. -->

No development should be done on a provider API script until the following info is gathered.

<!-- Replace  the [ ] with [x] to check the boxes. --> 
- [ ] Verify there is a way to retrieve the entire relevant portion of the provider's collection in a systematic way via their API.
- [ ] Verify the API provides license info viz. license type and version (license URL provides both, and is preferred).
- [ ] Verify the API provides stable direct links to individual works.
- [ ] Verify the API provides a stable landing page URL to individual works.
- [ ] Note other info the API provides, such as thumbnails, dimensions, attribution info (required if non-CC0 licenses will be kept), title, description, other meta data, tags, etc.
- [ ] Attach example responses to API queries that have the relevant info.

## Implementation guidelines
<!-- You must read and understand the following attestation. -->

<details>
<summary>Implementation guidelines</summary>

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

### Examples
<!-- Do not modify this section. -->

For example provider API scripts and accompanying test suites, please see any 
of the following pairs.

- `src/cc_catalog_airflow/dags/provider_api_scripts/flickr.py` and
  `src/cc_catalog_airflow/dags/provider_api_scripts/test_flickr.py`
- `src/cc_catalog_airflow/dags/provider_api_scripts/wikimedia_commons.py` and
  `src/cc_catalog_airflow/dags/provider_api_scripts/test_wikimedia_commons.py`

</details>