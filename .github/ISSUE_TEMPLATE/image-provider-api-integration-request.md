---
name: Image provider API integration request
about: Tell us about an API providing CC-licensed images
labels: "🛠 goal: addition, 🚦 status: awaiting triage, 🗂 aspect: research, 🟩 priority: low"
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

<!-- Replace the [ ] with [x] to check the boxes. -->
- [ ] Verify there is a way to retrieve the entire relevant portion of the provider's collection in a systematic way via their API.
- [ ] Verify the API provides license info viz. license type and version (license URL provides both, and is preferred).
- [ ] Verify the API provides stable direct links to individual works.
- [ ] Verify the API provides a stable landing page URL to individual works.
- [ ] Note other info the API provides, such as thumbnails, dimensions, attribution info (required if non-CC0 licenses will be kept), title, description, other meta data, tags, etc.
- [ ] Attach example responses to API queries that have the relevant info.

## Implementation guidelines
<!-- You must read and understand the following attestation. -->

<!-- Replace the [ ] with [x] to check the box. -->
- [ ] I have read and understood the [image provider API integration guidelines][guidelines].

[guidelines]:https://github.com/creativecommons/cccatalog/blob/master/CONTRIBUTING.md#image-provider-api-integration-guidelines