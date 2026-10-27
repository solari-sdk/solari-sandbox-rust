# Changelog

## 0.3.1

### Added
- `ClientOptions::with_api_key(key)` and `DEFAULT_BASE_URL` (`https://api.getsolari.com`).

### Changed
- An empty `base_url` now means the hosted Solari API, matching the TypeScript and Python SDKs.
  `ClientOptions::new(key, base_url)` with an explicit URL behaves as before.
