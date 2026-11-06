#!/usr/bin/env bash
# Fails when a vendored file differs from SOURCE.txt (the copy of the Solari conformance suite at the pinned sha), or when a
# vendored file is not listed. Offline. To repin, copy the files from the new desktop sha and run:  ./check-source.sh --write <sha>
set -euo pipefail
cd "$(dirname "$0")"
FILES=(README.md mock-server.mjs lib/mock-server.mjs lib/spec.mjs schema/row.schema.json spec/errors.json spec/gateway-responses.json spec/retry.json spec/wire.json)
if [[ ${1:-} == --write ]]; then
  { echo "desktop ${2:?usage: --write <sha>}"; sha256sum "${FILES[@]}" | sort -k2; } > SOURCE.txt
  echo "SOURCE.txt rewritten"; exit 0
fi
head -1 SOURCE.txt | grep -q '^desktop [0-9a-f]\{8,\}$' || { echo "SOURCE.txt: first line must be 'desktop <sha>'"; exit 1; }
tail -n +2 SOURCE.txt | sha256sum -c --quiet - || { echo "a vendored conformance file differs from SOURCE.txt: copy it again from the conformance suite at $(head -1 SOURCE.txt | cut -d' ' -f2)"; exit 1; }
listed=$(tail -n +2 SOURCE.txt | awk '{print $2}' | sort)
actual=$(printf '%s\n' "${FILES[@]}" | sort)
[[ $listed == "$actual" ]] || { echo "SOURCE.txt lists different files than check-source.sh expects"; exit 1; }
echo "conformance copy matches $(head -1 SOURCE.txt)"
