#!/usr/bin/env bash
# Re-vendors Swagger UI into internal/httpapi/swaggerui/ (#54, ADR 0016) from the npm package swagger-ui-dist.
# Run it via `make swagger-ui SWAGGER_UI_VERSION=x.y.z`, which is the single place the version is pinned.
#
# It downloads the package tarball, checks its sha512 against the registry's dist.integrity, and copies only the
# allowlisted files, then writes VERSION and SHA256SUMS. It never touches our own index.html and init.js. On a bad
# version, an HTTP error, an integrity mismatch or a missing file it exits non-zero and writes nothing. The new set
# is assembled in a sibling directory and swapped in with renames, so a failure while copying never leaves a mix
# of old and new files.
# Needs only curl, tar, openssl and sha256sum (or shasum); no Node or npm.
#
# Environment (for tests):
#   SWAGGER_UI_REGISTRY  npm registry base URL (default https://registry.npmjs.org)
#   SWAGGER_UI_DIR       destination directory (default internal/httpapi/swaggerui of this repository)
set -euo pipefail

die() {
  echo "update-swagger-ui: $*" >&2
  exit 1
}

version=${1:-}
registry=${SWAGGER_UI_REGISTRY:-https://registry.npmjs.org}
dest=${SWAGGER_UI_DIR:-$(cd "$(dirname "$0")/.." && pwd)/internal/httpapi/swaggerui}
# The files served or required for attribution. Source maps, the ES bundles and the standalone preset are not
# needed by our index.html (BaseLayout).
files=(swagger-ui-bundle.js swagger-ui.css favicon-32x32.png favicon-16x16.png
  LICENSE NOTICE swagger-ui-bundle.js.LICENSE.txt)

[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "usage: $0 <version>, e.g. 5.34.0"
[ -d "$dest" ] || die "destination $dest does not exist"
dest=$(cd "$dest" && pwd)
parent=$(dirname "$dest")
new="$parent/.$(basename "$dest").new.$$"
old="$parent/.$(basename "$dest").old.$$"

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$@"; }
else
  sha256() { shasum -a 256 "$@"; }
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp" "$new"' EXIT

meta_url="$registry/swagger-ui-dist/$version"
curl -fsSL -o "$tmp/meta.json" "$meta_url" || die "cannot get $meta_url (unknown version?)"
integrity=$(tr -d '\n' <"$tmp/meta.json" |
  sed -n 's|.*"integrity"[[:space:]]*:[[:space:]]*"\(sha512-[A-Za-z0-9+/=]*\)".*|\1|p')
[ -n "$integrity" ] || die "no sha512 dist.integrity in $meta_url"

tgz_url="$registry/swagger-ui-dist/-/swagger-ui-dist-$version.tgz"
curl -fsSL -o "$tmp/package.tgz" "$tgz_url" || die "cannot download $tgz_url"
actual="sha512-$(openssl dgst -sha512 -binary "$tmp/package.tgz" | openssl base64 -A)"
[ "$actual" = "$integrity" ] || die "integrity mismatch for $tgz_url: registry says $integrity, got $actual"

mkdir "$tmp/x"
tar -xzf "$tmp/package.tgz" -C "$tmp/x" "${files[@]/#/package/}" || die "the package lacks an expected file"
stage="$tmp/x/package"
for f in "${files[@]}"; do
  [ -f "$stage/$f" ] && [ ! -L "$stage/$f" ] || die "$f in the package is not a regular file"
done
printf '%s\n' "$version" >"$stage/VERSION"
(cd "$stage" && sha256 "${files[@]}") >"$stage/SHA256SUMS"

# Assemble the complete new directory next to the old one (same file system, so the renames are atomic): a
# copy of the current one, which keeps our index.html and init.js, with the vendored files replaced.
cp -Rp "$dest" "$new" || die "cannot stage $new"
for f in "${files[@]}" VERSION SHA256SUMS; do
  cp "$stage/$f" "$new/$f" || die "cannot stage $f"
done
mv "$dest" "$old" || die "cannot move $dest aside"
if ! mv "$new" "$dest"; then
  mv "$old" "$dest"
  die "cannot move the new files into $dest; the old ones were restored"
fi
rm -rf "$old"
echo "update-swagger-ui: vendored swagger-ui-dist $version into $dest"
