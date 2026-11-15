# Contributing

Thank you for your interest in Discus.

## Pull request titles

Discus uses [semVersie](https://github.com/RonaldPhilipsen/semVersie) to
calculate the next version. The tool reads the title of each merged pull
request, so the title must follow the conventional commits format.

- `fix: ...` raises the patch version.
- `feat: ...` raises the minor version.
- A `!` after the type, for example `feat!: ...`, raises the major version.
- The text `BREAKING CHANGE` anywhere in the pull request body also raises the
  major version.
- Other types, such as `docs:`, `test:`, and `chore:`, do not release.

semVersie has no special rule for versions below 1.0. From `0.x`, a major
raise gives `1.0.0`.

## Development

Run these commands before you open a pull request:

```sh
mise run lint
mise run test
mise run deny
```

`mise run deny` runs `cargo deny check` with the policy in `deny.toml`. It fails
when a dependency has a RustSec advisory, a license that is not on the allow
list, or a source other than crates.io.

## Releases

When a pull request with a version-raising title merges into `main`, the
release workflow runs. It calculates the version and builds two Docker images.
It pushes them to `ghcr.io/ronaldphilipsen/discus` with the version as tag, and
with the version and the suffix `-pyroscope`. The second image contains the
CPU profiler for benchmarks. The workflow then pushes the Helm chart and
creates a GitHub release.

### Release candidates

A release candidate is a test release of an open pull request. To publish one,
add the label `release-candidate` to the pull request. The title must raise the
version, so a `docs:` or `chore:` pull request publishes nothing. The release
workflow pushes the two images and the chart with the next version and an `-rc`
suffix. It also creates a Git tag on the head of the pull request and a GitHub
release that is marked as a prerelease.

For example, a `feat:` pull request after `0.4.9` publishes `0.5.0-rc0`. Each
new push to the pull request publishes the next number: `0.5.0-rc1`, then
`0.5.0-rc2`. semVersie takes the number from the existing `v0.5.0-rc` tags.

When semVersie looks for the last release, it skips prerelease tags. Thus the
merge of the pull request releases `0.5.0`.

A release candidate waits for CI. The job `Wait for CI` waits until the CI run
of the head commit of the pull request completes. When that run fails, the
release workflow stops, and it publishes no image, chart, tag, or release.

### Verify a release

Each image has an SBOM (a list of the software in the image) and a provenance
attestation (a record of how the image was built). The release workflow signs
both images and the chart with cosign keyless signing. Keyless signing uses no
private key. Sigstore gives the workflow a short-lived certificate for its
GitHub identity, and it records each signature in a public log. Older
releases have no signature.

To verify a release, use cosign 3. The certificate identity is the release
workflow on `main`:

```sh
workflow=https://github.com/RonaldPhilipsen/Discus/.github/workflows/release.yaml
issuer=https://token.actions.githubusercontent.com
cosign verify ghcr.io/ronaldphilipsen/discus:0.7.1 \
  --certificate-identity "$workflow@refs/heads/main" --certificate-oidc-issuer "$issuer"
cosign verify ghcr.io/ronaldphilipsen/charts/discus:0.7.1 \
  --certificate-identity "$workflow@refs/heads/main" --certificate-oidc-issuer "$issuer"
```

The image with the suffix `-pyroscope` uses the same command. A release
candidate runs in a pull request, so its identity ends in
`@refs/pull/<number>/merge`. To verify a release candidate, use the number of
the pull request, for example `@refs/pull/97/merge`.

To read the SBOM or the provenance of an image, use `docker buildx imagetools`:

```sh
image=ghcr.io/ronaldphilipsen/discus:0.7.1
docker buildx imagetools inspect "$image" --format '{{ json .SBOM }}'
docker buildx imagetools inspect "$image" --format '{{ json .Provenance }}'
```
