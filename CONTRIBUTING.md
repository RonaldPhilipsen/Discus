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
```

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
