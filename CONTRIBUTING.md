# Contributing

Thank you for your interest in Discus.

## Pull request titles

Discus uses [semVersie](https://github.com/RonaldPhilipsen/semVersie) to
calculate the next version. The tool reads the title of each merged pull
request, so the title must follow the conventional commits format.

- `fix: ...` raises the patch version.
- `feat: ...` raises the minor version.
- A `!` after the type, for example `feat!: ...`, raises the major version.
- Other types, such as `docs:`, `test:`, and `chore:`, do not release.

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
add the label `release-candidate` to the pull request. The release workflow
then pushes the two images and the chart with a version such as `0.5.0-rc0`.
Each new push to the pull request publishes the same version again.

A release candidate gets no Git tag and no GitHub release. semVersie reads the
newest tag as the last release. After a tag `v0.5.0-rc0`, it would calculate
`0.6.0` for the release of the same pull request.
