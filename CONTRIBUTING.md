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
release workflow runs. It calculates the version, builds the Docker image,
pushes it to `ghcr.io/ronaldphilipsen/discus` with the version as tag, and
creates a GitHub release.
