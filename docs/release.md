# Releases

`pgboss-effect` uses [Changesets](https://github.com/changesets/changesets) and
npm trusted publishing.

## Release flow

1. Add a changeset:

   ```sh
   pnpm changeset
   ```

   Choose the bump type and describe the change.

2. Commit the changeset with your change and open a PR.
   - `main` is protected: PRs need code-owner approval and a passing
     `checks` run.

3. Merge the PR. The `Release` workflow opens or updates a `chore: release`
   PR.

4. Review the version PR.
   - If GitHub marks the CI run as `action_required` due to the
     first-time-contributor policy for the Changesets bot, approve the
     workflow run.
   - Approve the version PR as code owner.

5. Merge the version PR.

6. The `Release` workflow runs again, publishes the package with trusted
   publishing (OIDC), and creates the `vX.Y.Z` tag and GitHub release.

7. Verify the release:

   ```sh
   npm view pgboss-effect dist-tags
   npm view pgboss-effect@<version> dist.attestations
   gh release view v<version>
   ```

## What the Release workflow does

- Installs with a frozen lockfile.
- Runs `pnpm check`, unit tests, and Postgres integration tests.
- Runs `changesets/action@v2`:
  - With changesets: opens or updates the version PR.
  - Without changesets: publishes any unpublished version with
    `pnpm run release`.
- Uses npm trusted publishing; no long-lived `NPM_TOKEN`.
- Creates the version tag and GitHub release.

## Branch protection

`main` requires:

- A pull request.
- One code-owner approval from `@erikputar`.
- A passing `checks` status check with the branch up to date.
- Conversation resolution and linear history.
- Squash merges only.

The repository admin can bypass protection for their own PRs because GitHub
does not allow self-approval. External PRs still require code-owner approval.

## Version PR notes

- The version PR is created by `github-actions[bot]`.
- Because of the repository's first-time-contributor approval policy, its CI
  run can start as `action_required`.
- Approve the workflow run, wait for `checks`, then approve and merge the PR.
- The version PR updates `package.json`, `CHANGELOG.md`, and removes the
  consumed changeset.

## Manual fallback

If the workflow is unavailable:

```sh
pnpm run version
git add .
git commit -m "chore: release"
pnpm run release
git push --follow-tags
```

Manual publishing requires interactive 2FA. Create the GitHub release
separately with `gh release create`.

## Troubleshooting

- Use `pnpm run version` and `pnpm run release`; bare `pnpm version` is pnpm's
  built-in command.
- Trusted publishing requires a matching GitHub repository, workflow filename
  (`release.yml`), and `id-token: write`.
- A new trusted publisher configuration expires if it does not complete a
  successful publish within 2 days.
- Provenance is generated only for public packages from public repositories
  via trusted publishing.
- `.changeset/**` is excluded from Biome because Biome does not understand
  Changesets frontmatter.

## Security

- No long-lived npm tokens are stored in GitHub.
- Trusted publishing uses short-lived OIDC credentials.
- Recommended package setting: **Require two-factor authentication and
  disallow tokens**.

  ```sh
  npm access set mfa=publish pgboss-effect
  ```

- Review changesets and version PRs before merging.
