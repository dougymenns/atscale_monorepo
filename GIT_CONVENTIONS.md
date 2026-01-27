# Git Branch Naming Convention

## Format

```
<type>/<integration>/<service>/<description>
```

## Branch Types

- `feature` - New functionality
- `fix` - Bug fixes
- `hotfix` - Critical production fixes
- `chore` - Maintenance tasks, dependency updates
- `refactor` - Code restructuring
- `docs` - Documentation updates

## Integrations

- `connecteam` - Connecteam integrations
- `everee` - Everee integrations
- `fountain` - Fountain integrations
- `external` - External integrations
- `shared` - Cross-cutting changes

## Examples

```
feature/connecteam/timesheets/add-retry-logic
fix/everee/workers/fix-api-timeout-handling
chore/external/workers_gsheets/update-dependencies
hotfix/fountain/fountain-users/fix-signature-verification
```

## Alternative (Shorter)

```
<type>/<integration>-<service>/<description>
```

Example: `feature/connecteam-timesheets/add-retry-logic`

---

# Commit Message Convention

## Format

```
<type>(<scope>): <subject>
```

## Commit Types

- `feat` - New feature
- `fix` - Bug fix
- `hotfix` - Critical production fix
- `chore` - Maintenance (dependencies, config)
- `refactor` - Code restructuring
- `docs` - Documentation changes
- `perf` - Performance improvements
- `test` - Adding/updating tests

## Scope

Use integration/service name: `connecteam`, `everee`, `fountain`, `external`, or specific service like `connecteam-timesheets`, `everee-workers`.

## Examples

```
feat(connecteam-timesheets): add retry logic for webhook processing
fix(everee-workers): handle API timeout errors gracefully
chore(external-gsheets): update google-api-client to v2.0
hotfix(fountain-users): fix signature verification bug
refactor(connecteam-workers): improve error handling in user sync
docs(shared): update README with new directory structure
```

## Guidelines

- Subject: imperative mood, lowercase, no period, ~50 characters
- Body (optional): explain what and why, wrap at 72 characters
- Footer (optional): reference issues (e.g., `Fixes #123`)
