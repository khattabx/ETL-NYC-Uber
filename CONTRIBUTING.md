# Contributing

Thanks for considering a contribution! Follow the steps below to keep the workflow consistent and clean.

## 1. Fork the repository (Git SSH)

- Fork the repo on GitHub.
- Clone your fork using SSH (preferred):

```sh
git clone git@github.com:<your-username>/ETL-NYC-Uber.git
```

## 2. Add the upstream remote

Add the original repository as upstream:

```sh
git remote add upstream git@github.com:khattabx/ETL-NYC-Uber.git
```

Sync your fork with upstream main when needed:

```sh
git fetch upstream
git checkout main
git merge upstream/main
```

## 3. Create a feature branch

Create a branch off main:

```sh
git checkout -b feat/short-description
```

## 4. Make changes and commit

- Keep commits focused and descriptive.
- Example:

```sh
git commit -m "docs(readme): add contributing guide"
```

## 5. Push to your fork

```sh
git push origin feat/short-description
```

## 6. Open a Pull Request (PR)

From your fork, open a PR to the upstream main branch:
- Provide a clear title and description.
- Link related issues if applicable.
- Add screenshots or logs if they help review.

## PR naming convention

Use the following format for PR titles:

```sh
type(scope): short summary
```

Types:

```sh
feat
fix
docs
chore
refactor
test
```

Examples:

```sh
docs(readme): add contributing guide
feat(spark): add ETL transformation job
fix(airflow): correct DAG schedule
```

---

If you’re unsure about anything, open a draft PR early and ask for feedback.
