Pre-Configured Handlers
=======================

This is an example to use [pre-configured handlers](https://github.com/nownabe/go-bqloader/tree/main/contrib/handlers).

Create your .envrc file:

```bash
cp .envrc.example .envrc
vi .envrc
```

(direnv is not necessary.)

Deploy to Cloud Function:

```bash
./deploy.sh
```

Upload CSVs and run the function.

```bash
./run.sh
```

Check BigQuery.
