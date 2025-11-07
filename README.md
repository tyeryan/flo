# flo

Go-based parser that ingests NEM12 interval meter data and stores the readings in PostgreSQL.

## Prerequisites

- Go 1.22+
- PostgreSQL 13+ running locally on port `5432`
- `pgcrypto` extension enabled in the target database (required for `gen_random_uuid()`)
- NEM12-formatted input file located in the project root (defaults to `test.csv`)

## Running the parser

```
go run main.go
```

Optionally specify a different file with the `-file` flag:

```
go run main.go -file test.csv
```

The application performs the following steps:

- Connects to PostgreSQL using the DSN defined in `main.go`.
- Creates the `meter_readings` table if it does not exist.
- Parses the NEM12 file specified via `-file` (default `test.csv`).
- Upserts meter interval readings into the database.

Adjust `main.go` if you need to change the database DSN or the default input file path.
