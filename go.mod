module messenger-agent

go 1.23

require (
	github.com/jackc/pgx/v5 v5.7.2
	github.com/pgvector/pgvector-go v0.2.2
	github.com/redis/go-redis/v9 v9.7.0
)

replace (
	github.com/jackc/pgx/v5 => ./third_party/pgx
	github.com/pgvector/pgvector-go => ./third_party/pgvector-go
	github.com/redis/go-redis/v9 => ./third_party/go-redis
)
