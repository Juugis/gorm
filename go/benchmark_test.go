package main

import (
	"fmt"
	"testing"
	"time"
	"timeseries-benchmark/db"
)

func BenchmarkTimeseries(b *testing.B) {
	mongo, err := db.NewMongoDB("mongodb", "localhost", db.PORT_MONGO, db.DB_USERNAME, db.DB_PASSWORD)
	if err != nil {
		b.Fatalf("Error: %v", err)
	}
	defer mongo.Close()

	pgNative, err := db.NewPostgresDB("pg-ntv", "localhost", db.PORT_POSTGRES, db.DB_USERNAME, db.DB_PASSWORD, db.DB_NAME, false)
	if err != nil {
		b.Fatalf("Error: %v", err)
	}
	defer pgNative.Close()

	pgTimescale, err := db.NewPostgresDB("pg-tsc", "localhost", db.PORT_TIMESCALE, db.DB_USERNAME, db.DB_PASSWORD, db.DB_NAME, true)
	if err != nil {
		b.Fatalf("Error: %v", err)
	}
	defer pgTimescale.Close()

	NUM_OBJECTS := 10_000

	fake := db.GenerateFakeData(NUM_OBJECTS)

	var dbs []db.Database
	dbs = append(dbs, mongo)
	dbs = append(dbs, pgNative)
	dbs = append(dbs, pgTimescale)

	// Initialize all of the dbs only once
	for _, dbInstance := range dbs {
		if err := dbInstance.Setup(); err != nil {
			b.Fatalf("Error: %v", err)
		}
	}

	for _, dbInstance := range dbs {
		b.Run(fmt.Sprintf("%v-upsert-single", dbInstance.GetName()), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := dbInstance.UpsertSingle(fake); err != nil {
					b.Fatalf("Error: %v", err)
				}
			}
		})
	}

	for _, dbInstance := range dbs {
		b.Run(fmt.Sprintf("%v-upsert-bulk", dbInstance.GetName()), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := dbInstance.UpsertBulk(fake); err != nil {
					b.Fatalf("Error: %v", err)
				}
			}
		})
	}

	if err := pgTimescale.ExecManualCompression(); err != nil {
		b.Fatalf("Error: %v", err)
	}

	for _, dbInstance := range dbs {
		const READ_LIMIT = 1000
		b.Run(fmt.Sprintf("%v-get-%v", dbInstance.GetName(), READ_LIMIT), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				docs, err := dbInstance.GetOrderedWithLimit(READ_LIMIT)
				if err != nil {
					b.Fatalf("Error: %v", err)
				}
				if len(docs) != READ_LIMIT {
					b.Fatalf("Expected %v docs, got %v", READ_LIMIT, len(docs))
				}
			}
		})
	}

	sleepTime := 30 * time.Second
	b.Logf("Sleeping for %v sec to get the correct mongodb collection storage size\n", sleepTime.Seconds())
	time.Sleep(sleepTime)

	b.Logf(" * storage size for %v rows", NUM_OBJECTS)
	for _, dbInstance := range dbs {
		size, err := dbInstance.TableSizeInKB()
		if err != nil {
			b.Fatalf("Error: %v", err)
		}

		b.Logf("	- %v: %v KB\n", dbInstance.GetName(), size)
	}
}

/*

## run compression

SELECT compress_chunk(chunk_schema || '.' || chunk_name)
	FROM timescaledb_information.chunks
	WHERE hypertable_name = 'data_objects';


SELECT hypertable_size('data_objects') AS total_size;


SELECT add_compression_policy('data_objects', INTERVAL '30 days');


*/