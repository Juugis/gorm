package db

import (
	"fmt"
	"strconv"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type GormPostgresDB struct {
	db             *gorm.DB
	usingTimescale bool
	name           string
}

func NewGormPostgresDB(name, host string, port int, username, password, dbname string, usingTimescale bool) (*GormPostgresDB, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=UTC", host, username, password, dbname, port)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %v", err)
	}

	return &GormPostgresDB{
		name:           name,
		db:             db,
		usingTimescale: usingTimescale,
	}, nil
}

func (db *GormPostgresDB) GetName() string {
	return db.name
}

func (db *GormPostgresDB) Setup() error {
	// Drop table if exists and create new one
	if err := db.db.Migrator().DropTable(&DataObject{}); err != nil {
		return err
	}

	if err := db.db.AutoMigrate(&DataObject{}); err != nil {
		return err
	}

	// If using Timescale, set up hypertable and compression
	if db.usingTimescale {
		dbName := &gorm.DB{}
		db.db.Raw(fmt.Sprintf(`SELECT create_hypertable('%v', 'start_time', chunk_time_interval => interval '60 days');`, dbName))
		db.db.Exec(fmt.Sprintf(`ALTER TABLE %v SET (timescaledb.compress);`, dbName))
	}

	return nil
}

func (db *GormPostgresDB) Close() error {
	sqlDB, err := db.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (db *GormPostgresDB) UpsertSingle(docs []DataObject) error {
	query := `
		INSERT INTO ` + DB_TABLE_NAME + ` (created_at, updated_at, start_time, interval, area, source, value)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (start_time, interval, area) DO UPDATE
		SET updated_at = EXCLUDED.updated_at, source = EXCLUDED.source, value = EXCLUDED.value`

	for _, doc := range docs {
		if err := db.db.Exec(query, doc.CreatedAt, doc.UpdatedAt, doc.StartTime, doc.Interval, doc.Area, doc.Source, doc.Value).Error; err != nil {
			return fmt.Errorf("UpsertSingle: %v", err)
		}
	}

	return nil
}

func (db *GormPostgresDB) UpsertBulk(docs []DataObject) error {
	if len(docs) == 0 {
		return nil // Nothing to upsert
	}

	// Prepare the base query
	query := fmt.Sprintf(`
        INSERT INTO %s (created_at, updated_at, start_time, interval, area, source, value)
        VALUES `, DB_TABLE_NAME)

	// Prepare the value placeholders and arguments
	var valueStrings []string
	var values []interface{}

	for _, doc := range docs {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?)")
		values = append(values, doc.CreatedAt, doc.UpdatedAt, doc.StartTime, doc.Interval, doc.Area, doc.Source, doc.Value)
	}

	// Join the placeholders into a single string
	query += strings.Join(valueStrings, ", ")
	query += `
        ON CONFLICT (start_time, interval, area) DO UPDATE
        SET updated_at = EXCLUDED.updated_at, source = EXCLUDED.source, value = EXCLUDED.value`

	// Execute the query
	return db.db.Exec(query, values...).Error
}

func (db *GormPostgresDB) GetOrderedWithLimit(limit int) ([]DataObject, error) {
	query := fmt.Sprintf(`SELECT * FROM %v ORDER BY start_time DESC LIMIT %v`, DB_TABLE_NAME, limit)
	var results []DataObject
	db.db.Raw(query).Scan(&results)
	return results, nil
}

func (db *GormPostgresDB) TableSizeInKB() (int, error) {
	var totalSize string

	var query string
	if db.usingTimescale {
		query = `SELECT hypertable_size($1) AS total_size;`
	} else {
		query = `SELECT pg_total_relation_size($1) AS total_size;`
	}

	err := db.db.Raw(query, DB_TABLE_NAME).Scan(&totalSize).Error
	if err != nil {
		return 0, err
	}

	sizeInBytes, err := strconv.Atoi(totalSize)
	if err != nil {
		return 0, err
	}

	return sizeInBytes / 1024, nil
}
