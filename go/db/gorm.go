package db

import (
	"fmt"
	"strconv"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	for _, doc := range docs {
		if err := db.db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "start_time"}, {Name: "interval"}, {Name: "area"}},
			DoUpdates: clause.AssignmentColumns([]string{"updated_at", "source", "value"}),
		}).Create(&doc).Error; err != nil {
			return err
		}
	}
	return nil
}

func (db *GormPostgresDB) UpsertBulk(docs []DataObject) error {
	if len(docs) == 0 {
		return nil // Nothing to upsert
	}
	return db.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "start_time"}, {Name: "interval"}, {Name: "area"}},
		DoUpdates: clause.AssignmentColumns([]string{"updated_at", "source", "value"}),
	}).CreateInBatches(docs, 4000).Error
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
