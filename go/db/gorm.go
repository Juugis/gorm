package db

import (
	"fmt"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type GormDataObject struct {
	created_at time.Time `gorm:"not null"`
	updated_at time.Time `gorm:"not null"`
	start_time time.Time `gorm:"not null;index;primaryKey"`
	interval   int64     `gorm:"not null;primaryKey"`
	area       string    `gorm:"not null;primaryKey"`
	source     string    `gorm:"not null"`
	value      float64   `gorm:"not null"`
}

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
	if err := db.db.Migrator().DropTable(&GormDataObject{}); err != nil {
		return err
	}

	if err := db.db.AutoMigrate(&GormDataObject{}); err != nil {
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

// USING GORM BUILTINS IS SLOWER THAN RAW QUERIES
// func (db *GormPostgresDB) UpsertSingle(docs []DataObject) error {
// 	for _, doc := range docs {
// 		if err := db.db.Clauses(clause.OnConflict{
// 			Columns:   []clause.Column{{Name: "start_time"}, {Name: "interval"}, {Name: "area"}},
// 			DoUpdates: clause.AssignmentColumns([]string{"updated_at", "source", "value"}),
// 		}).Create(&doc).Error; err != nil {
// 			return fmt.Errorf("UpsertSingle: %v", err)
// 		}
// 	}
// 	return nil
// }

// USING GORM BUILTINS IS SLOWER THAN RAW QUERIES
// func (db *GormPostgresDB) UpsertBulk(docs []DataObject) error {
// 	err := db.db.Clauses(clause.OnConflict{
// 		Columns:   []clause.Column{{Name: "start_time"}, {Name: "interval"}, {Name: "area"}},
// 		DoUpdates: clause.AssignmentColumns([]string{"updated_at", "source", "value"}),
// 	}).Create(&docs).Error

// 	if err != nil {
// 		return fmt.Errorf("UpsertBulk: %v", err)
// 	}

// 	return nil
// }

// USING GORM BUILTINS IS SLOWER THAN RAW QUERIES
// func (db *GormPostgresDB) GetOrderedWithLimit(limit int) ([]DataObject, error) {
// 	var results []DataObject
// 	err := db.db.Order("start_time desc").Limit(limit).Find(&results).Error
// 	if err != nil {
// 		return nil, err
// 	}
// 	return results, nil
// }

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
	query := `
		INSERT INTO ` + DB_TABLE_NAME + ` (created_at, updated_at, start_time, interval, area, source, value)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (start_time, interval, area) DO UPDATE
		SET updated_at = EXCLUDED.updated_at, source = EXCLUDED.source, value = EXCLUDED.value`

	tx := db.db.Begin()
	for _, doc := range docs {
		if err := tx.Exec(query, doc.CreatedAt, doc.UpdatedAt, doc.StartTime, doc.Interval, doc.Area, doc.Source, doc.Value).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("UpsertBulk: %v", err)
		}
	}
	tx.Commit()

	return nil
}

func (db *GormPostgresDB) GetOrderedWithLimit(limit int) ([]DataObject, error) {
	query := fmt.Sprintf(`SELECT * FROM %v ORDER BY start_time DESC LIMIT %v`, DB_TABLE_NAME, limit)

	rows, err := db.db.Raw(query).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []DataObject
	for rows.Next() {
		var obj DataObject
		if err := rows.Scan(&obj.CreatedAt, &obj.UpdatedAt, &obj.StartTime, &obj.Interval, &obj.Area, &obj.Source, &obj.Value); err != nil {
			return nil, err
		}
		results = append(results, obj)
	}

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
