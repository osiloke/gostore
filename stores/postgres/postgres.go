package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	common "github.com/osiloke/gostore/common"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
	postgres_driver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gorm_logger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

var logger = common.Logger("postgres")

// TODO: use generics and allow caller to specify db connector type
type PostgresObjectStore struct {
	db  *gorm.DB
	dsn string
}

func NewPostgresObjectStoreFromGorm(db *gorm.DB) *PostgresObjectStore {
	s := PostgresObjectStore{db: db}
	s.CreateDatabase()
	return &s
}
func NewPostgresObjectStore(dsn string) *PostgresObjectStore {
	newLogger := gorm_logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		gorm_logger.Config{
			// SlowThreshold:             time.Second,        // Slow SQL threshold
			LogLevel:                  gorm_logger.Silent, // Log level
			IgnoreRecordNotFoundError: true,               // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries:      true,               // Don't include params in the SQL log
			Colorful:                  false,              // Disable color
		},
	)
	db, err := gorm.Open(postgres_driver.Open(dsn), &gorm.Config{
		// PrepareStmt: true,

		// SkipDefaultTransaction: true,
		Logger: newLogger,
	})
	if err != nil {
		panic(err)
	}
	if err := db.Use(tracing.NewPlugin()); err != nil {
		panic(err)
	}
	s := PostgresObjectStore{db, dsn}
	s.CreateDatabase()
	return &s
}

type PostgresRows struct {
	cursor *sql.Rows
	total  int64
}

func (s *PostgresRows) LastError() error {
	return nil
}

func (s *PostgresRows) makeColumnPointers() ([]string, []interface{}) {
	cols, _ := s.cursor.Columns()
	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	return cols, columnPointers
}
func (s *PostgresRows) scanMap(m map[string]interface{}) error {
	cols, columnPointers := s.makeColumnPointers()
	// Scan the result into the column pointers...
	if err := s.cursor.Scan(columnPointers...); err != nil {
		return err
	}
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}
	return nil
}
func (s *PostgresRows) Next(dst interface{}) (ok bool, err error) {
	if ok = s.cursor.Next(); ok {
		switch v := dst.(type) {
		case map[string]interface{}:
			if err = s.scanMap(v); err == nil {
				ok = true
			}
		case *map[string]interface{}:
			*v = make(map[string]interface{})
			if err = s.scanMap(*v); err == nil {
				ok = true
			}
		default:
			if err = s.cursor.Scan(dst); err == nil {
				ok = true
			}
		}
	}
	return
}
func (s *PostgresRows) NextRaw() ([]byte, bool) {
	return nil, false
}
func (s *PostgresRows) Total() int64 {
	return s.total
}

func (s *PostgresRows) Close() {
	s.cursor.Close()
}

func (s PostgresObjectStore) CreateDatabase() (err error) {
	return nil
}

func safeStoreName(name string) string {
	// switch name {
	// case "user":
	// 	return "_user"
	// case "group":
	// 	return "_group"
	// }
	return name
}
func (s PostgresObjectStore) CreateTable(store string, config interface{}) (err error) {
	if c, ok := config.(map[string]interface{}); ok {
		if schema, ok := c["schema"].(string); ok {
			// db, _ := s.db.DB()
			// dbsch, err := goerd.SchemaFromPostgresDB(db)
			// if err != nil {
			// 	return fmt.Errorf("cannot migrate database: %w", err)
			// }
			// dbsch.SaveYaml(os.Stdout)
			// existing, err := generateCreateTableStatement(s.db, store)
			// if err != nil {
			// 	return err
			// }
			ctx := context.Background()
			tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
				name, err := UpdatePostgresDSN(s.dsn, dbName)
				if err != nil {
					return nil, err
				}
				return sql.Open("pgx", name)
			})
			if err != nil {
				return err
			}
			pool, err := sql.Open("pgx", s.dsn)
			if err != nil {
				return err
			}
			plan, err := diff.Generate(ctx, diff.DBSchemaSource(pool),
				diff.DDLSchemaSource([]string{schema}),
				diff.WithTempDbFactory(tempDbFactory),
			)
			if err != nil {
				return err
			}
			logger.Debug("plan", "ddl", plan)
			// existing = strings.ReplaceAll(existing, "\n", "")
			// existing = strings.ReplaceAll(existing, "\\", "")
			// oldMigration := sqlize.NewSqlize(sqlize.WithMigrationFolder(""))
			// newMigration := sqlize.NewSqlize(sqlize.WithMigrationFolder(""))
			// if err = oldMigration.FromString(existing); err != nil {
			// 	logger.Error("cannot parse old migration", "err", err)
			// 	return err
			// }
			// if err = newMigration.FromString(schema); err != nil {
			// 	return err
			// }

			// newMigration.Diff(*oldMigration)
			// println(newMigration.StringUp())

			// println(newMigration.StringDown())
			return s.db.Exec(schema).Error
		}
	}
	return nil
}

// Query retrieves documents matching a filter and calculates aggregations.
func (s PostgresObjectStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, common.AggregateResult, error) {
	rows, err := s.FilterGetAll(filter, count, skip, store, opts)
	return rows, common.AggregateResult{}, err
}

func (s PostgresObjectStore) All(count int, skip int, store string) (prows common.ObjectRows, err error) {
	var rows *sql.Rows
	var total int64
	rows, err = s.db.Table(safeStoreName(store)).Select("*").Count(&total).Limit(count).Offset(skip).Rows()
	if err != nil {
		return
	}
	prows = &PostgresRows{cursor: rows, total: total}
	return
}

func (s PostgresObjectStore) AllCursor(store string) (common.ObjectRows, error) {
	//	result, err := r.DB(s.Database).Table(store).Run(s.Session)
	//	if err != nil{
	//		return nil, err
	//	}
	//	defer result.Close()
	//	return RethinkRows{result}, nil
	return nil, common.ErrNotImplemented
}

func (s PostgresObjectStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s PostgresObjectStore) Get(id, store string, dst interface{}) (err error) {
	result := s.db.Table(safeStoreName(store)).Select("*").Where("id = ?", id)
	if result.Error != nil {
		return result.Error
	}
	var row []byte
	err = result.Row().Scan(&row)
	if err == sql.ErrNoRows {
		return common.ErrNotFound
	}
	json.Unmarshal(row, dst)
	return nil
}

// This will retrieve all old rows that were created before the row with id was created
// [1, 2, 3, 4], before 2 will return [3, 4]
func (s PostgresObjectStore) Before(id string, count int, skip int, store string) (prows common.ObjectRows, err error) {
	var rows *sql.Rows
	var total int64
	rows, err = s.db.Table(safeStoreName(store)).Select("*").Where("id < ?", id).Count(&total).Limit(count).Offset(skip).Rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, common.ErrNotFound
		}
		return
	}
	prows = &PostgresRows{cursor: rows, total: total}
	return
}

// This will retrieve all new rows that were created since the row with id was created
// [1, 2, 3, 4], since 2 will return [1]
func (s PostgresObjectStore) Since(id string, count, skip int, store string) (prows common.ObjectRows, err error) {
	var rows *sql.Rows
	var total int64
	rows, err = s.db.Table(safeStoreName(store)).Select("*").Where("id > ?", id).Count(&total).Limit(count).Offset(skip).Rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, common.ErrNotFound
		}
		return
	}
	prows = &PostgresRows{cursor: rows, total: total}
	return
}

func (s PostgresObjectStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (rows common.ObjectRows, err error) {
	return nil, common.ErrNotImplemented
}

func (s PostgresObjectStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (int64, error) {
	return 0, common.ErrNotImplemented
}

func (s PostgresObjectStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (rows common.ObjectRows, err error) {
	return nil, common.ErrNotImplemented
}

func (s PostgresObjectStore) Save(key, store string, src interface{}) (string, error) {
	var err error
	result := s.db.Table(safeStoreName(store)).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.Assignments(src.(map[string]interface{})),
	}).Create(src)
	if result.Error != nil {
		err = result.Error
		if err == sql.ErrNoRows {
			return "", common.ErrNotFound
		}
	}
	return key, err
}

func (s PostgresObjectStore) SaveAll(store string, srcArray ...interface{}) (keys []string, err error) {
	var data []byte
	// TODO: use batch insert transaction
	for _, src := range srcArray {
		if data, err = json.Marshal(src); err == nil {
			result := s.db.Table(safeStoreName(store)).Create(&data)
			if result.Error != nil {
				err = result.Error
				if err == sql.ErrNoRows {
					return nil, err
				}
			}
		}
		if err != nil {
			logger.Debug("Error saving doc", "Err", err)
			return
		}
	}
	return
}

func (s PostgresObjectStore) Update(id string, store string, src interface{}) (err error) {
	var data []byte
	//TODO:perform update by retrieving existing data and merging data
	if data, err = json.Marshal(src); err == nil {
		err = s.db.Table(safeStoreName(store)).Where("id = ?", id).Updates(data).Error
		if err == sql.ErrNoRows {
			return common.ErrNotFound
		}
	}

	return

}

func (s PostgresObjectStore) Replace(id string, store string, src interface{}) (err error) {
	var data []byte
	if data, err = json.Marshal(src); err == nil {
		err = s.db.Table(safeStoreName(store)).Where("id = ?", id).Updates(data).Error
		if err == sql.ErrNoRows {
			return common.ErrNotFound
		}
	}

	return

}

func (s PostgresObjectStore) Delete(id string, store string) (err error) {
	err = s.db.Table(safeStoreName(store)).Where("id = ?", id).Delete(&map[string]interface{}{}).Error

	return
}

func (s PostgresObjectStore) GetStore() interface{} {
	return s.db
}

func (s PostgresObjectStore) Stats(store string) (map[string]interface{}, error) {
	var cnt int64
	result := s.db.Table(safeStoreName(store)).Count(&cnt)
	if result.Error != nil {
		if result.Error == sql.ErrNoRows {
			return nil, common.ErrNotFound
		}
		return nil, result.Error
	}
	return map[string]interface{}{"count": cnt}, nil
}

type QueryField struct {
	Name string      `json:"name"`
	Val  interface{} `json:"val"`
}

func (f QueryField) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		f.Name: f.Val,
	})
}

func (s PostgresObjectStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}
func (s PostgresObjectStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}

func (s PostgresObjectStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions) (err error) {
	var ok bool
	q := filter["q"].(map[string]interface{})
	rows, err := s.db.Table(safeStoreName(store)).Select("*").Where(q).Limit(1).Rows()
	if err == nil {
		prows := &PostgresRows{cursor: rows}
		if ok, err = prows.Next(dst); !ok {
			err = common.ErrNotFound
		}
	}
	return err
}

func (s PostgresObjectStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (prows common.ObjectRows, err error) {
	var total int64
	logger.Debug("FilterGetAll", "store", store, "filter", filter, "count", count, "skip", skip, "opts", opts)
	rows, err := s.db.Table(safeStoreName(store)).
		// Clauses(hints.UseIndex(fmt.Sprintf("%s_pkey", store))).
		Select("*").
		Where(filter).
		Count(&total).
		Limit(count).
		Offset(skip).
		Rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, common.ErrNotFound
		}
		return
	}
	prows = &PostgresRows{cursor: rows, total: total}
	return
}
func (s PostgresObjectStore) BatchDelete(ids []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}
func (s PostgresObjectStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}
func (s PostgresObjectStore) FilterDelete(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}

func (s PostgresObjectStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}
func (s PostgresObjectStore) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	var cnt int64
	q := filter["q"].(map[string]interface{})
	result := s.db.Table(safeStoreName(store)).Where(q).Count(&cnt)
	if result.Error != nil {
		if result.Error == sql.ErrNoRows {
			return 0, common.ErrNotFound
		}
		return 0, result.Error
	}
	return cnt, nil
}

func (s PostgresObjectStore) GetByField(name, val, store string, dst interface{}) (err error) {
	result := s.db.Table(safeStoreName(store)).Select("*").Where("?", fmt.Sprintf(`{"`+name+`": "%s"}`, val)).Limit(1)
	if result.Error != nil {
		return result.Error
	}

	var row []byte
	err = result.Row().Scan(&row)
	if err == sql.ErrNoRows {
		return common.ErrNotFound
	}
	logger.Debug("Err if any", "err", err)
	json.Unmarshal(row, dst)
	return nil
}

func (s PostgresObjectStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	result := s.db.Table(safeStoreName(store)).Select(fields).Where("?", fmt.Sprintf(`{"`+name+`": "%s"}`, val)).Limit(1)
	if result.Error != nil {
		return result.Error
	}
	var row []byte
	err = result.Row().Scan(&row)
	if err == sql.ErrNoRows {
		return common.ErrNotFound
	}
	json.Unmarshal(row, dst)
	return nil
}
func (s PostgresObjectStore) BatchInsert(data []interface{}, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	return nil, common.ErrNotImplemented
}
func (s PostgresObjectStore) Close() {
	if db, err := s.db.DB(); err != nil {
		panic(err)
	} else {
		db.Close()
	}
}
