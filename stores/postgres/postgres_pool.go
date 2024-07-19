package postgres

import (
	"fmt"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gocache "zgo.at/zcache/v2"
)

var cacheHandler *gocache.Cache[string, *gorm.DB]

type StoreDB struct {
	CreationMutex sync.Mutex
	cache         *gocache.Cache[string, *gorm.DB]
}

type Context struct {
	ID       string
	Host     string
	User     string
	Password string
	Database string
}

func (s *StoreDB) CreateDBHandler(ctx *Context) (*gorm.DB, error) {
	// Maybe we could use a better mechanism to do this.
	s.CreationMutex.Lock()
	defer s.CreationMutex.Unlock()

	// Double check before moving to Creation precedures
	if db, found := s.cache.Touch(ctx.ID); found {
		return db, nil
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=enable", ctx.Host, ctx.User, ctx.Password, ctx.Database)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

	if err != nil {
		return nil, err
	}

	cacheHandler.Set(ctx.ID, db)

	return db, nil
}

func (s *StoreDB) GetDBHandler(ctx *Context) (*gorm.DB, error) {
	db, found := s.cache.Touch(ctx.ID)
	if found {
		return db, nil
	}

	return s.CreateDBHandler(ctx)
}

func initManager() *StoreDB {
	cacheHandler = gocache.New[string, *gorm.DB](10*time.Minute, 10*time.Minute)
	cacheHandler.OnEvicted(func(s string, i *gorm.DB) {
		// https://github.com/go-gorm/gorm/issues/3145
		sql, err := i.DB()
		if err != nil {
			panic(err)
		}
		sql.Close()
	})

	return &StoreDB{
		cache: cacheHandler,
	}
}
