package copy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	common "github.com/osiloke/gostore/common"
	gostore "github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/indexer"
	_ "github.com/osiloke/gostore/indexer/badger"
	"github.com/osiloke/gostore/stores/badger"
)

type KVStore interface {
	Close()
	All(count int, skip int, store string) (gostore.ObjectRows, error)
	AllCursor(store string) (gostore.ObjectRows, error)
	BatchInsertKV(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error)
	BatchInsertKVAndIndex(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error)
}

var logger = common.Logger("copy")

// CopyAll copies n rows from one db to another
func CopyStore(ctx context.Context, src, dst KVStore, batch int, store string) (int, error) {
	// logger.Info("copy %s", store)
	total := 0
	if grows, err := src.AllCursor(store); err == nil {
		rows := grows.(*common.CursorRows)
		defer rows.Close()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("cancelled copy store")
				return total, fmt.Errorf("canceled")
			default:
				dstRows := [][][]byte{}
				var err2 error
				for i := 0; i < batch; i++ {

					var kv [][]byte
					kv, err2 = rows.NextKV()
					if err2 != nil {
						fmt.Println(err2)
						break
					}
					if kv != nil {
						dstRows = append(dstRows, kv)
					}
				}
				total += len(dstRows)
				if len(dstRows) == 0 {
					return total, nil
				}
				logger.Debug(fmt.Sprintf("batch insert %d rows into %s", len(dstRows), store))
				_, err := dst.BatchInsertKVAndIndex(dstRows, store, nil)
				if err != nil {
					logger.Error("error at destination store", "err", err)
					return total, err
				}
				if err2 != nil {
					if err2 != gostore.ErrEOF {
						return total, err2
					}
					return total, nil
				}
			}
		}
	} else {
		logger.Error("unable to get rows - %v", err)
		return 0, err
	}
}

// Clone a store
func Clone(ctx context.Context, batchCount int, leftStore, rightStore, leftStorePath, rightStorePath string, stores []string) error {
	var src, dst KVStore
	var err error
	switch leftStore {
	case "badger":
		src, err = badger.NewDBOnly(leftStorePath)
		if err != nil {
			return err
		}
	default:
		return errors.New("unknown source store")
	}
	defer src.Close()

	switch rightStore {
	// case "firestore":
	// dst = firestoredb.NewFirestoreStoreWithServiceAccount(ctx, rightStorePath)
	case "badger":

		if _, err := os.Stat(rightStorePath); os.IsNotExist(err) {
			os.Mkdir(rightStorePath, os.FileMode(0777))
		}
		indexPath := filepath.Join(rightStorePath, "db.index")
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			os.Mkdir(indexPath, os.FileMode(0777))
		}
		dst, err = badger.NewWithIndexer(rightStorePath, indexer.NewBadgerIndexer(indexPath))
		if err != nil {
			return err
		}
	default:
		return errors.New("unknown destination store")
	}
	defer dst.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit
		logger.Debug("Received interrupt signal")
		cancel()
	}()

	for i := 0; i < len(stores); i++ {
		store := stores[i]
		total, err := CopyStore(ctx, src, dst, batchCount, store)
		if err != nil {
			if err == gostore.ErrEOF || strings.Contains(err.Error(), "timeout") {
				continue
			}
			return err

		}
		// if err := dst.(*badger.BadgerStore).Db.PurgeOlderVersions(); err != nil {
		// 	logger.Warn("unable to purge %s", err.Error())
		// }
		logger.Debug("copied %d rows from %s::%s to %s::%s", total, leftStore, store, rightStore, store)
	}
	// os.
	return nil
}
