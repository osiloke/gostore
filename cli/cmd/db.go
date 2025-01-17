// Copyright © 2017 Osiloke Emoekpere <me@osiloke.com>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gosimple/slug"
	"github.com/ungerik/go-dry"

	badgerdb "github.com/dgraph-io/badger/v4"
	gostore "github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/stores/badger"

	"github.com/spf13/cobra"
)

var (
	path, name, action, data, dataFile, key, store string
	count                                          int
	csv                                            bool
)

func getStore(name, path string) (gostore.ObjectStore, error) {
	switch name {
	case "BADGER":
		return badger.New(path)
	}
	return nil, errors.New("No store named " + name)
}

// dbCmd represents the db command
var dbCmd = &cobra.Command{
	Use:   "db",
	Short: "Execute db commands",
	Long:  `Execute db commands.`,
	Run: func(cmd *cobra.Command, args []string) {

		db, err := getStore(name, path)
		if err != nil {
			panic(err.Error())
		}
		defer db.Close()

		switch action {
		case "keys":
			if d, ok := db.(*badger.BadgerStore); ok {
				err = d.Db.View(func(txn *badgerdb.Txn) error {
					opts := badgerdb.DefaultIteratorOptions
					opts.PrefetchSize = 10
					opts.Reverse = true
					it := txn.NewIterator(opts)
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						k := item.Key()
						// obj := make([][]byte, 2)
						// err := item.Value(func(v []byte) error {
						// 	obj[1] = append([]byte{}, v...)
						// 	return nil
						// })
						// if err != nil {
						// 	return err
						// }
						// objs = append(objs, obj)
						// obj[0] = make([]byte, len(k))
						// copy(obj[0], k)
						key := string(k)
						fmt.Println(key)
					}
					return nil
				})
				if err != nil {
					fmt.Println(err)
				}
			}
		case "getAll":
			rows, err := db.All(count, 0, store)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			jrows := make([]map[string]interface{}, 0)
		OUTER:
			for {
				var d map[string]interface{}
				b, ok := rows.NextRaw()
				if !ok {
					break OUTER
				}
				json.Unmarshal(b, &d)
				if err == nil {
					jrows = append(jrows, d)
				} else {
					fmt.Println(err)
				}
			}
			rows.Close()
			stringRows, err := json.Marshal(&jrows)
			if err != nil {
				panic(err)
			}
			os.WriteFile(fmt.Sprintf("%s-%v.json", store, slug.Make(path+string(time.Now().String()))), []byte(stringRows), 0644)
		case "get":
			_data := make(map[string]interface{})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			err = db.Get(key, store, &_data)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Println(fmt.Sprintf("%s = %v", key, _data))
		case "create":
			_data := make(map[string]interface{})
			err = json.Unmarshal([]byte(data), _data)
			if err != nil {
				fmt.Println(err.Error())
				break
			}
			_k := gostore.NewObjectId().String()
			_k, err = db.Save(_k, store, &data)
			if err != nil {
				fmt.Println(err.Error())
				break
			}
			fmt.Println(_k + " created")
		case "update":
			if dataFile != "" {
				data, _ = dry.FileGetString(dataFile, time.Second*5)
			}
			_data := make(map[string]interface{})
			err = json.Unmarshal([]byte(data), &_data)
			if err != nil {
				fmt.Println(err.Error())
				break
			}
			_k, err := db.Save(key, store, &_data)
			if err != nil {
				fmt.Println(err.Error())
				break
			}
			fmt.Println(_k + " updated")
		case "delete":
			err := db.Delete(key, store)
			if err != nil {
				fmt.Println(err.Error())
				break
			}
			fmt.Println(key + " deleted")
		}

	},
}

func init() {
	RootCmd.AddCommand(dbCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dbCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dbCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	dbCmd.Flags().StringVarP(&path, "path", "p", "./db", "path to gostore data folder")
	dbCmd.Flags().StringVarP(&name, "type", "t", "BADGER", "type of gostore")
	dbCmd.Flags().StringVarP(&action, "action", "a", "get", "action to perform: get, getAll, create, update, delete, keys")
	dbCmd.Flags().StringVarP(&key, "key", "k", "", "key to operate on")
	dbCmd.Flags().StringVarP(&data, "data", "d", "", "data to create")
	dbCmd.Flags().StringVarP(&dataFile, "dataFile", "i", "", "data to create")
	dbCmd.Flags().StringVarP(&store, "store", "s", "_test", "store")
	dbCmd.Flags().BoolVarP(&csv, "csv", "f", false, "output to csv")
	dbCmd.Flags().IntVarP(&count, "count", "c", 1000, "count of rows to return")
}
