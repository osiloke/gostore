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
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gosimple/slug"
	"github.com/ungerik/go-dry"

	badgerdb "github.com/dgraph-io/badger/v4"
	gostore "github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/stores/badger"

	"github.com/spf13/cobra"
)

var (
	path, name, action, data, dataFile, key, store, output string
	count                                                  int
)

func getStore(name, path string) (gostore.ObjectStore, error) {
	switch name {
	case "BADGER":
		return badger.New(path)
	}
	return nil, errors.New("No store named " + name)
}

func writeCSV(data []map[string]interface{}, filename string) error {
	if len(data) == 0 {
		return errors.New("no data to write")
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	header := make([]string, 0, len(data[0]))
	for key := range data[0] {
		header = append(header, key)
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write CSV rows
	for _, row := range data {
		record := make([]string, len(header))
		for i, key := range header {
			if value, ok := row[key]; ok {
				record[i] = fmt.Sprintf("%v", value)
			} else {
				record[i] = ""
			}
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
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
				file, err := os.Create("keys.txt")
				if err != nil {
					fmt.Println("Error creating file, printing to stdout", err)
				} else {
					defer file.Close()
				}
				err = d.Db.View(func(txn *badgerdb.Txn) error {
					opts := badgerdb.DefaultIteratorOptions
					opts.PrefetchSize = 10
					opts.Reverse = true
					it := txn.NewIterator(opts)
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						k := item.Key()
						key := string(k)
						if file != nil {
							_, err = file.WriteString(key + "\n")
							if err != nil {
								fmt.Println("Error writing to file ", err)
							}
						} else {
							fmt.Println(key)
						}
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
				fmt.Fprintf(os.Stderr, "Error retrieving all records: %v\n", err)
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
			filename := fmt.Sprintf("%s-%v", store, slug.Make(path+string(time.Now().String())))
			if output == "csv" {
				fmt.Println("Writing output to CSV file")
				err = writeCSV(jrows, filename+".csv")
				if err != nil {
					fmt.Printf("Error writing CSV file: %v\n", err)
				} else {
					fmt.Println("CSV file written successfully")
				}
			} else {
				fmt.Println("Writing output to JSON file")
				err = os.WriteFile(filename+".json", []byte(stringRows), 0644)
				if err != nil {
					fmt.Printf("Error writing JSON file: %v\n", err)
				} else {
					fmt.Println("JSON file written successfully")
				}
			}
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
			fmt.Printf("%s = %v\n", key, _data)
		case "create":
			_data := make(map[string]interface{})
			err = json.Unmarshal([]byte(data), &_data)
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
			fmt.Printf("Are you sure you want to delete the key %s? (yes/no): ", key)
			var response string
			fmt.Scanln(&response)
			if strings.ToLower(response) == "yes" {
				err := db.Delete(key, store)
				if err != nil {
					fmt.Println(err.Error())
					break
				}
				fmt.Println(key + " deleted")
			} else {
				fmt.Println("Deletion aborted")
			}
		case "getPrefixes":
			if d, ok := db.(*badger.BadgerStore); ok {
				uniquePrefixes := make(map[string]struct{})
				err := d.Db.View(func(txn *badgerdb.Txn) error {
					opts := badgerdb.DefaultIteratorOptions
					opts.PrefetchValues = true
					it := txn.NewIterator(opts)
					defer it.Close()

					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						k := item.Key()
						key := string(k)
						parts := strings.Split(key, "|")
						prefix := parts[0]
						uniquePrefixes[prefix] = struct{}{}
					}

					return nil
				})
				if err != nil {
					fmt.Println(err)
					return
				}

				// Print unique prefixes
				for prefix := range uniquePrefixes {
					fmt.Println(prefix)
				}
			}
		case "findPrefix":
			if d, ok := db.(*badger.BadgerStore); ok {
				err := d.Db.View(func(txn *badgerdb.Txn) error {
					opts := badgerdb.DefaultIteratorOptions
					opts.PrefetchValues = true
					it := txn.NewIterator(opts)
					defer it.Close()

					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						k := item.Key()
						id := strings.Split(string(k), "$")
						if strings.HasPrefix(id[1], key) {
							fmt.Println(id, key)
						}
					}

					return nil
				})
				if err != nil {
					fmt.Println(err)
				}
			}
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
	dbCmd.Flags().StringVarP(&action, "action", "a", "get", "action to perform: get, getAll, create, update, delete, keys, getPrefixes, findPrefix")
	dbCmd.Flags().StringVarP(&key, "key", "k", "", "key to operate on")
	dbCmd.Flags().StringVarP(&data, "data", "d", "", "data to create")
	dbCmd.Flags().StringVarP(&dataFile, "dataFile", "i", "", "data to create")
	dbCmd.Flags().StringVarP(&store, "store", "s", "_test", "store")
	dbCmd.Flags().IntVarP(&count, "count", "c", 1000, "count of rows to return")
	dbCmd.Flags().StringVarP(&output, "output", "o", "json", "output format: json or csv")
}
