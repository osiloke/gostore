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
	dbPath, dbType, dbStore, dbOutput string
	dbData, dbDataFile                string
	dbCount                           int
)

func getStore(name, path string) (gostore.ObjectStore, error) {
	switch strings.ToUpper(name) {
	case "BADGER":
		return badger.NewDBOnly(path)
	default:
		return nil, fmt.Errorf("no store named %s", name)
	}
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
	Short: "Execute database commands",
	Long:  `Execute various database operations like get, list, create, update, and delete.`,
}

var dbGetCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Get a single record by key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		key := args[0]
		var data map[string]interface{}
		err = db.Get(key, dbStore, &data)
		if err != nil {
			return fmt.Errorf("error getting record: %v", err)
		}

		b, _ := json.MarshalIndent(data, "", "  ")
		fmt.Println(string(b))
		return nil
	},
}

var dbListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all records in a store",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		rows, err := db.All(dbCount, 0, dbStore)
		if err != nil {
			return fmt.Errorf("error retrieving records: %v", err)
		}
		defer rows.Close()

		var jrows []map[string]interface{}
		for {
			b, ok := rows.NextRaw()
			if !ok {
				break
			}
			var d map[string]interface{}
			if err := json.Unmarshal(b, &d); err != nil {
				fmt.Fprintf(os.Stderr, "Error unmarshalling row: %v\n", err)
				continue
			}
			jrows = append(jrows, d)
		}

		if len(jrows) == 0 {
			fmt.Println("No records found.")
			return nil
		}

		filename := fmt.Sprintf("%s-%v", dbStore, slug.Make(time.Now().Format("2006-01-02-150405")))
		if dbOutput == "csv" {
			err = writeCSV(jrows, filename+".csv")
			if err != nil {
				return fmt.Errorf("error writing CSV: %v", err)
			}
			fmt.Printf("CSV file written to %s.csv\n", filename)
		} else {
			out, _ := json.MarshalIndent(jrows, "", "  ")
			err = os.WriteFile(filename+".json", out, 0644)
			if err != nil {
				return fmt.Errorf("error writing JSON: %v", err)
			}
			fmt.Printf("JSON file written to %s.json\n", filename)
		}
		return nil
	},
}

var dbCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new record",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		var rawData string
		if dbDataFile != "" {
			rawData, err = dry.FileGetString(dbDataFile, time.Second*5)
			if err != nil {
				return fmt.Errorf("error reading data file: %v", err)
			}
		} else {
			rawData = dbData
		}

		if rawData == "" {
			return errors.New("data or dataFile must be provided")
		}

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(rawData), &data); err != nil {
			return fmt.Errorf("invalid JSON data: %v", err)
		}

		id := gostore.NewObjectId().String()
		newID, err := db.Save(id, dbStore, &data)
		if err != nil {
			return fmt.Errorf("error saving record: %v", err)
		}

		fmt.Printf("Record created with ID: %s\n", newID)
		return nil
	},
}

var dbUpdateCmd = &cobra.Command{
	Use:   "update [key]",
	Short: "Update an existing record",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		key := args[0]
		var rawData string
		if dbDataFile != "" {
			var err error
			rawData, err = dry.FileGetString(dbDataFile, time.Second*5)
			if err != nil {
				return fmt.Errorf("error reading data file: %v", err)
			}
		} else {
			rawData = dbData
		}

		if rawData == "" {
			return errors.New("data or dataFile must be provided")
		}

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(rawData), &data); err != nil {
			return fmt.Errorf("invalid JSON data: %v", err)
		}

		newID, err := db.Save(key, dbStore, &data)
		if err != nil {
			return fmt.Errorf("error updating record: %v", err)
		}

		fmt.Printf("Record %s updated\n", newID)
		return nil
	},
}

var dbDeleteCmd = &cobra.Command{
	Use:   "delete [key]",
	Short: "Delete a record",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		key := args[0]
		fmt.Printf("Are you sure you want to delete the key %s from store %s? (y/n): ", key, dbStore)
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
			fmt.Println("Deletion aborted")
			return nil
		}

		err = db.Delete(key, dbStore)
		if err != nil {
			return fmt.Errorf("error deleting record: %v", err)
		}

		fmt.Printf("Record %s deleted\n", key)
		return nil
	},
}

var dbCountCmd = &cobra.Command{
	Use:   "count [store]",
	Short: "Count keys in a store or the whole database",
	Long:  `Count keys in a store or the whole database. If a store name is provided, only keys in that store are counted.`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		store, ok := db.(*badger.BadgerStore)
		if !ok {
			return errors.New("count command only supported for Badger store")
		}

		targetStore := ""
		if len(args) > 0 {
			targetStore = args[0]
		} else if cmd.Flags().Changed("store") {
			targetStore = dbStore
		}

		if targetStore != "" {
			fmt.Printf("Counting keys in store %s...\n", targetStore)
		} else {
			fmt.Println("Counting all keys in database...")
		}

		count, err := store.Count(targetStore)
		if err != nil {
			return fmt.Errorf("error counting keys: %v", err)
		}

		fmt.Printf("Total keys: %d\n", count)
		return nil
	},
}

var dbKeysCmd = &cobra.Command{
	Use:   "keys [store]",
	Short: "List keys in the database",
	Long:  `List keys in the database. If a store name is provided, only keys in that store are listed.`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		store, ok := db.(*badger.BadgerStore)
		if !ok {
			return errors.New("keys command only supported for Badger store")
		}

		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		} else if cmd.Flags().Changed("store") {
			prefix = dbStore
		}

		if prefix != "" {
			fmt.Printf("Reading keys in store %s...\n", prefix)
			return store.Db.View(func(txn *badgerdb.Txn) error {
				opts := badgerdb.DefaultIteratorOptions
				opts.PrefetchSize = 100
				it := txn.NewIterator(opts)
				defer it.Close()
				p := []byte(store.KeyFormat.TablePrefix + prefix + store.KeyFormat.IdSeparator)
				for it.Seek(p); it.ValidForPrefix(p); it.Next() {
					key := string(it.Item().Key())
					fmt.Println(key)
				}
				return nil
			})
		}

		fmt.Println("Reading all keys...")
		return store.Db.View(func(txn *badgerdb.Txn) error {
			opts := badgerdb.DefaultIteratorOptions
			opts.PrefetchSize = 100
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				key := string(it.Item().Key())
				fmt.Println(key)
			}
			return nil
		})
	},
}

var dbFindCmd = &cobra.Command{
	Use:   "find [prefix]",
	Short: "Find keys with a specific prefix",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		store, ok := db.(*badger.BadgerStore)
		if !ok {
			return errors.New("find command only supported for Badger store")
		}

		prefix := args[0]
		return store.Db.View(func(txn *badgerdb.Txn) error {
			opts := badgerdb.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				k := string(it.Item().Key())
				if strings.Contains(k, "$") {
					parts := strings.Split(k, "$")
					if len(parts) > 1 && strings.HasPrefix(parts[1], prefix) {
						fmt.Printf("Key: %s, ID Part: %s\n", k, parts[1])
					}
				} else if strings.HasPrefix(k, prefix) {
					fmt.Println(k)
				}
			}
			return nil
		})
	},
}

var dbPrefixesCmd = &cobra.Command{
	Use:   "prefixes",
	Short: "List all unique prefixes",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getStore(dbType, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		store, ok := db.(*badger.BadgerStore)
		if !ok {
			return errors.New("prefixes command only supported for Badger store")
		}

		prefixes := make(map[string]struct{})
		err = store.Db.View(func(txn *badgerdb.Txn) error {
			it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				key := string(it.Item().Key())
				parts := strings.Split(key, "|")
				prefixes[parts[0]] = struct{}{}
			}
			return nil
		})

		if err != nil {
			return err
		}

		for p := range prefixes {
			fmt.Println(p)
		}
		return nil
	},
}

func init() {
	RootCmd.AddCommand(dbCmd)

	// Local flags for db and all subcommands
	dbCmd.PersistentFlags().StringVarP(&dbPath, "path", "p", "./db", "path to gostore data folder")
	dbCmd.PersistentFlags().StringVarP(&dbType, "type", "t", "BADGER", "type of gostore")
	dbCmd.PersistentFlags().StringVarP(&dbStore, "store", "s", "_test", "store name")

	// Subcommands
	dbCmd.AddCommand(dbGetCmd)
	dbCmd.AddCommand(dbListCmd)
	dbCmd.AddCommand(dbCreateCmd)
	dbCmd.AddCommand(dbUpdateCmd)
	dbCmd.AddCommand(dbDeleteCmd)
	dbCmd.AddCommand(dbKeysCmd)
	dbCmd.AddCommand(dbCountCmd)
	dbCmd.AddCommand(dbPrefixesCmd)
	dbCmd.AddCommand(dbFindCmd)

	// Subcommand specific flags
	dbListCmd.Flags().IntVarP(&dbCount, "count", "c", 1000, "maximum number of rows to return")
	dbListCmd.Flags().StringVarP(&dbOutput, "output", "o", "json", "output format: json or csv")

	dbCreateCmd.Flags().StringVarP(&dbData, "data", "d", "", "JSON data to create")
	dbCreateCmd.Flags().StringVarP(&dbDataFile, "file", "f", "", "path to JSON file with data")

	dbUpdateCmd.Flags().StringVarP(&dbData, "data", "d", "", "JSON data to update")
	dbUpdateCmd.Flags().StringVarP(&dbDataFile, "file", "f", "", "path to JSON file with data")
}
