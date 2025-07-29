// Copyright © 2019 Osiloke Harold Emoekpere <me@osiloke.com>
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
	"fmt"
	"os"

	"github.com/blevesearch/bleve/v2"
	badger "github.com/osiloke/gostore/stores/badger"
	"github.com/spf13/cobra"
)

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a badger backup. Note use tar to extract compressed backup file",
	Long:  `.`,
	Run: func(cmd *cobra.Command, args []string) {
		filename, _ := cmd.Flags().GetString("filename")
		// TODO: if filename is gz, use tar to extract
		mode := int(0777)
		rootPath := fmt.Sprintf("./%s_restored/", filename)
		os.RemoveAll(rootPath)
		os.Mkdir(rootPath, os.FileMode(mode))
		db, err := badger.NewWithIndex(rootPath, "", bleve.NewIndexMapping(), nil)
		if err != nil {
			panic(err)
		}
		err = db.Restore(filename)
		if err != nil {
			panic(err)
		}
		db.Close()
	},
}

func init() {
	RootCmd.AddCommand(restoreCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// restoreCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	restoreCmd.Flags().StringP("db-type", "d", "badger", "Database type")
	restoreCmd.Flags().StringP("filename", "b", "./badger.bak", "Database type")
}
