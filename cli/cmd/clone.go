// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
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
	"context"

	"github.com/osiloke/gostore/cli/copy"
	"github.com/spf13/cobra"
)

var (
	leftStore, leftStorePath, rightStore, rightStorePath string
	stores                                               []string
	batchCount                                           int
)

// cloneCmd represents the clone command
var cloneCmd = &cobra.Command{
	Use:   "clone",
	Short: "Clone a store",
	Long:  `Clone a store. This will generate a new db with indexed data in the target folder`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := copy.Clone(context.Background(), batchCount, leftStore, rightStore, leftStorePath, rightStorePath, stores); err != nil {
			println("ERROR " + err.Error())
			return
		}
		println("copied all stores")
	},
}

func init() {
	RootCmd.AddCommand(cloneCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// cloneCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// cloneCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	cloneCmd.Flags().StringVarP(&leftStore, "source-store", "s", "", "source store")
	cloneCmd.Flags().StringVarP(&leftStorePath, "source-path", "p", "./srcdb/db", "source path")
	cloneCmd.Flags().StringVarP(&rightStore, "target-store", "t", "", "destination store")
	cloneCmd.Flags().StringVarP(&rightStorePath, "target-path", "a", "./destdb", "target path")
	cloneCmd.Flags().StringSliceVarP(&stores, "stores", "e", []string{"default"}, "stores to copy")
	cloneCmd.Flags().IntVarP(&batchCount, "batch", "b", 500, "how many rows to save at a time")
}
