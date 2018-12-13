// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
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
	"database/sql"
	"log"

	_ "github.com/lib/pq"

	"github.com/olivere/elastic"
	"github.com/spf13/cobra"
)

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// pgurl := "postgres://postgres:8cyt5fsbINl95Num@35.185.157.74:5432/meepshop?sslmode=disable"
		pgurl := "postgres://meepshop-api:4JMgT6MzuoX4@104.155.234.76:5432/meepshop?sslmode=disable" // production
		// esurl := "http://35.234.41.43:9200"
		esurl := "http://35.189.161.16:9200" // production
		esType := "order"
		esIndex := "2910eb12_d64a_49cc_b2be_54201441e27b"
		id := "873931f3-76a1-4055-b466-e68b3b495775"
		// activityInfoDiscountPrice := 628
		// totalPrice := 7234
		db, err := sql.Open("postgres", pgurl)
		if err != nil {
			log.Fatal(err)
		}

		// tx, err := db.Begin()
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// defer tx.Rollback()
		// _, err = tx.Exec(`update orders set data = jsonb_set(data, '{activityInfo,0,discountPrice}', $1) where id = $2;`, activityInfoDiscountPrice, id)
		// _, err = tx.Exec(`update orders set data = jsonb_set(data, '{priceInfo,total}', $1) where id = $2;`, totalPrice, id)
		// _, err = tx.Exec(`update orders set data = jsonb_set(data, '{priceInfo,actualTotal}', $1) where id = $2;`, totalPrice, id)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// tx.Commit()

		row := db.QueryRow(`SELECT data FROM orders WHERE id = $1`, id)
		var body []byte
		row.Scan(&body)
		client, err := elastic.NewClient(
			elastic.SetURL(esurl),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Fatal(err)
		}
		ctx := context.Background()
		resp, err := client.Index().Index(esIndex).Id(id).Type(esType).BodyString(string(body)).Do(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if resp.Result != "updated" {
			log.Fatal(resp.Result)
		}
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// copyCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// copyCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
