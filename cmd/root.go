// Copyright © 2018 nukr <nukrs.w@gmail.com>
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
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/olivere/elastic"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// Doc ...
type Doc struct {
	Index       string
	Type        string
	VersionType string
	Version     int64
	ID          string
	Parent      *string
	Data        string
}

type nextScanner interface {
	Next() bool
	Scan(...interface{}) error
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "pg2es",
	Short: "A brief description of your application",
	Long: `
pg2es \
--pgtable "example" \
--estable "example" \
--esurl "http://localhost:9200"  \
--pgdsn "" \
--esindex "" \
--workernum 4 \
--jobsize 200`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()
		pgtable := viper.GetString("pg_table")
		estable := viper.GetString("es_table")
		esurl := viper.GetString("es_url")
		esindex := viper.GetString("es_index")
		pgdsn := viper.GetString("pg_dsn")
		workernum := viper.GetInt("worker_num")
		jobsize := viper.GetInt("job_size")
		fmt.Printf(`
		pgtable: %s,
		estable: %s,
		esurl: %s,
		esindex: %s,
		pgdsn: %s,
		workernum: %d,
		jobsize: %d
		`, pgtable, estable, esurl, esindex, pgdsn, workernum, jobsize)
		db, err := sql.Open("postgres", pgdsn)
		if err != nil {
			fmt.Printf(`sql.Open("postgres", pgdsn) error %v`, err)
			os.Exit(1)
		}
		client, err := elastic.NewClient(
			elastic.SetURL(esurl),
			elastic.SetSniff(false),
		)
		if err != nil {
			fmt.Printf(`elastic.NewClient(
				elastic.SetURL(esurl),
				elastic.SetSniff(false),
			) error: %v`, err)
			os.Exit(1)
		}
		query := createQuery(pgtable)
		rows, err := db.Query(query)
		if err != nil {
			fmt.Printf("db.Query(query) error: %v", err)
			os.Exit(1)
		}
		jobs := make(chan []*Doc, 100)
		done := make(chan struct{}, workernum)
		for i := 0; i < workernum; i++ {
			bulk := client.Bulk()
			go worker(i, jobs, bulk, done)
		}
		dispatch(rows, esindex, estable, jobs, jobsize)
		close(jobs)
		// wait for all workers done
		for i := 0; i < workernum; i++ {
			<-done
		}
		fmt.Printf("table %s Done!\n", pgtable)
		fmt.Printf("total cost %s\n", time.Since(start))
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	pgtable := "example"
	estable := "example"
	esindex := "2910eb12_d64a_49cc_b2be_54201441e27b"
	pgdsn := "postgres://postgres:postgres@localhost:5432/meepshop?sslmode=disable"
	esurl := "http://localhost:9200"
	workernum := 4
	jobsize := 50
	viper.SetDefault("pg_table", pgtable)
	viper.SetDefault("es_table", estable)
	viper.SetDefault("es_index", esindex)
	viper.SetDefault("pg_dsn", pgdsn)
	viper.SetDefault("es_url", esurl)
	viper.SetDefault("worker_num", workernum)
	viper.SetDefault("jobsize", jobsize)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cobra_test.yaml)")
	rootCmd.PersistentFlags().String("pgtable", pgtable, "postgres table")
	rootCmd.PersistentFlags().String("esindex", esindex, "elasticsearch index name")
	rootCmd.PersistentFlags().String("estable", estable, "elasticsearch type name")
	rootCmd.PersistentFlags().String("esurl", esurl, "elasticsearch url")
	rootCmd.PersistentFlags().String("pgdsn", pgdsn, "postgres connection string")
	rootCmd.PersistentFlags().Int("workernum", workernum, "worker number")
	rootCmd.PersistentFlags().Int("jobsize", jobsize, "job size")

	viper.BindPFlag("pg_table", rootCmd.PersistentFlags().Lookup("pgtable"))
	viper.BindPFlag("pg_dsn", rootCmd.PersistentFlags().Lookup("pgdsn"))
	viper.BindPFlag("es_index", rootCmd.PersistentFlags().Lookup("esindex"))
	viper.BindPFlag("es_table", rootCmd.PersistentFlags().Lookup("estable"))
	viper.BindPFlag("es_url", rootCmd.PersistentFlags().Lookup("esurl"))
	viper.BindPFlag("worker_num", rootCmd.PersistentFlags().Lookup("workernum"))
	viper.BindPFlag("job_size", rootCmd.PersistentFlags().Lookup("jobsize"))

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".cobra_test" (without extension).
		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func createQuery(pgtable string) string {
	return fmt.Sprintf(
		`
SELECT
data ->> 'id',
data ->> '__parent',
data ->> 'updatedAt',
data
FROM %s
ORDER BY data ->> 'createdAt'`,
		pgtable,
	)
}

func parseTime(t *string) time.Time {
	timeUpdatedAt := time.Now()
	if t != nil {
		var err error
		timeUpdatedAt, err = time.Parse(time.RFC3339, *t)
		// TODO: Handle error here
		_ = err
	}
	return timeUpdatedAt
}

func worker(id int, jobs chan []*Doc, bulk *elastic.BulkService, done chan struct{}) {
	ctx := context.Background()
	for job := range jobs {
		for _, doc := range job {
			req := elastic.NewBulkIndexRequest().
				Index(doc.Index).
				Type(doc.Type).
				Id(doc.ID).
				VersionType(doc.VersionType).
				Version(doc.Version).
				Doc(doc.Data)
			if doc.Parent != nil {
				req = req.Parent(*doc.Parent).Routing(*doc.Parent)
			}
			bulk.Add(req)
		}
		bulk.Do(ctx)
	}
	done <- struct{}{}
}

func dispatch(rows nextScanner, esindex, estable string, jobs chan []*Doc, jobsize int) {
	fmt.Printf("dispatching to [%s] with job size %d\n", estable, jobsize)
	start := time.Now()
	var docs []*Doc
	var counter int
	for rows.Next() {
		var id *string
		var data *string
		var strUpdatedAt *string
		var parent *string
		err := rows.Scan(&id, &parent, &strUpdatedAt, &data)
		if err != nil {
			fmt.Println(err)
			continue
		}
		timeUpdatedAt := parseTime(strUpdatedAt)
		doc := &Doc{
			Index:       esindex,
			Type:        estable,
			Data:        *data,
			ID:          *id,
			Parent:      parent,
			Version:     timeUpdatedAt.UnixNano(),
			VersionType: "external",
		}
		docs = append(docs, doc)
		counter++
		if counter == jobsize {
			jobs <- docs
			docs = nil
			counter = 0
		}
	}
	if len(docs) != 0 {
		jobs <- docs
	}
	fmt.Printf("dispatch done cost %s\n", time.Since(start).String())
}
