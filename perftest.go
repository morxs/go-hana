package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	// Register hdb driver.
	_ "github.com/SAP/go-hdb/driver"
	// cli helper
	"github.com/mkideal/cli"
	// ini config
	"github.com/go-ini/ini"
	// internal
	"github.com/morxs/go-hana/utils"
)

type argT struct {
	cli.Helper
	ArgConfig string `cli:"c" usage:"Custom config file" dft:"config.ini"`
	ArgSQL    string `cli:"*s" usage:"SQL query to execute" dft="query.sql"`
}

const (
	driverName = "hdb"
)

const (
	ekkoSQL = `select * from sapabap1.t001`
)

func main() {

	cli.Run(new(argT), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*argT)

		// read config file
		utils.WriteMsg("READ CONFIG")
		iniCfg, err := ini.Load(argv.ArgConfig)
		if err != nil {
			utils.WriteMsg("CONFIG")
			log.Fatal(err)
		}
		iniSection := iniCfg.Section("server")
		iniKeyUsername := iniSection.Key("uid").String()
		iniKeyPassword := iniSection.Key("pwd").String()
		iniKeyHost := iniSection.Key("host").String()
		iniKeyPort := iniSection.Key("port").String()
		hdbDsn := "hdb://" + iniKeyUsername + ":" + iniKeyPassword + "@" + iniKeyHost + ":" + iniKeyPort

		utils.WriteMsg("OPEN SQL")
		fSQL, err := ioutil.ReadFile(argv.ArgSQL)
		if err != nil {
			log.Fatal(err)
		}

		sqlSQL := string(fSQL)

		//fmt.Println(sqlSQL)

		utils.WriteMsg("OPEN HDB")
		db, err := sql.Open(driverName, hdbDsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			log.Fatal(err)
		}

		// create file
		/*
			utils.WriteMsg("CREATE FILE: " + File)
			file, err := os.Create(File)
			if err != nil {
				fmt.Println(err)
			}
			defer file.Close()
		*/

		// log starttime
		startTime := time.Now()

		// try to query
		utils.WriteMsg("QUERY")
		rows, err := db.Query(sqlSQL)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
		}

		if err := rows.Err(); err != nil {
			utils.WriteMsg("ROWS")
			log.Fatal(err)
		}

		// log end time
		endTime := time.Since(startTime)

		utils.WriteMsg("DONE")

		fmt.Printf("Query took %v\n", endTime)

		return nil
	})
}
