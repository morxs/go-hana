package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	// Register hdb driver.
	_ "github.com/SAP/go-hdb/driver"
	// ini config

	// internal
	"github.com/morxs/go-hana/utils"
	// cli
	"github.com/urfave/cli"
)

const (
	t024eSQL = `select
*
from sapabap1.t024e
where mandt = '777'`
)

const (
	cFile = "t024e"
)

func main() {
	var sCfg string
	var bLog bool

	app := cli.NewApp()
	app.Name = "T024E"
	app.Usage = "Get table T024E"
	app.Version = "0.1.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config, c",
			Value:       "config.ini",
			Usage:       "Custom config file",
			Destination: &sCfg,
		},
		cli.BoolFlag{
			Name:        "log, l",
			Hidden:      true,
			Usage:       "Enable logging. Log filename will be <query_filename>+.log",
			Destination: &bLog,
		},
	}

	app.Action = func(c *cli.Context) error {
		// read config file
		utils.WriteMsg("READ CONFIG")
		hdbDsn, extension, err := utils.ReadConfig(sCfg)
		// iniCfg, err := ini.Load(sCfg)
		// if err != nil {
		// 	utils.WriteMsg("CONFIG")
		// 	log.Fatal(err)
		// }
		// iniSection := iniCfg.Section("server")
		// iniKeyUsername := iniSection.Key("uid").String()
		// iniKeyPassword := iniSection.Key("pwd").String()
		// iniKeyHost := iniSection.Key("host").String()
		// iniKeyPort := iniSection.Key("port").String()
		// hdbDsn := "hdb://" + iniKeyUsername + ":" + iniKeyPassword + "@" + iniKeyHost + ":" + iniKeyPort

		utils.WriteMsg("OPEN HDB")
		db, err := sql.Open(utils.DriverName, hdbDsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			log.Fatal(err)
		}

		// create file
		filename := cFile + "." + extension
		utils.WriteMsg("CREATE FILE: " + filename)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// try to query
		utils.WriteMsg("QUERY")
		rows, err := db.Query(t024eSQL)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		// prepare file
		utils.WriteMsg("WRITE CSV")
		w := csv.NewWriter(file)
		w.Comma = ';'

		fmt.Println(rows.Columns())

		// add header to file
		rs, _ := rows.Columns()
		var rec []string
		for i := 0; i < len(rs); i++ {
			rec = append(rec, rs[i])
		}
		w.Write(rec)

		for rows.Next() {
			var mandt, ekorg, ekotx, bukrs, txadr string
			var txkop, txfus, txgru, kalse, mkals string
			var bpeff, bukrs_ntr string

			if err := rows.Scan(&mandt, &ekorg, &ekotx, &bukrs, &txadr, &txkop, &txfus, &txgru, &kalse, &mkals, &bpeff, &bukrs_ntr); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			/*
				var bi big.Int
				var z float64
				var neg bool
				var i int
			*/

			var record []string

			record = append(record, mandt)
			record = append(record, ekorg)
			record = append(record, ekotx)
			record = append(record, bukrs)
			record = append(record, txadr)
			record = append(record, txkop)
			record = append(record, txfus)
			record = append(record, txgru)
			record = append(record, kalse)
			record = append(record, mkals)
			record = append(record, bpeff)
			record = append(record, bukrs_ntr)
			w.Write(record)
			//fmt.Println(record)
		}
		w.Flush()

		if err := rows.Err(); err != nil {
			utils.WriteMsg("ROWS")
			log.Fatal(err)
		}
		return nil
	}

	// init the program
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
