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
	"github.com/go-ini/ini"
	// internal
	"github.com/morxs/go-hana/utils"
	// cli
	"github.com/urfave/cli"
)

const (
	t024SQL = `select
MANDT
, EKGRP
, EKNAM
, EKTEL
, LDEST
, TELFX
, TEL_NUMBER
, TEL_EXTENS
, SMTP_ADDR
from z_wilmar1.t024
where mandt = '777'`
)

const (
<<<<<<< HEAD
	cFile = "t024.csv"
=======
	File = "t024.xls"
>>>>>>> origin/master
)

func main() {
	var sCfg, sStartDate, sEndDate string
	var bLog bool

	app := cli.NewApp()
	app.Name = "T024"
	app.Usage = "Get table T024"
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
		iniCfg, err := ini.Load(sCfg)
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
		utils.WriteMsg("CREATE FILE: " + cFile)
		file, err := os.Create(cFile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// try to query
		utils.WriteMsg("QUERY")
		rows, err := db.Query(t024SQL)
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
			var mandt, ekgrp, eknam, ektel, ldest string
			var telfx, tel_number, tel_extens, smtp_addr string

			if err := rows.Scan(&mandt, &ekgrp, &eknam, &ektel, &ldest, &telfx, &tel_number, &tel_extens, &smtp_addr); err != nil {
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
			record = append(record, ekgrp)
			record = append(record, eknam)
			record = append(record, ektel)
			record = append(record, ldest)
			record = append(record, telfx)
			record = append(record, tel_number)
			record = append(record, tel_extens)
			record = append(record, smtp_addr)
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
