package main // import "github.com/morxs/go-hana"

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
	zestDivisionSQL = `select 
	MANDT
	, BUKRS
	, ESTNR
	, DIVNR
	, KDATB
	, KDATE
	, NAME1
	, CROP1
	, HEAD
	, LOEKZ
	, ERNAM
	, ERDAT
	, ERZET
	, AENAM
	, AEDAT
	, AEZET
	from sapabap1.zest_division`
)

const (
	cFile = "zest_division.csv"
)

func main() {
	var sCfg string
	var bLog bool

	app := cli.NewApp()
	app.Name = "ZEST_DIVISION"
	app.Usage = "Get table ZEST_DIVISION"
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
		rows, err := db.Query(zestDivisionSQL)
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
			var mandt, bukrs, estnr, divnr, kdatb string
			var kdate, name1, crop1, head, loekz string
			var ernam, erdat, erzet, aenam, aedat string
			var aezet string

			if err := rows.Scan(&mandt, &bukrs, &estnr, &divnr, &kdatb, &kdate, &name1, &crop1, &head, &loekz, &ernam, &erdat, &erzet, &aenam, &aedat, &aezet); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			// var bi big.Int
			// var z float64
			// var neg bool
			// var i int

			var record []string
			record = append(record, mandt)
			record = append(record, bukrs)
			record = append(record, estnr)
			record = append(record, divnr)
			record = append(record, kdatb)
			record = append(record, kdate)
			record = append(record, name1)
			record = append(record, crop1)
			record = append(record, head)
			record = append(record, loekz)
			record = append(record, ernam)
			record = append(record, erdat)
			record = append(record, erzet)
			record = append(record, aenam)
			record = append(record, aedat)
			record = append(record, aezet)
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
