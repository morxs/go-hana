package main // import "github.com/morxs/go-hana"

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
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
	zestRdaySQL = `select
MANDT
, BUKRS
, ESTNR
, BUDAT
, ZAUKUR
, RDAY
, RKALI
, RUKUR
, RDESC
, RLAMA
, ZLOCK
, ZUKUR
, NRDAY
from sapabap1.zest_rday
where budat between ? and ?`
)

const (
	cFile = "zest_rday.csv"
)

func main() {
	var sCfg string
	var sStartDate, sEndDate string
	var bLog bool

	app := cli.NewApp()
	app.Name = "ZEST_RDAY"
	app.Usage = "Get table ZEST_RDAY"
	app.Version = "0.1.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config, c",
			Value:       "config.ini",
			Usage:       "Custom config file",
			Destination: &sCfg,
		},
		cli.StringFlag{
			Name:        "start, s",
			Usage:       "Start Period (SAP format)",
			Destination: &sStartDate,
		},
		cli.StringFlag{
			Name:        "end, e",
			Usage:       "End Period (SAP format)",
			Destination: &sEndDate,
		},
		cli.BoolFlag{
			Name:        "log, l",
			Hidden:      true,
			Usage:       "Enable logging. Log filename will be <query_filename>+.log",
			Destination: &bLog,
		},
	}

	app.Action = func(c *cli.Context) error {
		if sStartDate == "" || sEndDate == "" {
			log.Fatal("You need to enter Start and End Date")
		}

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
		rows, err := db.Query(zestRdaySQL, sStartDate, sEndDate)
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
			var mandt, bukrs, estnr, budat, zaukur string
			var rday, rdesc, zlock, nrday string
			var rkali, rukur, rlama, zukur []byte

			if err := rows.Scan(&mandt, &bukrs, &estnr, &budat, &zaukur, &rday, &rkali, &rukur, &rdesc, &rlama, &zlock, &zukur, &nrday); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			var bi big.Int
			var z float64
			var neg bool
			var i int

			var record []string
			record = append(record, mandt)
			record = append(record, bukrs)
			record = append(record, estnr)
			record = append(record, budat)
			record = append(record, zaukur)
			record = append(record, rday)
			neg, i = utils.DecodeDecimal(rkali, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(rukur, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, rdesc)
			neg, i = utils.DecodeDecimal(rlama, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, zlock)
			neg, i = utils.DecodeDecimal(zukur, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, nrday)
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
