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
	zestBlock2SQL = `select 
	MANDT
	, BUKRS
	, ESTNR
	, DIVNR
	, BLOCK
	, KDATB
	, KDATE
	, PLBLK
	, UPBLK
	, LCBLK
	, PHASE
	, LNTYP
	, HGU
	, ABLOCK
	, RBLOCK
	, BNAME
	, RPLNT
	, BTYPE
	, TPGRP
	, SEEDO
	, BYPLAN
	, INITL
	, BMAINT
	, BMATRE
	, REBGT
	, ERNAM
	, ERDAT
	, ERZET
	, AENAM
	, AEDAT
	, AEZET
	from sapabap1.zest_block2`
)

const (
	cFile = "zest_block2.csv"
)

func main() {
	var sCfg string
	var bLog bool

	app := cli.NewApp()
	app.Name = "ZEST_BLOCK2"
	app.Usage = "Get table ZEST_BLOCK2"
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
		rows, err := db.Query(zestBlock2SQL)
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
			var mandt, bukrs, estnr, divnr, block string
			var kdatb, kdate, plblk, upblk, lcblk string
			var phase, lntyp, hgu, ablock, rblock string
			var bname, rplnt, btype, tpgrp, seedo string
			var byplan, initl, bmaint, bmatre, rebgt string
			var ernam, erdat, erzet, aenam, aedat string
			var aezet string

			if err := rows.Scan(&mandt, &bukrs, &estnr, &divnr, &block, &kdatb, &kdate, &plblk, &upblk, &lcblk, &phase, &lntyp, &hgu, &ablock, &rblock, &bname, &rplnt, &btype, &tpgrp, &seedo, &byplan, &initl, &bmaint, &bmatre, &rebgt, &ernam, &erdat, &erzet, &aenam, &aedat, &aezet); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			var record []string
			record = append(record, mandt)
			record = append(record, bukrs)
			record = append(record, estnr)
			record = append(record, divnr)
			record = append(record, block)
			record = append(record, kdatb)
			record = append(record, kdate)
			record = append(record, plblk)
			record = append(record, upblk)
			record = append(record, lcblk)
			record = append(record, phase)
			record = append(record, lntyp)
			record = append(record, hgu)
			record = append(record, ablock)
			record = append(record, rblock)
			record = append(record, bname)
			record = append(record, rplnt)
			record = append(record, btype)
			record = append(record, tpgrp)
			record = append(record, seedo)
			record = append(record, byplan)
			record = append(record, initl)
			record = append(record, bmaint)
			record = append(record, bmatre)
			record = append(record, rebgt)
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
