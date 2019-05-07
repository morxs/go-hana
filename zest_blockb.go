package main // import "github.com/morxs/go-hana"

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"

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
	zestBlockBSQL = `select 
	MANDT
	, BUKRS
	, ESTNR
	, DIVNR
	, BLOCK
	, SPMON
	, POINT
	, CLONAL
	, DXPP
	, HECTR
	, PLNTD
	, UPLNT
	, OBMATRE
	, OBMAINT
	, ZLOCK
	, ZLOCKC
	, QBDGT
	, MATNR
	, TOTKG
	, RATEP
	, UBDGT
	, REMRK
	, PROHA
	, ERNAM
	, ERDAT
	, ERZET
	, AENAM
	, AEDAT
	, AEZET
	, REMRK2
	, ERNAM2
	, ERDAT2
	, ERZET2
	, AENAM2
	, AEDAT2
	, AEZET2
	from sapabap1.zest_blockb
	where spmon between ? and ?`
)

const (
	cFile = "zest_blockb.csv"
)

func main() {
	var sCfg string
	var sStartDate, sEndDate string
	var bLog bool

	app := cli.NewApp()
	app.Name = "ZEST_BLOCKB"
	app.Usage = "Get table ZEST_BLOCKB"
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
		rows, err := db.Query(zestBlockBSQL, sStartDate, sEndDate)
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
			var spmon, obmatre, obmaint, zlock, zlockc string
			var matnr, ubdgt, remrk, ernam, erdat string
			var erzet, aenam, aedat, aezet, remrk2 string
			var ernam2, erdat2, erzet2, aenam2, aedat2 string
			var aezet2 string
			var point, clonal, dxpp int
			var hectr, plntd, uplnt, qbdgt, totkg []byte
			var ratep, proha []byte

			if err := rows.Scan(&mandt, &bukrs, &estnr, &divnr, &block, &spmon, &point, &clonal, &dxpp, &hectr, &plntd, &uplnt, &obmatre, &obmaint, &zlock, &zlockc, &qbdgt, &matnr, &totkg, &ratep, &ubdgt, &remrk, &proha, &ernam, &erdat, &erzet, &aenam, &aedat, &aezet, &remrk2, &ernam2, &erdat2, &erzet2, &aenam2, &aedat2, &aezet2); err != nil {
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
			record = append(record, divnr)
			record = append(record, block)
			record = append(record, spmon)
			record = append(record, strconv.Itoa(point))
			record = append(record, strconv.Itoa(clonal))
			record = append(record, strconv.Itoa(dxpp))
			neg, i = utils.DecodeDecimal(hectr, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(plntd, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(uplnt, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, obmatre)
			record = append(record, obmaint)
			record = append(record, zlock)
			record = append(record, zlockc)
			neg, i = utils.DecodeDecimal(qbdgt, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, matnr)
			neg, i = utils.DecodeDecimal(totkg, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(ratep, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, ubdgt)
			record = append(record, remrk)
			neg, i = utils.DecodeDecimal(proha, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, ernam)
			record = append(record, erdat)
			record = append(record, erzet)
			record = append(record, aenam)
			record = append(record, aedat)
			record = append(record, aezet)
			record = append(record, remrk2)
			record = append(record, ernam2)
			record = append(record, erdat2)
			record = append(record, erzet2)
			record = append(record, aenam2)
			record = append(record, aedat2)
			record = append(record, aezet2)
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
