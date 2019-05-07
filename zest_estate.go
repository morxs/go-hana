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
	zestEstateSQL = `select 
	MANDT
	, BUKRS
	, ESTNR
	, KDATB
	, KDATE
	, RGNNR
	, NAME1
	, ADRNR
	, ADRNR2
	, ADRNR3
	, ORT01
	, LAND1
	, CONT1
	, TEL_NUMBER
	, FAX_NUMBER
	, PSTLZ
	, LOEKZ
	, ERNAM
	, ERDAT
	, ERZET
	, AENAM
	, AEDAT
	, AEZET
	, ESTATE
	, WERKS
	, LGORT
	, LGORT2
	, LGORT3
	, LGORT4
	, ZLDAT
	, MENAM
	, MEDAT
	, MEZET
	, AMNAM
	, AMDAT
	, AMZET
	, PRO01
	from sapabap1.zest_estate`
)

const (
	cFile = "zest_estate.csv"
)

func main() {
	var sCfg string
	var bLog bool

	app := cli.NewApp()
	app.Name = "ZEST_ESTATE"
	app.Usage = "Get table ZEST_ESTATE"
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
		rows, err := db.Query(zestEstateSQL)
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
			var mandt, bukrs, estnr, kdatb, kdate string
			var rgnnr, name1, adrnr, adrnr2, adrnr3 string
			var ort01, land1, cont1, tel_number, fax_number string
			var pstlz, loekz, ernam, erdat, erzet string
			var aenam, aedat, aezet, estate, werks string
			var lgort, lgort2, lgort3, lgort4, zldat string
			var menam, medat, mezet, amnam, amdat string
			var amzet, pro01 string

			if err := rows.Scan(&mandt, &bukrs, &estnr, &kdatb, &kdate, &rgnnr, &name1, &adrnr, &adrnr2, &adrnr3, &ort01, &land1, &cont1, &tel_number, &fax_number, &pstlz, &loekz, &ernam, &erdat, &erzet, &aenam, &aedat, &aezet, &estate, &werks, &lgort, &lgort2, &lgort3, &lgort4, &zldat, &menam, &medat, &mezet, &amnam, &amdat, &amzet, &pro01); err != nil {
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
			record = append(record, kdatb)
			record = append(record, kdate)
			record = append(record, rgnnr)
			record = append(record, name1)
			record = append(record, adrnr)
			record = append(record, adrnr2)
			record = append(record, adrnr3)
			record = append(record, ort01)
			record = append(record, land1)
			record = append(record, cont1)
			record = append(record, tel_number)
			record = append(record, fax_number)
			record = append(record, pstlz)
			record = append(record, loekz)
			record = append(record, ernam)
			record = append(record, erdat)
			record = append(record, erzet)
			record = append(record, aenam)
			record = append(record, aedat)
			record = append(record, aezet)
			record = append(record, estate)
			record = append(record, werks)
			record = append(record, lgort)
			record = append(record, lgort2)
			record = append(record, lgort3)
			record = append(record, lgort4)
			record = append(record, zldat)
			record = append(record, menam)
			record = append(record, medat)
			record = append(record, mezet)
			record = append(record, amnam)
			record = append(record, amdat)
			record = append(record, amzet)
			record = append(record, pro01)

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
