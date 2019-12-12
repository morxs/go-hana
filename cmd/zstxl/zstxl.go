package main // import "github.com/morxs/go-hana"

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	// Register hdb driver.
	_ "github.com/SAP/go-hdb/driver"
	// ini config

	// internal
	"github.com/morxs/go-hana/utils"
	// cli
	"github.com/urfave/cli"
)

const (
	zstxlSQL = `select
MANDT,
TDOBJECT,
TDNAME,
TDID,
TDSPRAS,
LINNO,
TDLINE
from sapabap1.zstxl
where tdname in
(
	select
	concat(a.ebeln,a.ebelp) as "TDNAME"
	from sapabap1.ekpo a
	left join sapabap1.ekko b
	on a.mandt = b.mandt
	and a.ebeln = b.ebeln
	where b.aedat between ? and ?
	and b.bstyp = 'F'
	and (b.bsart like '%20' or b.bsart like '%25')
	and b.loekz = ''
	and a.loekz = ''
	and b.bukrs in ($$coy$$)
)`
)

const (
	cFile = "zstxl"
)

func init() {
}

func main() {
	var sCfg, sStartDate, sEndDate string
	var bLog bool

	app := cli.NewApp()
	app.Name = "ZSTXL"
	app.Usage = "Get table ZSTXL"
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
			Usage:       "Start Date (SAP format)",
			Destination: &sStartDate,
		},
		cli.StringFlag{
			Name:        "end, e",
			Usage:       "End Date (SAP format)",
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
		hdbDsn, extension, err := utils.ReadConfig(sCfg)

		// iniCfg, err := ini.ShadowLoad(sCfg) //Load(sCfg)
		// if err != nil {
		// 	utils.WriteMsg("CONFIG")
		// 	log.Fatal(err)
		// }
		// iniSection := iniCfg.Section("server")
		// iniKeyUsername := iniSection.Key("uid").String()
		// iniKeyPassword := iniSection.Key("pwd").String()
		// iniKeyHost := iniSection.Key("host").String()
		// iniKeyPort := iniSection.Key("port").String()

		// iniSaveSection := iniCfg.Section("save")
		// iniExtension := iniSaveSection.Key("extension").String()

		// // test out nested value
		// iniOption := iniCfg.Section("option")
		// iniCompany := iniOption.Key("company").ValueWithShadows()
		// for _, ic := range iniCompany {
		// 	fmt.Println(ic)
		// }

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
		sql := strings.Replace(zstxlSQL, "$$coy$$", utils.AfricaCoy, -1)
		rows, err := db.Query(sql, sStartDate, sEndDate)
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
			var mandt, tdobject, tdname, tdid, tdspras, linno, tdline string
			if err := rows.Scan(&mandt, &tdobject, &tdname, &tdid, &tdspras, &linno, &tdline); err != nil {
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
			record = append(record, tdobject)
			record = append(record, tdname)
			record = append(record, tdid)
			record = append(record, tdspras)
			record = append(record, linno)
			// delete all \n
			tdline = strings.Replace(tdline, "\n", " ", -1)
			tdline = strings.Replace(tdline, "\r", " ", -1)
			record = append(record, tdline)
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
