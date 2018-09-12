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

func main() {
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
	and b.bukrs in ('BM', 'BO', 'CL', 'DE', 'EB', 'EC', 'EE', 'EL', 'EP', 'ES', 'FB', 'FM', 'GM', 'GU', 'HM', 'JW', 'KI', 'KM', 'NE', 'NO', 'NS', 'NX', 'OE', 'PB', 'PE', 'PO', 'RB', 'RH', 'RM', 'SE', 'SF', 'SG', 'SH', 'SO', 'SU', 'VI', 'WH',
	'AA', 'AD', 'AG', 'AJ', 'AN', 'AP', 'BN', 'BV', 'BW', 'BX', 'BY', 'CA', 'CC', 'CX', 'DA',
	'DB', 'DC', 'DG', 'DI', 'GA', 'GK', 'IA', 'ID', 'IE', 'IF', 'KD', 'KF', 'KG', 'MD', 'MF', 'MH',
	'MJ', 'MO', 'NI', 'PA', 'PF', 'PR', 'PT', 'PV', 'PX', 'RA', 'RJ',
	'SB', 'SJ', 'SN', 'SV', 'SX', 'TB', 'TC', 'TM', 'TN', 'UD', 'UI', 'WJ')
)`
	)

	var sCfg, sStartDate, sEndDate, sOutputFile string
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
		cli.StringFlag{
			Name:        "output, o",
			Usage:       "Output file",
			Value:       "zstxl.csv",
			Destination: &sOutputFile,
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
		hdbDsn, err := utils.ReadConfig(sCfg)
		if err != nil {
			log.Fatal(err)
		}

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
		utils.WriteMsg("CREATE FILE: " + sOutputFile)
		file, err := os.Create(sOutputFile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// try to query
		utils.WriteMsg("QUERY")
		rows, err := db.Query(zstxlSQL, sStartDate, sEndDate)
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
