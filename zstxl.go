package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

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
	ArgStart  string `cli:"*s" usage:"Start Date (SAP format)"`
	ArgEnd    string `cli:"*e" usage:"End Date (SAP format)"`
	ArgConfig string `cli:"c" usage:"Custom config file" dft:"config.ini"`
}

const (
	driverName = "hdb"
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
from z_wilmar1.zstxl
where tdname in
(
	select
	concat(a.ebeln,a.ebelp) as "TDNAME"
	from z_wilmar1.ekpo a
	left join z_wilmar1.ekko b
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

const (
	File = "zstxl.csv"
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
		utils.WriteMsg("CREATE FILE: " + File)
		file, err := os.Create(File)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// try to query
		utils.WriteMsg("QUERY")
		rows, err := db.Query(zstxlSQL, argv.ArgStart, argv.ArgEnd)
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
	})
}
