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
	"github.com/urfave/cli"
)

/*
type argT struct {
	cli.Helper
	ArgConfig string `cli:"c" usage:"Custom config file" dft:"config.ini"`
}
*/

type GLConsolPackMap struct {
	Year            string
	Group1          string
	Sort1           string
	Group2          string
	Sort2           string
	Group3          string
	Sort3           string
	WorksheetID     sql.NullString
	GLAccount       sql.NullString
	TransactionType string
	Sign            sql.NullString
	ShortCode       string
	Remark          string
	ReportSheet     string
}

func main() {
	const (
		CPASQL = `SELECT
YEAR
, "Group 1"
, "Sort 1"
, "Group 2"
, "Sort 2"
, "Group 3"
, "Sort 3"
, "Worksheet ID"
, "GL Account"
, "Transaction Type"
, "SIGN"
, "Short Code"
, "REMARK"
, "Report Sheet"
FROM Z_WILMAR_CONSODB.GL_CONSOL_PACK_MAP`
		cFile = "consolpack_gl_consol_pack_map.csv"
	)
	var sCfg string

	app := cli.NewApp()
	app.Name = "dl_gl_consol_pack_map"
	app.Usage = "Get consolpack mapping"
	app.Version = "0.0.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config, c",
			Value:       "config.ini",
			Usage:       "Custom config file",
			Destination: &sCfg,
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
		/*
			fmt.Println(iniSection)
			fmt.Println(iniSection.KeyStrings())
			fmt.Println(iniSection.Key("uid").String())
			fmt.Println(iniSection.GetKey("uid"))
		*/
		iniKeyUsername := iniSection.Key("uid").String()
		iniKeyPassword := iniSection.Key("pwd").String()
		iniKeyHost := iniSection.Key("host").String()
		iniKeyHost = "10.11.1.53"
		iniKeyPort := iniSection.Key("port").String()
		hdbDsn := "hdb://" + iniKeyUsername + ":" + iniKeyPassword + "@" + iniKeyHost + ":" + iniKeyPort

		utils.WriteMsg("OPEN HDB")
		//fmt.Print("OPENDB...")
		db, err := sql.Open(utils.DriverName, hdbDsn)
		if err != nil {
			//fmt.Print("OPENDB")
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
		rows, err := db.Query(CPASQL)
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
			var gl GLConsolPackMap
			// var report, reportworksheet_id, worksheet_id, worksheet_desc, report_table, gl_account, transaction_type, gl_account_sort, transaction_type_sort, header_gl_account, sign, header_trx_type, header_worksheet string
			if err := rows.Scan(&gl.Year, &gl.Group1, &gl.Sort1,
				&gl.Group2, &gl.Sort2, &gl.Group3,
				&gl.Sort3, &gl.WorksheetID, &gl.GLAccount,
				&gl.TransactionType, &gl.Sign, &gl.ShortCode,
				&gl.Remark, &gl.ReportSheet); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			var record []string

			record = append(record, gl.Year)
			record = append(record, gl.Group1)
			record = append(record, gl.Sort1)
			record = append(record, gl.Group2)
			record = append(record, gl.Sort2)
			record = append(record, gl.Group3)
			record = append(record, gl.Sort3)
			record = append(record, NewEmptyString(gl.WorksheetID))
			// if gl.WorksheetID.Valid {
			// 	record = append(record, gl.WorksheetID.String)
			// } else {
			// 	record = append(record, "")
			// }
			record = append(record, NewEmptyString(gl.GLAccount))
			// if gl.GLAccount.Valid {
			// 	record = append(record, gl.GLAccount.String)
			// } else {
			// 	record = append(record, "")
			// }
			record = append(record, gl.TransactionType)
			record = append(record, NewEmptyString(gl.Sign))
			// if gl.Sign.Valid {
			// 	record = append(record, gl.Sign.String)
			// } else {
			// 	record = append(record, "")
			// }
			record = append(record, gl.ShortCode)
			record = append(record, gl.Remark)
			record = append(record, gl.ReportSheet)

			w.Write(record)
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

func NewEmptyString(s sql.NullString) string {
	if s.Valid {
		return s.String
	}
	return ""
}
