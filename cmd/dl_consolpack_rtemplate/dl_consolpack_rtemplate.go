package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	// Register hdb driver.
	_ "github.com/SAP/go-hdb/driver"

	"github.com/urfave/cli"

	// ini config
	"github.com/go-ini/ini"
	// internal
	"github.com/morxs/go-hana/utils"
)

/*
type argT struct {
	cli.Helper
	ArgConfig string `cli:"c" usage:"Custom config file" dft:"config.ini"`
}
*/

const (
	CPASQL = `select
"REPORT",
"ReportWorksheet ID" as "ReportWorksheet_ID",
"Worksheet ID" as "Worksheet_ID",
"Worksheet Desc" as "Worksheet_Desc",
"Report Table" as "Report_Table",
"GL Account" as "GL_Account",
"Transaction Type" as "Transaction_Type",
"GL Account Sort" as "GL_Account_Sort",
"Transaction Type Sort" as "Transaction_Type_Sort",
"Header GL Account" as "Header_GL_Account",
"SIGN",
"Header Trx Type" as "Header_Trx_Type",
"Header Worksheet" as "Header_Worksheet"
from z_wilmar_consodb.consolpack_rtemplate`
)

const (
	cFile = "consolpack_rtemplate.csv"
)

func main() {
	var sCfg string

	app := cli.NewApp()
	app.Name = "consolpack_rtemplate"
	app.Usage = "Get consolpack template"
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
			var report, reportworksheet_id, worksheet_id, worksheet_desc, report_table, gl_account, transaction_type, gl_account_sort, transaction_type_sort, header_gl_account, sign, header_trx_type, header_worksheet string
			if err := rows.Scan(&report, &reportworksheet_id, &worksheet_id, &worksheet_desc, &report_table, &gl_account, &transaction_type, &gl_account_sort, &transaction_type_sort, &header_gl_account, &sign, &header_trx_type, &header_worksheet); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			var record []string

			record = append(record, report)
			record = append(record, reportworksheet_id)
			record = append(record, worksheet_id)
			record = append(record, worksheet_desc)
			record = append(record, report_table)
			record = append(record, gl_account)
			record = append(record, transaction_type)
			record = append(record, gl_account_sort)
			record = append(record, transaction_type_sort)
			record = append(record, header_gl_account)
			record = append(record, sign)
			record = append(record, header_trx_type)
			record = append(record, header_worksheet)

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

	/*
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
			//fmt.Print("OPENDB...")
			db, err := sql.Open(driverName, hdbDsn)
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
				var report, reportworksheet_id, worksheet_id, worksheet_desc, report_table, gl_account, transaction_type, gl_account_sort, transaction_type_sort, header_gl_account, sign, header_trx_type, header_worksheet string
				if err := rows.Scan(&report, &reportworksheet_id, &worksheet_id, &worksheet_desc, &report_table, &gl_account, &transaction_type, &gl_account_sort, &transaction_type_sort, &header_gl_account, &sign, &header_trx_type, &header_worksheet); err != nil {
					utils.WriteMsg("SCAN")
					log.Fatal(err)
				}

				var record []string

				record = append(record, report)
				record = append(record, reportworksheet_id)
				record = append(record, worksheet_id)
				record = append(record, worksheet_desc)
				record = append(record, report_table)
				record = append(record, gl_account)
				record = append(record, transaction_type)
				record = append(record, gl_account_sort)
				record = append(record, transaction_type_sort)
				record = append(record, header_gl_account)
				record = append(record, sign)
				record = append(record, header_trx_type)
				record = append(record, header_worksheet)

				w.Write(record)
			}
			w.Flush()

			if err := rows.Err(); err != nil {
				utils.WriteMsg("ROWS")
				log.Fatal(err)
			}
			return nil
		})
	*/
}
