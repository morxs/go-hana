package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"

	// Register hdb driver.
	_ "github.com/SAP/go-hdb/driver"

	// internal
	"github.com/morxs/go-hana/utils"
	// cli
	"github.com/urfave/cli"
)

func main() {
	const (
		tcurrSQL = `select MANDT,
	   	KURST,
	   	FCURR,
	   	TCURR,
	   	GDATU,
	   	UKURS,
	   	FFACT,
	   	TFACT,
	   	DATUM
	   from
	   (
	   	select
	   	MANDT,
	   	KURST,
	   	FCURR,
	   	TCURR,
	   	GDATU,
		UKURS as "UKURS",
		FFACT as "FFACT",
		TFACT as "TFACT",
	   	cast(('99999999' - gdatu) as varchar(8)) as "DATUM"
	   	from z_wilmar1.tcurr
	   	where mandt = '777'
		and kurst = 'M'
	   )
	   where "DATUM" between ? and ?`
	)

	var sCfg, sStartDate, sEndDate, sOutputFile string
	var bLog bool

	app := cli.NewApp()
	app.Name = "TCURR"
	app.Usage = "Get table TCURR"
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
			Value:       "tcurr.csv",
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
		rows, err := db.Query(tcurrSQL, sStartDate, sEndDate)
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
			var mandt, kurst, fcurr, tcurr, gdatu, datum string
			//var ukurs, ffact, tfact string
			//var ukurs2, tfact2, ffact2 []byte
			var ukurs, ffact, tfact []byte
			if err := rows.Scan(&mandt, &kurst, &fcurr, &tcurr, &gdatu, &ukurs, &ffact, &tfact, &datum); err != nil {
				//if err := rows.Scan(&mandt, &kurst, &fcurr, &tcurr, &gdatu, ukurs, ffact, tfact, &datum); err != nil {
				//fmt.Print("SCAN")
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}
			//encoded := base64.StdEncoding.EncodeToString(ukurs)
			//fmt.Println("....>", ukurs2, ffact2, tfact2)
			/*
				var z float64
				buf := bytes.NewReader(ukurs2)
				err := binary.Read(buf, binary.LittleEndian, &z)
				if err != nil {
					fmt.Println("binary.Read failed:", err)
				}
				fmt.Println(z)
			*/
			//fmt.Print(strconv.Itoa(ukurs[1]), strconv.Itoa(ukurs[2]))

			var bi big.Int
			var z float64
			var neg bool
			var i int

			var record []string

			record = append(record, mandt)
			record = append(record, kurst)
			record = append(record, fcurr)
			record = append(record, tcurr)
			record = append(record, gdatu)

			// ukurs
			neg, i = utils.DecodeDecimal(ukurs, &bi)
			//fmt.Println(neg, bi, i)
			z = utils.BigIntToFloat(neg, &bi, i)
			//fmt.Printf("%.4f", z)
			record = append(record, fmt.Sprintf("%.4f", z))

			// ffact
			neg, i = utils.DecodeDecimal(ffact, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			// tfact
			neg, i = utils.DecodeDecimal(tfact, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			//record = append(record, string(ffact2))
			/*
				record = append(record, string(ukurs[:bytes.IndexByte(ukurs, 0)]))
				record = append(record, string(ffact[:bytes.IndexByte(ffact, 0)]))
				record = append(record, string(tfact[:bytes.IndexByte(tfact, 0)]))
			*/
			/*
				record = append(record, strconv.FormatFloat(ukurs, 'f', 4, 64))
				record = append(record, strconv.FormatFloat(ffact, 'f', 4, 64))
				record = append(record, strconv.FormatFloat(tfact, 'f', 4, 64))
			*/
			record = append(record, datum)
			w.Write(record)
			//fmt.Println(record)
			//fmt.Printf("%s;%s;%s;%s;%s;%v;%v;%v;%s\n", mandt, kurst, fcurr, tcurr, gdatu, ukurs, ffact, tfact, datum)
		}
		w.Flush()

		if err := rows.Err(); err != nil {
			//fmt.Print("ROWS")
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
