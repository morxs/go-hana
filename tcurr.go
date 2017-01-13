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

const (
	File = "tcurr.csv"
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
		utils.WriteMsg("CREATE FILE: " + File)
		file, err := os.Create(File)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// try to query
		utils.WriteMsg("QUERY")
		rows, err := db.Query(tcurrSQL, argv.ArgStart, argv.ArgEnd)
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
	})
}
