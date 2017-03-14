package main

import (
	"database/sql"
	"fmt"
	"log"
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
	ArgFile   string `cli:"*f" usage:"Bulk data (CSV, semicolon separated) file"`
	ArgConfig string `cli:"c" usage:"Custom config file" dft:"config.ini"`
}

const (
	driverName = "hdb"
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
		iniKeyHost = "10.11.1.53"
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

		stmt, err := db.Prepare("bulk insert into Z_WILMAR_CONSODB.GL_CONSOL_PACK_MAP values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

		// baca file
		rec, _ := utils.ReadCsv(argv.ArgFile, ';')

		for i := 0; i < len(rec); i++ {
			/*
				if rec[i][10] == "" {
					rec[i][10] = sql.NullString{}
				}
			*/
			if _, err := stmt.Exec(
				rec[i][0],
				rec[i][1],
				rec[i][2],
				rec[i][3],
				rec[i][4],
				rec[i][5],
				rec[i][6],
				NewNullString(rec[i][7]),
				NewNullString(rec[i][8]),
				rec[i][9],
				NewNullString(rec[i][10]),
				rec[i][11],
				rec[i][12],
				rec[i][13]); err != nil {
				log.Fatal(err)
			}
		}

		if _, err := stmt.Exec(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("DONE")
		return nil
	})
}

func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}
