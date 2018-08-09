package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	// Register hdb driver.
	_ "github.com/SAP/go-hdb/driver"
	// internals
	"github.com/morxs/go-hana/utils"
	//cli
	"github.com/urfave/cli"

	// ini config
	"github.com/go-ini/ini"
)

/*
type argT struct {
	cli.Helper
	ArgConfig string `cli:"c" usage:"Custom config file" dft:"config.ini"`
	ArgSQL    string `cli:"*s" usage:"SQL query to execute" dft="query.sql"`
	ArgLog    bool   `cli:"l" usage:"Enable logging. Log filename will be query filename + .log"`
}
*/

func main() {
	var sCfg, sSQL string
	var bLog bool

	app := cli.NewApp()
	app.Name = "perftest"
	app.Usage = "Performance Test for HANA"
	app.Version = "0.0.2"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config, c",
			Value:       "config.ini",
			Usage:       "Custom config file",
			Destination: &sCfg,
		},
		cli.StringFlag{
			Name:        "query, q",
			Usage:       "SQL query file",
			Destination: &sSQL,
		},
		cli.BoolFlag{
			Name:        "log, l",
			Destination: &bLog,
		},
	}

	app.Action = func(c *cli.Context) error {
		if sSQL == "" {
			log.Fatal("You need to enter Start and End Date also Created Start and End Date")
		}

		// write to log if enable
		var fLog os.File

		if bLog {
			// prepare log file
			strLogFile := sSQL + ".log"
			fLog, err := os.OpenFile(strLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Fatal(err)
			}
			log.SetOutput(fLog)

		}
		defer fLog.Close()

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

		// utils.WriteMsg("OPEN SQL")
		log.Println("OPEN SQL: " + sSQL)
		fSQL, err := ioutil.ReadFile(sSQL)
		if err != nil {
			log.Fatal(err)
		}

		sqlSQL := string(fSQL)

		//fmt.Println(sqlSQL)

		// utils.WriteMsg("OPEN HDB")
		log.Println("OPEN HDB")
		db, err := sql.Open(utils.DriverName, hdbDsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			log.Fatal(err)
		}

		// create file
		/*
			utils.WriteMsg("CREATE FILE: " + File)
			file, err := os.Create(File)
			if err != nil {
				fmt.Println(err)
			}
			defer file.Close()
		*/

		// log starttime
		startTime := time.Now()

		// try to query
		// utils.WriteMsg("QUERY")
		log.Println("QUERY")
		rows, err := db.Query(sqlSQL)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
		}

		if err := rows.Err(); err != nil {
			// utils.WriteMsg("ROWS")
			log.Println("ROWS")
			log.Fatal(err)
		}

		// log end time
		endTime := time.Since(startTime)

		// utils.WriteMsg("DONE")
		log.Println("DONE")

		fmt.Printf("Query took %v\n", endTime)
		log.Println("Elapse: " + endTime.String())
		log.Println()

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

			// write to log if enable
			var fLog os.File

			if argv.ArgLog {
				// prepare log file
				strLogFile := argv.ArgSQL + ".log"
				fLog, err := os.OpenFile(strLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
				if err != nil {
					log.Fatal(err)
				}
				log.SetOutput(fLog)

			}
			defer fLog.Close()

			// read config file
			utils.WriteMsg("START")
			// utils.WriteMsg("READ CONFIG")
			log.Println("READ CONFIG: " + argv.ArgConfig)
			iniCfg, err := ini.Load(argv.ArgConfig)
			if err != nil {
				// utils.WriteMsg("CONFIG")
				log.Println("CONFIG")
				log.Fatal(err)
			}
			iniSection := iniCfg.Section("server")
			iniKeyUsername := iniSection.Key("uid").String()
			iniKeyPassword := iniSection.Key("pwd").String()
			iniKeyHost := iniSection.Key("host").String()
			iniKeyPort := iniSection.Key("port").String()
			hdbDsn := "hdb://" + iniKeyUsername + ":" + iniKeyPassword + "@" + iniKeyHost + ":" + iniKeyPort

			// utils.WriteMsg("OPEN SQL")
			log.Println("OPEN SQL: " + argv.ArgSQL)
			fSQL, err := ioutil.ReadFile(argv.ArgSQL)
			if err != nil {
				log.Fatal(err)
			}

			sqlSQL := string(fSQL)

			//fmt.Println(sqlSQL)

			// utils.WriteMsg("OPEN HDB")
			log.Println("OPEN HDB")
			db, err := sql.Open(utils.DriverName, hdbDsn)
			if err != nil {
				log.Fatal(err)
			}
			defer db.Close()

			if err := db.Ping(); err != nil {
				log.Fatal(err)
			}

			// log starttime
			startTime := time.Now()

			// try to query
			// utils.WriteMsg("QUERY")
			log.Println("QUERY")
			rows, err := db.Query(sqlSQL)
			if err != nil {
				log.Fatal(err)
			}
			defer rows.Close()

			for rows.Next() {
			}

			if err := rows.Err(); err != nil {
				// utils.WriteMsg("ROWS")
				log.Println("ROWS")
				log.Fatal(err)
			}

			// log end time
			endTime := time.Since(startTime)

			// utils.WriteMsg("DONE")
			log.Println("DONE")

			fmt.Printf("Query took %v\n", endTime)
			log.Println("Elapse: " + endTime.String())
			log.Println()

			return nil
		})
	*/
}
