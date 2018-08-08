package main

import (
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli"
)

func main() {
	var sCfg, sQuery string
	var bLog bool

	app := cli.NewApp()
	app.Name = "cli_usage"
	app.Usage = "test cli usage"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config, c",
			Value:       "config.ini",
			Usage:       "Custom config file",
			Destination: &sCfg,
		},
		cli.StringFlag{
			Name:        "query, q",
			Value:       "query.sql",
			Usage:       "SQL sQuery to execute",
			Destination: &sQuery,
		},
		cli.BoolFlag{
			Name: "log, l",
			// Value: false,
			Hidden:      false,
			Usage:       "Enable logging. Log filename will be <query_filename>+.log",
			Destination: &bLog,
		},
	}

	app.Action = func(c *cli.Context) error {
		fmt.Println(sCfg)
		fmt.Println(sQuery)
		fmt.Println(bLog)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
