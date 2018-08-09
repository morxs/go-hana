package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/morxs/go-hana/utils"
	"github.com/urfave/cli"
)

/*
type struct DDL {
    Name string
    Type string
}*/

func main() {
	const (
		Filename              = "ddf.csv"
		MaxField              = 5
		AppendStringTemplate  = "record = append(record, $)"
		AppendIntTemplate     = "record = append(record, strconv.Itoa($))"
		AppendDecimalTemplate = `neg, i = utils.DecodeDecimal($, &bi)
z = utils.BigIntToFloat(neg, &bi, i)
record = append(record, fmt.Sprintf("%.4f", z))`
	)

	var sCSVFile string

	app := cli.NewApp()
	app.Name = "dl_gl_consol_pack_map"
	app.Usage = "Get consolpack mapping"
	app.Version = "0.0.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "file, f",
			Value:       "ddl.csv",
			Usage:       "DDL (.csv, comma-seperated)",
			Destination: &sCSVFile,
		},
	}
	app.Action = func(c *cli.Context) error {

		if sCSVFile == "" {
			log.Fatal("No CSV file supplied. Please supply CSV file.")
		}

		//var rec [][]string
		var RecString []string
		var RecInt []string
		var RecDecimal []string
		var readCount int

		ScanString := "if err := rows.Scan("

		var AppendString []string

		readCount = 1
		rec, _ := utils.ReadCsv(sCSVFile, '\t')
		for i := 0; i < len(rec); i++ {
			switch strings.ToUpper(rec[i][1]) {
			case "NVARCHAR", "VARCHAR":
				RecString = append(RecString, strings.Replace(strings.ToLower(rec[i][0]), "/", "", -1))
				AppendString = append(AppendString, strings.Replace(AppendStringTemplate, "$", strings.Replace(strings.ToLower(rec[i][0]), "/", "", -1), 1))
			case "DECIMAL":
				RecDecimal = append(RecDecimal, strings.Replace(strings.ToLower(rec[i][0]), "/", "", -1))
				AppendString = append(AppendString, strings.Replace(AppendDecimalTemplate, "$", strings.Replace(strings.ToLower(rec[i][0]), "/", "", -1), 1))
			case "INTEGER", "SMALLINT":
				RecInt = append(RecInt, strings.Replace(strings.ToLower(rec[i][0]), "/", "", -1))
				AppendString = append(AppendString, strings.Replace(AppendIntTemplate, "$", strings.Replace(strings.ToLower(rec[i][0]), "/", "", -1), 1))
			}
			fmt.Println(rec[i])
			ScanString = ScanString + "&" + strings.Replace(strings.ToLower(rec[i][0]), "/", "", -1)
			if readCount != len(rec) {
				ScanString = ScanString + ", "
			} else {
				ScanString = ScanString + ")"
			}
			readCount++
		}

		// variable code generation
		fmt.Println("----------------------- VAR ----------------------")
		readCount = 1
		for i := 0; i < len(RecString); i++ {
			if readCount%MaxField == 0 {
				//fmt.Print(strings.Replace(strings.ToLower(RecString[i]), "/", "", -1))
				fmt.Print(RecString[i])
				fmt.Println(" string")
			} else {
				if readCount%MaxField == 1 {
					fmt.Print("var ")
				}
				//fmt.Print(strings.Replace(strings.ToLower(RecString[i]), "/", "", -1))
				fmt.Print(RecString[i])
				if readCount == len(RecString) {
					fmt.Println(" string")
				} else {
					fmt.Print(", ")
				}
			}
			readCount++
		}

		readCount = 1
		for i := 0; i < len(RecInt); i++ {
			if readCount%MaxField == 0 {
				//fmt.Print(strings.Replace(strings.ToLower(RecInt[i]), "/", "", -1))
				fmt.Print(RecInt[i])
				fmt.Println(" int")
			} else {
				if readCount%MaxField == 1 {
					fmt.Print("var ")
				}
				//fmt.Print(strings.Replace(strings.ToLower(RecInt[i]), "/", "", -1))
				fmt.Print(RecInt[i])
				if readCount == len(RecInt) {
					fmt.Println(" int")
				} else {
					fmt.Print(", ")
				}
			}
			readCount++
		}

		readCount = 1
		for i := 0; i < len(RecDecimal); i++ {
			if readCount%MaxField == 0 {
				//fmt.Print(strings.Replace(strings.ToLower(RecDecimal[i]), "/", "", -1))
				fmt.Print(RecDecimal[i])
				fmt.Println(" []byte")
			} else {
				if readCount%MaxField == 1 {
					fmt.Print("var ")
				}
				//fmt.Print(strings.Replace(strings.ToLower(RecDecimal[i]), "/", "", -1))
				fmt.Print(RecDecimal[i])
				if readCount == len(RecDecimal) {
					fmt.Println(" []byte")
				} else {
					fmt.Print(", ")
				}
			}
			readCount++
		}

		// scan code generation
		fmt.Println("----------------------- SCAN ----------------------")
		fmt.Println(ScanString)

		// assignment code generation
		fmt.Println("----------------------- APPEND ----------------------")
		for i := 0; i < len(AppendString); i++ {
			fmt.Println(AppendString[i])
		}

		return nil
	}

	// init the program
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
