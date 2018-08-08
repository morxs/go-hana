package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
)

const (
	C_TAB       = "\t"
	C_SPACE     = " "
	C_SEMICOLON = ";"
	C_COMMA     = ","
	C_PERIOD    = "."
)

func main() {

	var (
		// for flag parsing
		//fromDelimit string
		//toDelimit   string
		// for conversion parsing
		tFromDelimit string
		tToDelimit   string
	)

	filenamePtr := flag.String("f", "", "Filename to convert the delimiter")
	verbosePtr := flag.Bool("v", false, "Verbose output")
	fromdelimitPtr := flag.String("from", "TAB", "From delimiter (TAB,SEMICOLON,SPACE)")
	todelimitPtr := flag.String("to", "SEMICOLON", "To delimiter (TAB,SEMICOLON,SPACE)")

	// Parsing flags
	flag.Parse()

	read, err := ioutil.ReadFile(*filenamePtr)
	if err != nil {
		panic(err)
	}

	if *verbosePtr {
		fmt.Println("From:")
		fmt.Println(string(read))
	}

	switch strings.ToUpper(*fromdelimitPtr) {
	default:
		tFromDelimit = C_TAB
	case "SEMICOLON":
		tFromDelimit = C_SEMICOLON
	case "SPACE":
		tFromDelimit = C_SPACE
	case "COMMA":
		tFromDelimit = C_COMMA
	case "PERIOD":
		tFromDelimit = C_PERIOD
	}

	switch strings.ToUpper(*todelimitPtr) {
	default:
		tToDelimit = C_SEMICOLON
	case "TAB":
		tToDelimit = C_TAB
	case "SPACE":
		tToDelimit = C_SPACE
	case "COMMA":
		tToDelimit = C_COMMA
	case "PERIOD":
		tToDelimit = C_PERIOD
	}

	newContent := strings.Replace(string(read), tFromDelimit, tToDelimit, -1)

	if *verbosePtr {
		fmt.Println("\nTo:")
		fmt.Println(newContent)
	}

	err = ioutil.WriteFile(*filenamePtr, []byte(newContent), 0)
	if err != nil {
		panic(err)
	}
}
