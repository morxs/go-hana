# Go-HANA

Is a project for Golang POC to retrieve data from HANA DB using SAP HANA DB sql driver. This project has highly unoptimized go code.

## Plugins

- github.com/SAP/go-hdb/driver
- github.com/mkideal/cli
- github.com/go-ini/ini

## Config

Please change `config.ini.sample` to `config.ini` for below program works with exception of `gen_code_ddf.go`.

Only `gen_code_ddf.go` have different configuration to generate the code for easier development. See for `ddf.csv.sample` for sample configuration structure.

## Usage of gen_code_ddf.go

The source code is only print out to terminal. However, you can easily to use `>` to save it into file (ie `go run gen_code_ddf.go > output.txt`