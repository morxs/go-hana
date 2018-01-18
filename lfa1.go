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
	ArgStart  string `cli:"*s" usage:"PO Start Date (SAP format)"`
	ArgEnd    string `cli:"*e" usage:"PO End Date (SAP format)"`
	ArgConfig string `cli:"c" usage:"Custom config file" dft:"config.ini"`
}

const (
	driverName = "hdb"
)

const (
	lfa1SQL = `select
MANDT
, LIFNR, LAND1, NAME1, NAME2, NAME3
, NAME4, ORT01, ORT02, PFACH, PSTL2
, PSTLZ, REGIO, SORTL, STRAS, ADRNR
, MCOD1, MCOD2, MCOD3, ANRED, BAHNS
, BBBNR, BBSNR, BEGRU, BRSCH, BUBKZ
, DATLT, DTAMS, DTAWS, ERDAT, ERNAM
, ESRNR, KONZS, KTOKK, KUNNR, LNRZA
, LOEVM, SPERR, SPERM, SPRAS, STCD1
, STCD2, STKZA, STKZU, TELBX, TELF1
, TELF2, TELFX, TELTX, TELX1, XCPDK
, XZEMP, VBUND, FISKN, STCEG, STKZN
, SPERQ, GBORT, GBDAT, SEXKZ, KRAUS
, REVDB, QSSYS, KTOCK, PFORT, WERKS
, LTSNA, WERKR, PLKAL, DUEFL, TXJCD
, SPERZ, SCACD, SFRGR, LZONE, XLFZA
, DLGRP, FITYP, STCDT, REGSS, ACTSS
, STCD3, STCD4, STCD5, IPISP, TAXBS
, PROFS, STGDL, EMNFR, LFURL, J_1KFREPRE
, J_1KFTBUS, J_1KFTIND, CONFS, UPDAT, UPTIM
, NODEL, QSSYSDAT, PODKZB, FISKU, STENR
, CARRIER_CONF, J_SC_CAPITAL, J_SC_CURRENCY, ALC, PMT_OFFICE
, PSOFG, PSOIS, PSON1, PSON2, PSON3
, PSOVN, PSOTL, PSOHS, PSOST, TRANSPORT_CHAIN
, STAGING_TIME, SCHEDULING_TYPE, SUBMI_RELEVANT, ETHNIC, CATEGORY
from z_wilmar1.lfa1
where mandt = '777'
and lifnr in (
	select 
	distinct lifnr
	from z_wilmar1.ekko
	where bedat between ? and ?
	and bstyp = 'F'
	and (bsart like '%20' or bsart like '%25')
	and loekz = ''
	and bukrs in 
	('BM', 'BO', 'CL', 'DE', 'EB', 'EC', 'EE', 'EL', 'EP', 'ES', 'FB', 'FM', 'GM', 'GU', 'HM', 'JW', 'KI', 'KM', 'NE', 'NO', 'NS', 'NX', 'OE', 'PB', 'PE', 'PO', 'RB', 'RH', 'RM', 'SE', 'SF', 'SG', 'SH', 'SO', 'SU', 'VI', 'WH',
	'AA', 'AD', 'AG', 'AJ', 'AN', 'AP', 'BN', 'BV', 'BW', 'BX', 'BY', 'CA', 'CC', 'CX', 'DA',
	'DB', 'DC', 'DG', 'DI', 'GA', 'GK', 'IA', 'ID', 'IE', 'IF', 'KD', 'KF', 'KG', 'MD', 'MF', 'MH',
	'MJ', 'MO', 'NI', 'PA', 'PF', 'PR', 'PT', 'PV', 'PX', 'RA', 'RJ',
	'SB', 'SJ', 'SN', 'SV', 'SX', 'TB', 'TC', 'TM', 'TN', 'UD', 'UI', 'WJ',
	'BD', 'OU', 'WL', 'GS', 'BZ', 'SZ', 'WR', 'WF', 'BC', 'EY')
)`
)

const (
	File = "lfa1.xls"
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
		iniKeyUsername := iniSection.Key("uid").String()
		iniKeyPassword := iniSection.Key("pwd").String()
		iniKeyHost := iniSection.Key("host").String()
		iniKeyPort := iniSection.Key("port").String()
		hdbDsn := "hdb://" + iniKeyUsername + ":" + iniKeyPassword + "@" + iniKeyHost + ":" + iniKeyPort

		utils.WriteMsg("OPEN HDB")
		db, err := sql.Open(driverName, hdbDsn)
		if err != nil {
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
		rows, err := db.Query(lfa1SQL, argv.ArgStart, argv.ArgEnd)
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
			var mandt, lifnr, land1, name1, name2 string
			var name3, name4, ort01, ort02, pfach string
			var pstl2, pstlz, regio, sortl, stras string
			var adrnr, mcod1, mcod2, mcod3, anred string
			var bahns, bbbnr, bbsnr, begru, brsch string
			var bubkz, datlt, dtams, dtaws, erdat string
			var ernam, esrnr, konzs, ktokk, kunnr string
			var lnrza, loevm, sperr, sperm, spras string
			var stcd1, stcd2, stkza, stkzu, telbx string
			var telf1, telf2, telfx, teltx, telx1 string
			var xcpdk, xzemp, vbund, fiskn, stceg string
			var stkzn, sperq, gbort, gbdat, sexkz string
			var kraus, revdb, qssys, ktock, pfort string
			var werks, ltsna, werkr, plkal, duefl string
			var txjcd, sperz, scacd, sfrgr, lzone string
			var xlfza, dlgrp, fityp, stcdt, regss string
			var actss, stcd3, stcd4, stcd5, ipisp string
			var taxbs, profs, stgdl, emnfr, lfurl string
			var j_1kfrepre, j_1kftbus, j_1kftind, confs, updat string
			var uptim, nodel, qssysdat, podkzb, fisku string
			var stenr, carrier_conf, j_sc_currency, alc, pmt_office string
			var psofg, psois, pson1, pson2, pson3 string
			var psovn, psotl, psohs, psost, transport_chain string
			var scheduling_type, submi_relevant, ethnic, category string
			var j_sc_capital, staging_time []byte

			if err := rows.Scan(&mandt, &lifnr, &land1, &name1, &name2, &name3, &name4, &ort01, &ort02, &pfach, &pstl2, &pstlz, &regio, &sortl, &stras, &adrnr, &mcod1, &mcod2, &mcod3, &anred, &bahns, &bbbnr, &bbsnr, &begru, &brsch, &bubkz, &datlt, &dtams, &dtaws, &erdat, &ernam, &esrnr, &konzs, &ktokk, &kunnr, &lnrza, &loevm, &sperr, &sperm, &spras, &stcd1, &stcd2, &stkza, &stkzu, &telbx, &telf1, &telf2, &telfx, &teltx, &telx1, &xcpdk, &xzemp, &vbund, &fiskn, &stceg, &stkzn, &sperq, &gbort, &gbdat, &sexkz, &kraus, &revdb, &qssys, &ktock, &pfort, &werks, &ltsna, &werkr, &plkal, &duefl, &txjcd, &sperz, &scacd, &sfrgr, &lzone, &xlfza, &dlgrp, &fityp, &stcdt, &regss, &actss, &stcd3, &stcd4, &stcd5, &ipisp, &taxbs, &profs, &stgdl, &emnfr, &lfurl, &j_1kfrepre, &j_1kftbus, &j_1kftind, &confs, &updat, &uptim, &nodel, &qssysdat, &podkzb, &fisku, &stenr, &carrier_conf, &j_sc_capital, &j_sc_currency, &alc, &pmt_office, &psofg, &psois, &pson1, &pson2, &pson3, &psovn, &psotl, &psohs, &psost, &transport_chain, &staging_time, &scheduling_type, &submi_relevant, &ethnic, &category); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			var bi big.Int
			var z float64
			var neg bool
			var i int

			var record []string

			record = append(record, mandt)
			record = append(record, lifnr)
			record = append(record, land1)
			record = append(record, name1)
			record = append(record, name2)
			record = append(record, name3)
			record = append(record, name4)
			record = append(record, ort01)
			record = append(record, ort02)
			record = append(record, pfach)
			record = append(record, pstl2)
			record = append(record, pstlz)
			record = append(record, regio)
			record = append(record, sortl)
			record = append(record, stras)
			record = append(record, adrnr)
			record = append(record, mcod1)
			record = append(record, mcod2)
			record = append(record, mcod3)
			record = append(record, anred)
			record = append(record, bahns)
			record = append(record, bbbnr)
			record = append(record, bbsnr)
			record = append(record, begru)
			record = append(record, brsch)
			record = append(record, bubkz)
			record = append(record, datlt)
			record = append(record, dtams)
			record = append(record, dtaws)
			record = append(record, erdat)
			record = append(record, ernam)
			record = append(record, esrnr)
			record = append(record, konzs)
			record = append(record, ktokk)
			record = append(record, kunnr)
			record = append(record, lnrza)
			record = append(record, loevm)
			record = append(record, sperr)
			record = append(record, sperm)
			record = append(record, spras)
			record = append(record, stcd1)
			record = append(record, stcd2)
			record = append(record, stkza)
			record = append(record, stkzu)
			record = append(record, telbx)
			record = append(record, telf1)
			record = append(record, telf2)
			record = append(record, telfx)
			record = append(record, teltx)
			record = append(record, telx1)
			record = append(record, xcpdk)
			record = append(record, xzemp)
			record = append(record, vbund)
			record = append(record, fiskn)
			record = append(record, stceg)
			record = append(record, stkzn)
			record = append(record, sperq)
			record = append(record, gbort)
			record = append(record, gbdat)
			record = append(record, sexkz)
			record = append(record, kraus)
			record = append(record, revdb)
			record = append(record, qssys)
			record = append(record, ktock)
			record = append(record, pfort)
			record = append(record, werks)
			record = append(record, ltsna)
			record = append(record, werkr)
			record = append(record, plkal)
			record = append(record, duefl)
			record = append(record, txjcd)
			record = append(record, sperz)
			record = append(record, scacd)
			record = append(record, sfrgr)
			record = append(record, lzone)
			record = append(record, xlfza)
			record = append(record, dlgrp)
			record = append(record, fityp)
			record = append(record, stcdt)
			record = append(record, regss)
			record = append(record, actss)
			record = append(record, stcd3)
			record = append(record, stcd4)
			record = append(record, stcd5)
			record = append(record, ipisp)
			record = append(record, taxbs)
			record = append(record, profs)
			record = append(record, stgdl)
			record = append(record, emnfr)
			record = append(record, lfurl)
			record = append(record, j_1kfrepre)
			record = append(record, j_1kftbus)
			record = append(record, j_1kftind)
			record = append(record, confs)
			record = append(record, updat)
			record = append(record, uptim)
			record = append(record, nodel)
			record = append(record, qssysdat)
			record = append(record, podkzb)
			record = append(record, fisku)
			record = append(record, stenr)
			record = append(record, carrier_conf)
			neg, i = utils.DecodeDecimal(j_sc_capital, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, j_sc_currency)
			record = append(record, alc)
			record = append(record, pmt_office)
			record = append(record, psofg)
			record = append(record, psois)
			record = append(record, pson1)
			record = append(record, pson2)
			record = append(record, pson3)
			record = append(record, psovn)
			record = append(record, psotl)
			record = append(record, psohs)
			record = append(record, psost)
			record = append(record, transport_chain)
			neg, i = utils.DecodeDecimal(staging_time, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, scheduling_type)
			record = append(record, submi_relevant)
			record = append(record, ethnic)
			record = append(record, category)
			w.Write(record)
			//fmt.Println(record)
		}
		w.Flush()

		if err := rows.Err(); err != nil {
			utils.WriteMsg("ROWS")
			log.Fatal(err)
		}
		return nil
	})
}
