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
	ekkoSQL = `select
MANDT
, EBELN
, BUKRS
, BSTYP
, BSART
, BSAKZ
, LOEKZ
, STATU
, AEDAT
, ERNAM
, PINCR
, LPONR
, LIFNR
, SPRAS
, ZTERM
, ZBD1T
, ZBD2T
, ZBD3T
, ZBD1P
, ZBD2P
, EKORG
, EKGRP
, WAERS
, WKURS
, KUFIX
, BEDAT
, KDATB
, KDATE
, BWBDT
, ANGDT
, BNDDT
, GWLDT
, AUSNR
, ANGNR
, IHRAN
, IHREZ
, VERKF
, TELF1
, LLIEF
, KUNNR
, KONNR
, ABGRU
, AUTLF
, WEAKT
, RESWK
, LBLIF
, INCO1
, INCO2
, KTWRT
, SUBMI
, KNUMV
, KALSM
, STAFO
, LIFRE
, EXNUM
, UNSEZ
, LOGSY
, UPINC
, STAKO
, FRGGR
, FRGSX
, FRGKE
, FRGZU
, FRGRL
, LANDS
, LPHIS
, ADRNR
, STCEG_L
, STCEG
, ABSGR
, ADDNR
, KORNR
, MEMORY
, PROCSTAT
, RLWRT
, REVNO
, SCMPROC
, REASON_CODE
, MEMORYTYPE
, RETTP
, RETPC
, DPTYP
, DPPCT
, DPAMT
, DPDAT
, MSR_ID
, HIERARCHY_EXISTS
, THRESHOLD_EXISTS
, LEGAL_CONTRACT
, DESCRIPTION
, RELEASE_DATE
, FORCE_ID
, FORCE_CNT
, RELOC_ID
, RELOC_SEQ_ID
, ZZVESSEL
, ZZTRANS_VSL
, ZZTRUCK_NO
, ZZDO_SO
, ZZMTART
, ZZREF
, ZZTYPE
, ZZERZET
, ZZVOYAGE
, ZZBEZET
, ZZMPOB
, ZZFINAL
, ZZSAILDATE
, ZZQTYOFCONT
, ZZBROKER
, ZZTYPEKONT
, ZZCONT
, ZZVAT_TERM
, ZZUPD_PLAN
, ZZCPNO
, ZZCPITEM
, ZZNOPRINT_TERM
, ZZDP
, ZZORIGIN
, ZZIMPTXT
, ZZSEALNO
, ZZCONNM
, ZZPIBNO
, ZZXYEAR
, ZZPIBPORT
, ZZKPPBC_P
, ZZMERGE_PRC
, ZZTANKERNO
, ZZBILL_ADRCO
, ZZFRGTXT
, ZZISCC
, ZZWBREF
, ZZTMP_CTR_NO
, ZZDEM_COST
, ZZDEM_COST_CCY
, ZZDEM_TAX
, ZZDEM_TAX_CCY
, ZZORDRCV
, ZZPOWOPR
, ZZEXTO
, ZZCARNO
, ZZFBANO
, ZZCHKAFCE
, ZZOPFEE
, ZZPRPEZ
, ZZNPWP
, ZZITEM
, ZZAUFNR
, ZZPNLTYTXT
, ZZBLNO
, ZZDATE
, ZZPORT
, ZZYEAR
, POHF_TYPE
, EQ_EINDT
, EQ_WERKS
, FIXPO
, EKGRP_ALLOW
, WERKS_ALLOW
, CONTRACT_ALLOW
, PSTYP_ALLOW
, FIXPO_ALLOW
, KEY_ID_ALLOW
, AUREL_ALLOW
, DELPER_ALLOW
, EINDT_ALLOW
, OTB_LEVEL
, OTB_COND_TYPE
, KEY_ID
, OTB_VALUE
, OTB_CURR
, OTB_RES_VALUE
, OTB_SPEC_VALUE
, SPR_RSN_PROFILE
, BUDG_TYPE
, OTB_STATUS
, OTB_REASON
, CHECK_TYPE
, CON_OTB_REQ
, CON_PREBOOK_LEV
, CON_DISTR_LEV
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
`
)

const (
	File = "ekko.xls"
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
			fmt.Println(err)
		}
		defer file.Close()

		// try to query
		utils.WriteMsg("QUERY")
		rows, err := db.Query(ekkoSQL, argv.ArgStart, argv.ArgEnd)
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
			var mandt, ebeln, bukrs, bstyp, bsart string
			var bsakz, loekz, statu, aedat, ernam string
			var pincr, lponr, lifnr, spras, zterm string
			var ekorg, ekgrp, waers, kufix, bedat string
			var kdatb, kdate, bwbdt, angdt, bnddt string
			var gwldt, ausnr, angnr, ihran, ihrez string
			var verkf, telf1, llief, kunnr, konnr string
			var abgru, autlf, weakt, reswk, lblif string
			var inco1, inco2, submi, knumv, kalsm string
			var stafo, lifre, exnum, unsez, logsy string
			var upinc, stako, frggr, frgsx, frgke string
			var frgzu, frgrl, lands, lphis, adrnr string
			var stceg_l, stceg, absgr, addnr, kornr string
			var memory, procstat, revno, scmproc, reason_code string
			var memorytype, rettp, dptyp, dpdat, msr_id string
			var hierarchy_exists, threshold_exists, legal_contract, description, release_date string
			var force_id, force_cnt, reloc_id, reloc_seq_id, zzvessel string
			var zztrans_vsl, zztruck_no, zzdo_so, zzmtart, zzref string
			var zztype, zzerzet, zzvoyage, zzbezet, zzmpob string
			var zzfinal, zzsaildate, zzbroker, zztypekont, zzcont string
			var zzvat_term, zzupd_plan, zzcpno, zzcpitem, zznoprint_term string
			var zzdp, zzorigin, zzimptxt, zzsealno, zzconnm string
			var zzpibno, zzxyear, zzpibport, zzkppbc_p, zzmerge_prc string
			var zztankerno, zzbill_adrco, zzfrgtxt, zziscc, zzwbref string
			var zztmp_ctr_no, zzdem_cost_ccy, zzdem_tax_ccy, zzordrcv, zzpowopr string
			var zzcarno, zzfbano, zzchkafce, zzopfee, zzprpez string
			var zznpwp, zzitem, zzaufnr, zzpnltytxt, zzblno string
			var zzdate, zzport, zzyear, pohf_type, eq_eindt string
			var eq_werks, fixpo, ekgrp_allow, werks_allow, contract_allow string
			var pstyp_allow, fixpo_allow, key_id_allow, aurel_allow, delper_allow string
			var eindt_allow, otb_level, otb_cond_type, key_id, otb_curr string
			var spr_rsn_profile, budg_type, otb_status, otb_reason, check_type string
			var con_otb_req, con_prebook_lev, con_distr_lev string
			var zbd1t, zbd2t, zbd3t, zbd1p, zbd2p []byte
			var wkurs, ktwrt, rlwrt, retpc, dppct []byte
			var dpamt, zzqtyofcont, zzdem_cost, zzdem_tax, zzexto []byte
			var otb_value, otb_res_value, otb_spec_value []byte

			if err := rows.Scan(&mandt, &ebeln, &bukrs, &bstyp, &bsart, &bsakz, &loekz, &statu, &aedat, &ernam, &pincr, &lponr, &lifnr, &spras, &zterm, &zbd1t, &zbd2t, &zbd3t, &zbd1p, &zbd2p, &ekorg, &ekgrp, &waers, &wkurs, &kufix, &bedat, &kdatb, &kdate, &bwbdt, &angdt, &bnddt, &gwldt, &ausnr, &angnr, &ihran, &ihrez, &verkf, &telf1, &llief, &kunnr, &konnr, &abgru, &autlf, &weakt, &reswk, &lblif, &inco1, &inco2, &ktwrt, &submi, &knumv, &kalsm, &stafo, &lifre, &exnum, &unsez, &logsy, &upinc, &stako, &frggr, &frgsx, &frgke, &frgzu, &frgrl, &lands, &lphis, &adrnr, &stceg_l, &stceg, &absgr, &addnr, &kornr, &memory, &procstat, &rlwrt, &revno, &scmproc, &reason_code, &memorytype, &rettp, &retpc, &dptyp, &dppct, &dpamt, &dpdat, &msr_id, &hierarchy_exists, &threshold_exists, &legal_contract, &description, &release_date, &force_id, &force_cnt, &reloc_id, &reloc_seq_id, &zzvessel, &zztrans_vsl, &zztruck_no, &zzdo_so, &zzmtart, &zzref, &zztype, &zzerzet, &zzvoyage, &zzbezet, &zzmpob, &zzfinal, &zzsaildate, &zzqtyofcont, &zzbroker, &zztypekont, &zzcont, &zzvat_term, &zzupd_plan, &zzcpno, &zzcpitem, &zznoprint_term, &zzdp, &zzorigin, &zzimptxt, &zzsealno, &zzconnm, &zzpibno, &zzxyear, &zzpibport, &zzkppbc_p, &zzmerge_prc, &zztankerno, &zzbill_adrco, &zzfrgtxt, &zziscc, &zzwbref, &zztmp_ctr_no, &zzdem_cost, &zzdem_cost_ccy, &zzdem_tax, &zzdem_tax_ccy, &zzordrcv, &zzpowopr, &zzexto, &zzcarno, &zzfbano, &zzchkafce, &zzopfee, &zzprpez, &zznpwp, &zzitem, &zzaufnr, &zzpnltytxt, &zzblno, &zzdate, &zzport, &zzyear, &pohf_type, &eq_eindt, &eq_werks, &fixpo, &ekgrp_allow, &werks_allow, &contract_allow, &pstyp_allow, &fixpo_allow, &key_id_allow, &aurel_allow, &delper_allow, &eindt_allow, &otb_level, &otb_cond_type, &key_id, &otb_value, &otb_curr, &otb_res_value, &otb_spec_value, &spr_rsn_profile, &budg_type, &otb_status, &otb_reason, &check_type, &con_otb_req, &con_prebook_lev, &con_distr_lev); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			var bi big.Int
			var z float64
			var neg bool
			var i int

			var record []string
			record = append(record, mandt)
			record = append(record, ebeln)
			record = append(record, bukrs)
			record = append(record, bstyp)
			record = append(record, bsart)
			record = append(record, bsakz)
			record = append(record, loekz)
			record = append(record, statu)
			record = append(record, aedat)
			record = append(record, ernam)
			record = append(record, pincr)
			record = append(record, lponr)
			record = append(record, lifnr)
			record = append(record, spras)
			record = append(record, zterm)

			neg, i = utils.DecodeDecimal(zbd1t, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			// record = append(record, utils.ConvertByteToStr(zbd1t))

			neg, i = utils.DecodeDecimal(zbd2t, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(zbd3t, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(zbd1p, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(zbd2p, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, ekorg)
			record = append(record, ekgrp)
			record = append(record, waers)

			neg, i = utils.DecodeDecimal(wkurs, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, kufix)
			record = append(record, bedat)
			record = append(record, kdatb)
			record = append(record, kdate)
			record = append(record, bwbdt)
			record = append(record, angdt)
			record = append(record, bnddt)
			record = append(record, gwldt)
			record = append(record, ausnr)
			record = append(record, angnr)
			record = append(record, ihran)
			record = append(record, ihrez)
			record = append(record, verkf)
			record = append(record, telf1)
			record = append(record, llief)
			record = append(record, kunnr)
			record = append(record, konnr)
			record = append(record, abgru)
			record = append(record, autlf)
			record = append(record, weakt)
			record = append(record, reswk)
			record = append(record, lblif)
			record = append(record, inco1)
			record = append(record, inco2)

			neg, i = utils.DecodeDecimal(ktwrt, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, submi)
			record = append(record, knumv)
			record = append(record, kalsm)
			record = append(record, stafo)
			record = append(record, lifre)
			record = append(record, exnum)
			record = append(record, unsez)
			record = append(record, logsy)
			record = append(record, upinc)
			record = append(record, stako)
			record = append(record, frggr)
			record = append(record, frgsx)
			record = append(record, frgke)
			record = append(record, frgzu)
			record = append(record, frgrl)
			record = append(record, lands)
			record = append(record, lphis)
			record = append(record, adrnr)
			record = append(record, stceg_l)
			record = append(record, stceg)
			record = append(record, absgr)
			record = append(record, addnr)
			record = append(record, kornr)
			record = append(record, memory)
			record = append(record, procstat)

			neg, i = utils.DecodeDecimal(rlwrt, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, revno)
			record = append(record, scmproc)
			record = append(record, reason_code)
			record = append(record, memorytype)
			record = append(record, rettp)

			neg, i = utils.DecodeDecimal(retpc, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, dptyp)

			neg, i = utils.DecodeDecimal(dppct, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(dpamt, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, dpdat)
			record = append(record, msr_id)
			record = append(record, hierarchy_exists)
			record = append(record, threshold_exists)
			record = append(record, legal_contract)
			record = append(record, description)
			record = append(record, release_date)
			record = append(record, force_id)
			record = append(record, force_cnt)
			record = append(record, reloc_id)
			record = append(record, reloc_seq_id)
			record = append(record, zzvessel)
			record = append(record, zztrans_vsl)
			record = append(record, zztruck_no)
			record = append(record, zzdo_so)
			record = append(record, zzmtart)
			record = append(record, zzref)
			record = append(record, zztype)
			record = append(record, zzerzet)
			record = append(record, zzvoyage)
			record = append(record, zzbezet)
			record = append(record, zzmpob)
			record = append(record, zzfinal)
			record = append(record, zzsaildate)

			neg, i = utils.DecodeDecimal(zzqtyofcont, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, zzbroker)
			record = append(record, zztypekont)
			record = append(record, zzcont)
			record = append(record, zzvat_term)
			record = append(record, zzupd_plan)
			record = append(record, zzcpno)
			record = append(record, zzcpitem)
			record = append(record, zznoprint_term)
			record = append(record, zzdp)
			record = append(record, zzorigin)
			record = append(record, zzimptxt)
			record = append(record, zzsealno)
			record = append(record, zzconnm)
			record = append(record, zzpibno)
			record = append(record, zzxyear)
			record = append(record, zzpibport)
			record = append(record, zzkppbc_p)
			record = append(record, zzmerge_prc)
			record = append(record, zztankerno)
			record = append(record, zzbill_adrco)
			record = append(record, zzfrgtxt)
			record = append(record, zziscc)
			record = append(record, zzwbref)
			record = append(record, zztmp_ctr_no)

			neg, i = utils.DecodeDecimal(zzdem_cost, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, zzdem_cost_ccy)

			neg, i = utils.DecodeDecimal(zzdem_tax, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, zzdem_tax_ccy)
			record = append(record, zzordrcv)
			record = append(record, zzpowopr)

			neg, i = utils.DecodeDecimal(zzexto, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, zzcarno)
			record = append(record, zzfbano)
			record = append(record, zzchkafce)
			record = append(record, zzopfee)
			record = append(record, zzprpez)
			record = append(record, zznpwp)
			record = append(record, zzitem)
			record = append(record, zzaufnr)
			record = append(record, zzpnltytxt)
			record = append(record, zzblno)
			record = append(record, zzdate)
			record = append(record, zzport)
			record = append(record, zzyear)
			record = append(record, pohf_type)
			record = append(record, eq_eindt)
			record = append(record, eq_werks)
			record = append(record, fixpo)
			record = append(record, ekgrp_allow)
			record = append(record, werks_allow)
			record = append(record, contract_allow)
			record = append(record, pstyp_allow)
			record = append(record, fixpo_allow)
			record = append(record, key_id_allow)
			record = append(record, aurel_allow)
			record = append(record, delper_allow)
			record = append(record, eindt_allow)
			record = append(record, otb_level)
			record = append(record, otb_cond_type)
			record = append(record, key_id)

			neg, i = utils.DecodeDecimal(otb_value, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, otb_curr)

			neg, i = utils.DecodeDecimal(otb_res_value, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(otb_spec_value, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, spr_rsn_profile)
			record = append(record, budg_type)
			record = append(record, otb_status)
			record = append(record, otb_reason)
			record = append(record, check_type)
			record = append(record, con_otb_req)
			record = append(record, con_prebook_lev)
			record = append(record, con_distr_lev)

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
