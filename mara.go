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
	ArgStart        string `cli:"*s" usage:"Start Date (SAP format)"`
	ArgEnd          string `cli:"*e" usage:"End Date (SAP format)"`
	ArgCreatedStart string `cli:"*t" usage:"Created Start Date (SAP format)"`
	ArgCreatedEnd   string `cli:"*d" usage:"Created End Date (SAP format)"`
	ArgConfig       string `cli:"c" usage:"Custom config file" dft:"config.ini"`
}

const (
	driverName = "hdb"
)

const (
	maraSQL = `select 
	MANDT, MATNR, ERSDA, ERNAM, LAEDA, AENAM, VPSTA, PSTAT, LVORM, MTART, MBRSH, 
	MATKL, BISMT, MEINS, BSTME, ZEINR, ZEIAR, ZEIVR, ZEIFO, AESZN, BLATT, BLANZ, 
	FERTH, FORMT, GROES, WRKST, NORMT, LABOR, EKWSL, BRGEW, NTGEW, GEWEI, VOLUM, 
	VOLEH, BEHVO, RAUBE, TEMPB, DISST, TRAGR, STOFF, SPART, KUNNR, EANNR, WESCH, 
	BWVOR, BWSCL, SAISO, ETIAR, ETIFO, ENTAR, EAN11, NUMTP, LAENG, BREIT, HOEHE, 
	MEABM, PRDHA, AEKLK, CADKZ, QMPUR, ERGEW, ERGEI, ERVOL, ERVOE, GEWTO, VOLTO, 
	VABME, KZREV, KZKFG, XCHPF, VHART, FUELG, STFAK, MAGRV, BEGRU, DATAB, LIQDT, 
	SAISJ, PLGTP, MLGUT, EXTWG, SATNR, ATTYP, KZKUP, KZNFM, PMATA, MSTAE, MSTAV, 
	MSTDE, MSTDV, TAKLV, RBNRM, MHDRZ, MHDHB, MHDLP, INHME, INHAL, VPREH, ETIAG, 
	INHBR, CMETH, CUOBF, KZUMW, KOSCH, SPROF, NRFHG, MFRPN, MFRNR, BMATN, MPROF, 
	KZWSM, SAITY, PROFL, IHIVI, ILOOS, SERLV, KZGVH, XGCHP, KZEFF, COMPL, IPRKZ, 
	RDMHD, PRZUS, MTPOS_MARA, BFLME, MATFI, CMREL, BBTYP, SLED_BBD, 
	GTIN_VARIANT, GENNR, RMATP, GDS_RELEVANT, WEORA, HUTYP_DFLT, PILFERABLE, 
	WHSTC, WHMATGR, HNDLCODE, HAZMAT, HUTYP, TARE_VAR, MAXC, MAXC_TOL, MAXL, 
	MAXB, MAXH, MAXDIM_UOM, HERKL, MFRGR, QQTIME, QQTIMEUOM, QGRP, SERIAL, 
	PS_SMARTFORM, LOGUNIT, CWQREL, CWQPROC, CWQTOLGR, ADPROF, IPMIPPRODUCT, 
	ALLOW_PMAT_IGNO, MEDIUM, "/BEV1/LULEINH", "/BEV1/LULDEGRP", "/BEV1/NESTRUCCAT", 
	"/DSD/SL_TOLTYP", "/DSD/SV_CNT_GRP", "/DSD/VC_GROUP", "/VSO/R_TILT_IND", 
	"/VSO/R_STACK_IND", "/VSO/R_BOT_IND", "/VSO/R_TOP_IND", "/VSO/R_STACK_NO", 
	"/VSO/R_PAL_IND", "/VSO/R_PAL_OVR_D", "/VSO/R_PAL_OVR_W", "/VSO/R_PAL_B_HT", 
	"/VSO/R_PAL_MIN_H", "/VSO/R_TOL_B_HT", "/VSO/R_NO_P_GVH", "/VSO/R_QUAN_UNIT", 
	"/VSO/R_KZGVH_IND", PACKCODE, DG_PACK_STATUS, MCOND, RETDELC, LOGLEV_RETO, 
	NSNID, IMATN, PICNUM, BSTAT, COLOR_ATINN, SIZE1_ATINN, SIZE2_ATINN, COLOR, 
	SIZE1, SIZE2, FREE_CHAR, CARE_CODE, BRAND_ID, FIBER_CODE1, FIBER_PART1, 
	FIBER_CODE2, FIBER_PART2, FIBER_CODE3, FIBER_PART3, FIBER_CODE4, 
	FIBER_PART4, FIBER_CODE5, FIBER_PART5, FASHGRD, MENGE1, MEINS1, MENGE2, 
	MEINS2, ZMATTYPE, ZZCERT, ZZBMATNR
	from z_wilmar1.mara
	where mandt = '777'
	and matnr in 
	(
		select 
		distinct a.matnr
		from z_wilmar1.ekpo a
		left join z_wilmar1.ekko b
		on a.mandt = b.mandt
		and a.ebeln = b.ebeln
		left join z_wilmar1.t001 c
		on a.mandt = c.mandt
		and a.bukrs = c.bukrs
		where b.bedat between ? and ?
		and b.bstyp = 'F'
		and (b.bsart like '%20' or b.bsart like '%25')
		and b.loekz = ''
		and a.loekz = ''
		and b.bukrs in 
		('BM', 'BO', 'CL', 'DE', 'EB', 'EC', 'EE', 'EL', 'EP', 'ES', 'FB', 'FM', 'GM', 'GU', 'HM', 'JW', 'KI', 'KM', 'NE', 'NO', 'NS', 'NX', 'OE', 'PB', 'PE', 'PO', 'RB', 'RH', 'RM', 'SE', 'SF', 'SG', 'SH', 'SO', 'SU', 'VI', 'WH',
		'AA', 'AD', 'AG', 'AJ', 'AN', 'AP', 'BN', 'BV', 'BW', 'BX', 'BY', 'CA', 'CC', 'CX', 'DA',
		'DB', 'DC', 'DG', 'DI', 'GA', 'GK', 'IA', 'ID', 'IE', 'IF', 'KD', 'KF', 'KG', 'MD', 'MF', 'MH',
		'MJ', 'MO', 'NI', 'PA', 'PF', 'PR', 'PT', 'PV', 'PX', 'RA', 'RJ',
		'SB', 'SJ', 'SN', 'SV', 'SX', 'TB', 'TC', 'TM', 'TN', 'UD', 'UI', 'WJ',
		'BD', 'OU', 'WL', 'GS', 'BZ', 'SZ', 'WR', 'WF', 'BC', 'EY')
	)
	and ersda between ? and ?`
)

const (
	File = "mara.csv"
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
		rows, err := db.Query(maraSQL, argv.ArgStart, argv.ArgEnd, argv.ArgCreatedStart, argv.ArgCreatedEnd)
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
			var mandt, matnr, ersda, ernam, laeda string
			var aenam, vpsta, pstat, lvorm, mtart string
			var mbrsh, matkl, bismt, meins, bstme string
			var zeinr, zeiar, zeivr, zeifo, aeszn string
			var blatt, blanz, ferth, formt, groes string
			var wrkst, normt, labor, ekwsl, gewei string
			var voleh, behvo, raube, tempb, disst string
			var tragr, stoff, spart, kunnr, eannr string
			var bwvor, bwscl, saiso, etiar, etifo string
			var entar, ean11, numtp, meabm, prdha string
			var aeklk, cadkz, qmpur, ergei, ervoe string
			var vabme, kzrev, kzkfg, xchpf, vhart string
			var magrv, begru, datab, liqdt, saisj string
			var plgtp, mlgut, extwg, satnr, attyp string
			var kzkup, kznfm, pmata, mstae, mstav string
			var mstde, mstdv, taklv, rbnrm, inhme string
			var etiag, cmeth, cuobf, kzumw, kosch string
			var sprof, nrfhg, mfrpn, mfrnr, bmatn string
			var mprof, kzwsm, saity, profl, ihivi string
			var iloos, serlv, kzgvh, xgchp, kzeff string
			var compl, iprkz, rdmhd, przus, mtpos_mara string
			var bflme, matfi, cmrel, bbtyp, sled_bbd string
			var gtin_variant, gennr, rmatp, gds_relevant, weora string
			var hutyp_dflt, pilferable, whstc, whmatgr, hndlcode string
			var hazmat, hutyp, tare_var, maxdim_uom, herkl string
			var mfrgr, qqtimeuom, qgrp, serial, ps_smartform string
			var logunit, cwqrel, cwqproc, cwqtolgr, adprof string
			var ipmipproduct, allow_pmat_igno, medium, bev1luleinh, bev1luldegrp string
			var bev1nestruccat, dsdsl_toltyp, dsdsv_cnt_grp, dsdvc_group, vsor_tilt_ind string
			var vsor_stack_ind, vsor_bot_ind, vsor_top_ind, vsor_stack_no, vsor_pal_ind string
			var vsor_no_p_gvh, vsor_quan_unit, vsor_kzgvh_ind, packcode, dg_pack_status string
			var mcond, retdelc, loglev_reto, nsnid, imatn string
			var picnum, bstat, color_atinn, size1_atinn, size2_atinn string
			var color, size1, size2, free_char, care_code string
			var brand_id, fiber_code1, fiber_part1, fiber_code2, fiber_part2 string
			var fiber_code3, fiber_part3, fiber_code4, fiber_part4, fiber_code5 string
			var fiber_part5, fashgrd, meins1, meins2, zmattype, zzbmatnr string
			var zzcert string
			var stfak int
			var brgew, ntgew, volum, wesch, laeng []byte
			var breit, hoehe, ergew, ervol, gewto []byte
			var volto, fuelg, mhdrz, mhdhb, mhdlp []byte
			var inhal, vpreh, inhbr, maxc, maxc_tol []byte
			var maxl, maxb, maxh, qqtime, vsor_pal_ovr_d []byte
			var vsor_pal_ovr_w, vsor_pal_b_ht, vsor_pal_min_h, vsor_tol_b_ht, menge1 []byte
			var menge2 []byte

			if err := rows.Scan(&mandt, &matnr, &ersda, &ernam, &laeda, &aenam, &vpsta, &pstat, &lvorm, &mtart, &mbrsh, &matkl, &bismt, &meins, &bstme, &zeinr, &zeiar, &zeivr, &zeifo, &aeszn, &blatt, &blanz, &ferth, &formt, &groes, &wrkst, &normt, &labor, &ekwsl, &brgew, &ntgew, &gewei, &volum, &voleh, &behvo, &raube, &tempb, &disst, &tragr, &stoff, &spart, &kunnr, &eannr, &wesch, &bwvor, &bwscl, &saiso, &etiar, &etifo, &entar, &ean11, &numtp, &laeng, &breit, &hoehe, &meabm, &prdha, &aeklk, &cadkz, &qmpur, &ergew, &ergei, &ervol, &ervoe, &gewto, &volto, &vabme, &kzrev, &kzkfg, &xchpf, &vhart, &fuelg, &stfak, &magrv, &begru, &datab, &liqdt, &saisj, &plgtp, &mlgut, &extwg, &satnr, &attyp, &kzkup, &kznfm, &pmata, &mstae, &mstav, &mstde, &mstdv, &taklv, &rbnrm, &mhdrz, &mhdhb, &mhdlp, &inhme, &inhal, &vpreh, &etiag, &inhbr, &cmeth, &cuobf, &kzumw, &kosch, &sprof, &nrfhg, &mfrpn, &mfrnr, &bmatn, &mprof, &kzwsm, &saity, &profl, &ihivi, &iloos, &serlv, &kzgvh, &xgchp, &kzeff, &compl, &iprkz, &rdmhd, &przus, &mtpos_mara, &bflme, &matfi, &cmrel, &bbtyp, &sled_bbd, &gtin_variant, &gennr, &rmatp, &gds_relevant, &weora, &hutyp_dflt, &pilferable, &whstc, &whmatgr, &hndlcode, &hazmat, &hutyp, &tare_var, &maxc, &maxc_tol, &maxl, &maxb, &maxh, &maxdim_uom, &herkl, &mfrgr, &qqtime, &qqtimeuom, &qgrp, &serial, &ps_smartform, &logunit, &cwqrel, &cwqproc, &cwqtolgr, &adprof, &ipmipproduct, &allow_pmat_igno, &medium, &bev1luleinh, &bev1luldegrp, &bev1nestruccat, &dsdsl_toltyp, &dsdsv_cnt_grp, &dsdvc_group, &vsor_tilt_ind, &vsor_stack_ind, &vsor_bot_ind, &vsor_top_ind, &vsor_stack_no, &vsor_pal_ind, &vsor_pal_ovr_d, &vsor_pal_ovr_w, &vsor_pal_b_ht, &vsor_pal_min_h, &vsor_tol_b_ht, &vsor_no_p_gvh, &vsor_quan_unit, &vsor_kzgvh_ind, &packcode, &dg_pack_status, &mcond, &retdelc, &loglev_reto, &nsnid, &imatn, &picnum, &bstat, &color_atinn, &size1_atinn, &size2_atinn, &color, &size1, &size2, &free_char, &care_code, &brand_id, &fiber_code1, &fiber_part1, &fiber_code2, &fiber_part2, &fiber_code3, &fiber_part3, &fiber_code4, &fiber_part4, &fiber_code5, &fiber_part5, &fashgrd, &menge1, &meins1, &menge2, &meins2, &zmattype, &zzcert, &zzbmatnr); err != nil {
				utils.WriteMsg("SCAN")
				log.Fatal(err)
			}

			var bi big.Int
			var z float64
			var neg bool
			var i int

			var record []string

			record = append(record, mandt)
			record = append(record, matnr)
			record = append(record, ersda)
			record = append(record, ernam)
			record = append(record, laeda)
			record = append(record, aenam)
			record = append(record, vpsta)
			record = append(record, pstat)
			record = append(record, lvorm)
			record = append(record, mtart)
			record = append(record, mbrsh)
			record = append(record, matkl)
			record = append(record, bismt)
			record = append(record, meins)
			record = append(record, bstme)
			record = append(record, zeinr)
			record = append(record, zeiar)
			record = append(record, zeivr)
			record = append(record, zeifo)
			record = append(record, aeszn)
			record = append(record, blatt)
			record = append(record, blanz)
			record = append(record, ferth)
			record = append(record, formt)
			record = append(record, groes)
			record = append(record, wrkst)
			record = append(record, normt)
			record = append(record, labor)
			record = append(record, ekwsl)
			neg, i = utils.DecodeDecimal(brgew, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(ntgew, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, gewei)
			neg, i = utils.DecodeDecimal(volum, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, voleh)
			record = append(record, behvo)
			record = append(record, raube)
			record = append(record, tempb)
			record = append(record, disst)
			record = append(record, tragr)
			record = append(record, stoff)
			record = append(record, spart)
			record = append(record, kunnr)
			record = append(record, eannr)
			neg, i = utils.DecodeDecimal(wesch, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, bwvor)
			record = append(record, bwscl)
			record = append(record, saiso)
			record = append(record, etiar)
			record = append(record, etifo)
			record = append(record, entar)
			record = append(record, ean11)
			record = append(record, numtp)
			neg, i = utils.DecodeDecimal(laeng, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(breit, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(hoehe, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, meabm)
			record = append(record, prdha)
			record = append(record, aeklk)
			record = append(record, cadkz)
			record = append(record, qmpur)
			neg, i = utils.DecodeDecimal(ergew, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, ergei)
			neg, i = utils.DecodeDecimal(ervol, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, ervoe)
			neg, i = utils.DecodeDecimal(gewto, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(volto, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, vabme)
			record = append(record, kzrev)
			record = append(record, kzkfg)
			record = append(record, xchpf)
			record = append(record, vhart)
			neg, i = utils.DecodeDecimal(fuelg, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, magrv)
			record = append(record, begru)
			record = append(record, datab)
			record = append(record, liqdt)
			record = append(record, saisj)
			record = append(record, plgtp)
			record = append(record, mlgut)
			record = append(record, extwg)
			record = append(record, satnr)
			record = append(record, attyp)
			record = append(record, kzkup)
			record = append(record, kznfm)
			record = append(record, pmata)
			record = append(record, mstae)
			record = append(record, mstav)
			record = append(record, mstde)
			record = append(record, mstdv)
			record = append(record, taklv)
			record = append(record, rbnrm)
			neg, i = utils.DecodeDecimal(mhdrz, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(mhdhb, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(mhdlp, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, inhme)
			neg, i = utils.DecodeDecimal(inhal, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(vpreh, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, etiag)
			neg, i = utils.DecodeDecimal(inhbr, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, cmeth)
			record = append(record, cuobf)
			record = append(record, kzumw)
			record = append(record, kosch)
			record = append(record, sprof)
			record = append(record, nrfhg)
			record = append(record, mfrpn)
			record = append(record, mfrnr)
			record = append(record, bmatn)
			record = append(record, mprof)
			record = append(record, kzwsm)
			record = append(record, saity)
			record = append(record, profl)
			record = append(record, ihivi)
			record = append(record, iloos)
			record = append(record, serlv)
			record = append(record, kzgvh)
			record = append(record, xgchp)
			record = append(record, kzeff)
			record = append(record, compl)
			record = append(record, iprkz)
			record = append(record, rdmhd)
			record = append(record, przus)
			record = append(record, mtpos_mara)
			record = append(record, bflme)
			record = append(record, matfi)
			record = append(record, cmrel)
			record = append(record, bbtyp)
			record = append(record, sled_bbd)
			record = append(record, gtin_variant)
			record = append(record, gennr)
			record = append(record, rmatp)
			record = append(record, gds_relevant)
			record = append(record, weora)
			record = append(record, hutyp_dflt)
			record = append(record, pilferable)
			record = append(record, whstc)
			record = append(record, whmatgr)
			record = append(record, hndlcode)
			record = append(record, hazmat)
			record = append(record, hutyp)
			record = append(record, tare_var)
			neg, i = utils.DecodeDecimal(maxc, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(maxc_tol, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(maxl, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(maxb, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(maxh, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, maxdim_uom)
			record = append(record, herkl)
			record = append(record, mfrgr)
			neg, i = utils.DecodeDecimal(qqtime, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, qqtimeuom)
			record = append(record, qgrp)
			record = append(record, serial)
			record = append(record, ps_smartform)
			record = append(record, logunit)
			record = append(record, cwqrel)
			record = append(record, cwqproc)
			record = append(record, cwqtolgr)
			record = append(record, adprof)
			record = append(record, ipmipproduct)
			record = append(record, allow_pmat_igno)
			record = append(record, medium)
			record = append(record, bev1luleinh)
			record = append(record, bev1luldegrp)
			record = append(record, bev1nestruccat)
			record = append(record, dsdsl_toltyp)
			record = append(record, dsdsv_cnt_grp)
			record = append(record, dsdvc_group)
			record = append(record, vsor_tilt_ind)
			record = append(record, vsor_stack_ind)
			record = append(record, vsor_bot_ind)
			record = append(record, vsor_top_ind)
			record = append(record, vsor_stack_no)
			record = append(record, vsor_pal_ind)
			neg, i = utils.DecodeDecimal(vsor_pal_ovr_d, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(vsor_pal_ovr_w, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(vsor_pal_b_ht, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(vsor_pal_min_h, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			neg, i = utils.DecodeDecimal(vsor_tol_b_ht, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, vsor_no_p_gvh)
			record = append(record, vsor_quan_unit)
			record = append(record, vsor_kzgvh_ind)
			record = append(record, packcode)
			record = append(record, dg_pack_status)
			record = append(record, mcond)
			record = append(record, retdelc)
			record = append(record, loglev_reto)
			record = append(record, nsnid)
			record = append(record, imatn)
			record = append(record, picnum)
			record = append(record, bstat)
			record = append(record, color_atinn)
			record = append(record, size1_atinn)
			record = append(record, size2_atinn)
			record = append(record, color)
			record = append(record, size1)
			record = append(record, size2)
			record = append(record, free_char)
			record = append(record, care_code)
			record = append(record, brand_id)
			record = append(record, fiber_code1)
			record = append(record, fiber_part1)
			record = append(record, fiber_code2)
			record = append(record, fiber_part2)
			record = append(record, fiber_code3)
			record = append(record, fiber_part3)
			record = append(record, fiber_code4)
			record = append(record, fiber_part4)
			record = append(record, fiber_code5)
			record = append(record, fiber_part5)
			record = append(record, fashgrd)
			neg, i = utils.DecodeDecimal(menge1, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, meins1)
			neg, i = utils.DecodeDecimal(menge2, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, meins2)
			record = append(record, zmattype)
			record = append(record, zzcert)
			record = append(record, zzbmatnr)
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
