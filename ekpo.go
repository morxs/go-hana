package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"

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
	ekpoSQL = `select
a.*, b.bukrs, c.land1
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
'BD', 'OU', 'WL', 'GS', 'BZ', 'SZ', 'WR', 'WF', 'BC', 'EY')`
)

const (
	File = "ekpo.csv"
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
		rows, err := db.Query(ekpoSQL, argv.ArgStart, argv.ArgEnd)
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
			var mandt, ebeln, ebelp, loekz, statu, aedat, txz01, matnr, ematn, bukrs, werks, lgort, bednr, matkl, infnr string
			var idnlf, meins, bprme, agdat, mwskz, bonus, insmk, spinf, prsdr, schpr, uebtk, bwtar, bwtty, abskz, agmem string
			var elikz, erekz, pstyp, knttp, kzvbr, vrtkz, twrkz, wepos, weunb, repos, webre, kzabs, labnr, konnr, ktpnr string
			var abdat, kzstu, notkz, lmein, evers, prdat, bstyp, xoblr, kunnr, adrnr, ekkol, sktof, stafo, gewei, txjcd string
			var etdrk, sobkz, arsnr, arsps, insnc, ssqss, zgtyp, ean11, bstae, revlv, geber, fistl, fipos, ko_gsber string
			var ko_pargb, ko_prctr, ko_pprctr, meprf, voleh, inco1, inco2, vorab, kolif, ltsnr, packno, fplnr, stapo string
			var uebpo, lewed, emlif, lblkz, satnr, attyp, kanba, adrn2, cuobj, xersy, eildt, drdat, druhr, drunr, aktnr string
			var abeln, abelp, punei, saiso, saisj, ebon2, ebon3, ebonf, mlmaa, anfnr, anfps, kzkfg, usequ, umsok, banfn string
			var bnfpo, mtart, uptyp, upvor, sikgr, retpo, aurel, bsgru, lfret, mfrgr, nrfhg, j_1bnbm, j_1bmatuse string
			var j_1bmatorg, j_1bownpro, j_1bindust, abueb, nlabd, nfabd, kzbws, fabkz, j_1aindxp, j_1aidatep, mprof string
			var eglkz, kztlf, kzfme, rdprf, techs, chg_srv, chg_fplnr, mfrpn, mfrnr, emnfr, novet, afnam, tzonrc, iprkz string
			var lebre, berid, xconditions, apoms, ccomp, grant_nbr, fkber, status, reslo, kblnr, kblpos, weora string
			var srv_bas_com, prio_urg, prio_req, empst, diff_invoice, trmrisk_relevant, spe_abgru, spe_crm_so string
			var spe_crm_so_item, spe_crm_ref_so, spe_crm_ref_item, spe_crm_fkrel, spe_chng_sys, spe_insmk_src string
			var spe_cq_ctrltype, spe_cq_nocq, reason_code, spe_ewm_dtc, exlin, exsnr, ehtyp, dptyp, dpdat, fls_rsto string
			var ext_rfx_number, ext_rfx_item, ext_rfx_system, srm_contract_id, srm_contract_itm, blk_reason_id string
			var blk_reason_txt, itcons, fixmg, bev1negen_item, bev1nedepfree, bev1nestruccat, advcode, budget_pd string
			var excpe, iuid_relevant, mrpind, zztrans_type, zztransp_type, zzloadport, zzdestport, zzdischarge string
			var zztrans_port, zzfrchl, zzupdate, zzdo_so, zzuom_gr, zzanln1, zzanln2, zzinventory, zzestate, zzmatnr string
			var zzdono, zzdodate, zzctr_num, zzctr_dat, zzccpj, zzafce, zzxcont, zzcpno_long, zzcpit, zitgrp, zzstono string
			var zzstoitem, zzqtyhl, zzstor_no, zzstor_it, zzkb, zzrefhno, zzpino, zzpono, zzvbeln_v1, zzposnr_v1 string
			var zz_qm_opr_rm, zz_qm_opr_fp, zzposnr, zzrsnum, zzrspos, zzbudgetcode, refsite, ref_item, source_id string
			var source_key, put_back, pol_id, cons_order string
			var anzsn int
			var ktmng, menge, bpumz, bpumn, umrez, umren, netpr, peinh, netwr, brtwr, webaz, mahnz, mahn1, mahn2, mahn3, uebto, untto, abftz, etfz1, etfz2, zwert, navnw, abmng, effwr, plifz, ntgew, brgew, volum, gnetwr, anzpu, mhdrz, kzwi1, kzwi2, kzwi3, kzwi4, kzwi5, kzwi6, mfzhi, ffzhi, bonba, cqu_sar, retpc, dppct, dpamt, zzdoqty []byte
			var bukrs1, land1 string

			if err := rows.Scan(&mandt, &ebeln, &ebelp, &loekz, &statu, &aedat, &txz01, &matnr, &ematn, &bukrs, &werks, &lgort, &bednr, &matkl, &infnr, &idnlf, &ktmng, &menge, &meins, &bprme, &bpumz, &bpumn, &umrez, &umren, &netpr, &peinh, &netwr, &brtwr, &agdat, &webaz, &mwskz, &bonus, &insmk, &spinf, &prsdr, &schpr, &mahnz, &mahn1, &mahn2, &mahn3, &uebto, &uebtk, &untto, &bwtar, &bwtty, &abskz, &agmem, &elikz, &erekz, &pstyp, &knttp, &kzvbr, &vrtkz, &twrkz, &wepos, &weunb, &repos, &webre, &kzabs, &labnr, &konnr, &ktpnr, &abdat, &abftz, &etfz1, &etfz2, &kzstu, &notkz, &lmein, &evers, &zwert, &navnw, &abmng, &prdat, &bstyp, &effwr, &xoblr, &kunnr, &adrnr, &ekkol, &sktof, &stafo, &plifz, &ntgew, &gewei, &txjcd, &etdrk, &sobkz, &arsnr, &arsps, &insnc, &ssqss, &zgtyp, &ean11, &bstae, &revlv, &geber, &fistl, &fipos, &ko_gsber, &ko_pargb, &ko_prctr, &ko_pprctr, &meprf, &brgew, &volum, &voleh, &inco1, &inco2, &vorab, &kolif, &ltsnr, &packno, &fplnr, &gnetwr, &stapo, &uebpo, &lewed, &emlif, &lblkz, &satnr, &attyp, &kanba, &adrn2, &cuobj, &xersy, &eildt, &drdat, &druhr, &drunr, &aktnr, &abeln, &abelp, &anzpu, &punei, &saiso, &saisj, &ebon2, &ebon3, &ebonf, &mlmaa, &mhdrz, &anfnr, &anfps, &kzkfg, &usequ, &umsok, &banfn, &bnfpo, &mtart, &uptyp, &upvor, &kzwi1, &kzwi2, &kzwi3, &kzwi4, &kzwi5, &kzwi6, &sikgr, &mfzhi, &ffzhi, &retpo, &aurel, &bsgru, &lfret, &mfrgr, &nrfhg, &j_1bnbm, &j_1bmatuse, &j_1bmatorg, &j_1bownpro, &j_1bindust, &abueb, &nlabd, &nfabd, &kzbws, &bonba, &fabkz, &j_1aindxp, &j_1aidatep, &mprof, &eglkz, &kztlf, &kzfme, &rdprf, &techs, &chg_srv, &chg_fplnr, &mfrpn, &mfrnr, &emnfr, &novet, &afnam, &tzonrc, &iprkz, &lebre, &berid, &xconditions, &apoms, &ccomp, &grant_nbr, &fkber, &status, &reslo, &kblnr, &kblpos, &weora, &srv_bas_com, &prio_urg, &prio_req, &empst, &diff_invoice, &trmrisk_relevant, &spe_abgru, &spe_crm_so, &spe_crm_so_item, &spe_crm_ref_so, &spe_crm_ref_item, &spe_crm_fkrel, &spe_chng_sys, &spe_insmk_src, &spe_cq_ctrltype, &spe_cq_nocq, &reason_code, &cqu_sar, &anzsn, &spe_ewm_dtc, &exlin, &exsnr, &ehtyp, &retpc, &dptyp, &dppct, &dpamt, &dpdat, &fls_rsto, &ext_rfx_number, &ext_rfx_item, &ext_rfx_system, &srm_contract_id, &srm_contract_itm, &blk_reason_id, &blk_reason_txt, &itcons, &fixmg, &bev1negen_item, &bev1nedepfree, &bev1nestruccat, &advcode, &budget_pd, &excpe, &iuid_relevant, &mrpind, &zztrans_type, &zztransp_type, &zzloadport, &zzdestport, &zzdischarge, &zztrans_port, &zzfrchl, &zzupdate, &zzdo_so, &zzuom_gr, &zzanln1, &zzanln2, &zzinventory, &zzestate, &zzmatnr, &zzdono, &zzdodate, &zzdoqty, &zzctr_num, &zzctr_dat, &zzccpj, &zzafce, &zzxcont, &zzcpno_long, &zzcpit, &zitgrp, &zzstono, &zzstoitem, &zzqtyhl, &zzstor_no, &zzstor_it, &zzkb, &zzrefhno, &zzpino, &zzpono, &zzvbeln_v1, &zzposnr_v1, &zz_qm_opr_rm, &zz_qm_opr_fp, &zzposnr, &zzrsnum, &zzrspos, &zzbudgetcode, &refsite, &ref_item, &source_id, &source_key, &put_back, &pol_id, &cons_order, &bukrs, &land1); err != nil {
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
			record = append(record, ebelp)
			record = append(record, loekz)
			record = append(record, statu)
			record = append(record, aedat)
			record = append(record, txz01)
			record = append(record, matnr)
			record = append(record, ematn)
			record = append(record, bukrs)
			record = append(record, werks)
			record = append(record, lgort)
			record = append(record, bednr)
			record = append(record, matkl)
			record = append(record, infnr)
			record = append(record, idnlf)

			neg, i = utils.DecodeDecimal(ktmng, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(menge, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, meins)
			record = append(record, bprme)

			neg, i = utils.DecodeDecimal(bpumz, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(bpumn, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(umrez, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(umren, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(netpr, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(peinh, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(netwr, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(brtwr, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, agdat)

			neg, i = utils.DecodeDecimal(webaz, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, mwskz)
			record = append(record, bonus)
			record = append(record, insmk)
			record = append(record, spinf)
			record = append(record, prsdr)
			record = append(record, schpr)

			neg, i = utils.DecodeDecimal(mahnz, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(mahn1, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(mahn2, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(mahn3, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(uebto, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, uebtk)

			neg, i = utils.DecodeDecimal(untto, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, bwtar)
			record = append(record, bwtty)
			record = append(record, abskz)
			record = append(record, agmem)
			record = append(record, elikz)
			record = append(record, erekz)
			record = append(record, pstyp)
			record = append(record, knttp)
			record = append(record, kzvbr)
			record = append(record, vrtkz)
			record = append(record, twrkz)
			record = append(record, wepos)
			record = append(record, weunb)
			record = append(record, repos)
			record = append(record, webre)
			record = append(record, kzabs)
			record = append(record, labnr)
			record = append(record, konnr)
			record = append(record, ktpnr)
			record = append(record, abdat)

			neg, i = utils.DecodeDecimal(abftz, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(etfz1, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(etfz2, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, kzstu)
			record = append(record, notkz)
			record = append(record, lmein)
			record = append(record, evers)

			neg, i = utils.DecodeDecimal(zwert, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(navnw, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(abmng, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, prdat)
			record = append(record, bstyp)

			neg, i = utils.DecodeDecimal(effwr, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, xoblr)
			record = append(record, kunnr)
			record = append(record, adrnr)
			record = append(record, ekkol)
			record = append(record, sktof)
			record = append(record, stafo)

			neg, i = utils.DecodeDecimal(plifz, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(ntgew, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, gewei)
			record = append(record, txjcd)
			record = append(record, etdrk)
			record = append(record, sobkz)
			record = append(record, arsnr)
			record = append(record, arsps)
			record = append(record, insnc)
			record = append(record, ssqss)
			record = append(record, zgtyp)
			record = append(record, ean11)
			record = append(record, bstae)
			record = append(record, revlv)
			record = append(record, geber)
			record = append(record, fistl)
			record = append(record, fipos)
			record = append(record, ko_gsber)
			record = append(record, ko_pargb)
			record = append(record, ko_prctr)
			record = append(record, ko_pprctr)
			record = append(record, meprf)

			neg, i = utils.DecodeDecimal(brgew, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(volum, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, voleh)
			record = append(record, inco1)
			record = append(record, inco2)
			record = append(record, vorab)
			record = append(record, kolif)
			record = append(record, ltsnr)
			record = append(record, packno)
			record = append(record, fplnr)

			neg, i = utils.DecodeDecimal(gnetwr, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, stapo)
			record = append(record, uebpo)
			record = append(record, lewed)
			record = append(record, emlif)
			record = append(record, lblkz)
			record = append(record, satnr)
			record = append(record, attyp)
			record = append(record, kanba)
			record = append(record, adrn2)
			record = append(record, cuobj)
			record = append(record, xersy)
			record = append(record, eildt)
			record = append(record, drdat)
			record = append(record, druhr)
			record = append(record, drunr)
			record = append(record, aktnr)
			record = append(record, abeln)
			record = append(record, abelp)

			neg, i = utils.DecodeDecimal(anzpu, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, punei)
			record = append(record, saiso)
			record = append(record, saisj)
			record = append(record, ebon2)
			record = append(record, ebon3)
			record = append(record, ebonf)
			record = append(record, mlmaa)

			neg, i = utils.DecodeDecimal(mhdrz, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, anfnr)
			record = append(record, anfps)
			record = append(record, kzkfg)
			record = append(record, usequ)
			record = append(record, umsok)
			record = append(record, banfn)
			record = append(record, bnfpo)
			record = append(record, mtart)
			record = append(record, uptyp)
			record = append(record, upvor)

			neg, i = utils.DecodeDecimal(kzwi1, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(kzwi2, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(kzwi3, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(kzwi4, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(kzwi5, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(kzwi6, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))
			record = append(record, sikgr)

			neg, i = utils.DecodeDecimal(mfzhi, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			neg, i = utils.DecodeDecimal(ffzhi, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, retpo)
			record = append(record, aurel)
			record = append(record, bsgru)
			record = append(record, lfret)
			record = append(record, mfrgr)
			record = append(record, nrfhg)
			record = append(record, j_1bnbm)
			record = append(record, j_1bmatuse)
			record = append(record, j_1bmatorg)
			record = append(record, j_1bownpro)
			record = append(record, j_1bindust)
			record = append(record, abueb)
			record = append(record, nlabd)
			record = append(record, nfabd)
			record = append(record, kzbws)

			neg, i = utils.DecodeDecimal(bonba, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, fabkz)
			record = append(record, j_1aindxp)
			record = append(record, j_1aidatep)
			record = append(record, mprof)
			record = append(record, eglkz)
			record = append(record, kztlf)
			record = append(record, kzfme)
			record = append(record, rdprf)
			record = append(record, techs)
			record = append(record, chg_srv)
			record = append(record, chg_fplnr)
			record = append(record, mfrpn)
			record = append(record, mfrnr)
			record = append(record, emnfr)
			record = append(record, novet)
			record = append(record, afnam)
			record = append(record, tzonrc)
			record = append(record, iprkz)
			record = append(record, lebre)
			record = append(record, berid)
			record = append(record, xconditions)
			record = append(record, apoms)
			record = append(record, ccomp)
			record = append(record, grant_nbr)
			record = append(record, fkber)
			record = append(record, status)
			record = append(record, reslo)
			record = append(record, kblnr)
			record = append(record, kblpos)
			record = append(record, weora)
			record = append(record, srv_bas_com)
			record = append(record, prio_urg)
			record = append(record, prio_req)
			record = append(record, empst)
			record = append(record, diff_invoice)
			record = append(record, trmrisk_relevant)
			record = append(record, spe_abgru)
			record = append(record, spe_crm_so)
			record = append(record, spe_crm_so_item)
			record = append(record, spe_crm_ref_so)
			record = append(record, spe_crm_ref_item)
			record = append(record, spe_crm_fkrel)
			record = append(record, spe_chng_sys)
			record = append(record, spe_insmk_src)
			record = append(record, spe_cq_ctrltype)
			record = append(record, spe_cq_nocq)
			record = append(record, reason_code)

			neg, i = utils.DecodeDecimal(cqu_sar, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, strconv.Itoa(anzsn))
			record = append(record, spe_ewm_dtc)
			record = append(record, exlin)
			record = append(record, exsnr)
			record = append(record, ehtyp)

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
			record = append(record, fls_rsto)
			record = append(record, ext_rfx_number)
			record = append(record, ext_rfx_item)
			record = append(record, ext_rfx_system)
			record = append(record, srm_contract_id)
			record = append(record, srm_contract_itm)
			record = append(record, blk_reason_id)
			record = append(record, blk_reason_txt)
			record = append(record, itcons)
			record = append(record, fixmg)
			record = append(record, bev1negen_item)
			record = append(record, bev1nedepfree)
			record = append(record, bev1nestruccat)
			record = append(record, advcode)
			record = append(record, budget_pd)
			record = append(record, excpe)
			record = append(record, iuid_relevant)
			record = append(record, mrpind)
			record = append(record, zztrans_type)
			record = append(record, zztransp_type)
			record = append(record, zzloadport)
			record = append(record, zzdestport)
			record = append(record, zzdischarge)
			record = append(record, zztrans_port)
			record = append(record, zzfrchl)
			record = append(record, zzupdate)
			record = append(record, zzdo_so)
			record = append(record, zzuom_gr)
			record = append(record, zzanln1)
			record = append(record, zzanln2)
			record = append(record, zzinventory)
			record = append(record, zzestate)
			record = append(record, zzmatnr)
			record = append(record, zzdono)
			record = append(record, zzdodate)

			neg, i = utils.DecodeDecimal(zzdoqty, &bi)
			z = utils.BigIntToFloat(neg, &bi, i)
			record = append(record, fmt.Sprintf("%.4f", z))

			record = append(record, zzctr_num)
			record = append(record, zzctr_dat)
			record = append(record, zzccpj)
			record = append(record, zzafce)
			record = append(record, zzxcont)
			record = append(record, zzcpno_long)
			record = append(record, zzcpit)
			record = append(record, zitgrp)
			record = append(record, zzstono)
			record = append(record, zzstoitem)
			record = append(record, zzqtyhl)
			record = append(record, zzstor_no)
			record = append(record, zzstor_it)
			record = append(record, zzkb)
			record = append(record, zzrefhno)
			record = append(record, zzpino)
			record = append(record, zzpono)
			record = append(record, zzvbeln_v1)
			record = append(record, zzposnr_v1)
			record = append(record, zz_qm_opr_rm)
			record = append(record, zz_qm_opr_fp)
			record = append(record, zzposnr)
			record = append(record, zzrsnum)
			record = append(record, zzrspos)
			record = append(record, zzbudgetcode)
			record = append(record, refsite)
			record = append(record, ref_item)
			record = append(record, source_id)
			record = append(record, source_key)
			record = append(record, put_back)
			record = append(record, pol_id)
			record = append(record, cons_order)
			record = append(record, bukrs1)
			record = append(record, land1)
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