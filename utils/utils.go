package utils

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"

	"github.com/go-ini/ini"
	"github.com/morxs/go-hana/utils"
)

const (
	// DriverName - Default driver name for HANA DB from SAP
	DriverName = "hdb"
)

// ReadConfig - Read config from ini files
func ReadConfig(p string) string {
	iniCfg, err := ini.Load(p)
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

	return hdbDsn
}

//WriteMsg - Just a wrapper of fmt.Print()
func WriteMsg(s string) {
	fmt.Println(s)
}

// DecodeDecimal - Copy code from SAP drive to enable decode Decimals
func DecodeDecimal(b []byte, m *big.Int) (bool, int) {

	//bigint word size (*--> src/pkg/math/big/arith.go)
	const (
		dec128Bias = 6176
		// Compute the size _S of a Word in bytes.
		_m    = ^big.Word(0)
		_logS = _m>>8&1 + _m>>16&1 + _m>>32&1
		_S    = 1 << _logS
	)

	neg := (b[15] & 0x80) != 0
	exp := int((((uint16(b[15])<<8)|uint16(b[14]))<<1)>>2) - dec128Bias

	b14 := b[14]  // save b[14]
	b[14] &= 0x01 // keep the mantissa bit (rest: sign and exp)

	//most significand byte
	msb := 14
	for msb > 0 {
		if b[msb] != 0 {
			break
		}
		msb--
	}

	//calc number of words
	numWords := (msb / _S) + 1
	w := make([]big.Word, numWords)

	k := numWords - 1
	d := big.Word(0)
	for i := msb; i >= 0; i-- {
		d |= big.Word(b[i])
		if k*_S == i {
			w[k] = d
			k--
			d = 0
		}
		d <<= 8
	}
	b[14] = b14 // restore b[14]
	m.SetBits(w)
	return neg, exp
}

// BigIntToFloat - Convert to float
func BigIntToFloat(sign bool, m *big.Int, exp int) float64 {
	var neg int64
	if sign {
		neg = -1
	} else {
		neg = 1
	}

	return float64(neg*m.Int64()) * math.Pow10(exp)
}

// ReadCsv - Read CSV file and return as string slice
func ReadCsv(f string, comma rune) (rec [][]string, count int) {
	file, err := os.Open(f)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = comma

	lineCount := 0

	var WholeRecord [][]string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error: ", err)
			return
		}
		lineCount++
		WholeRecord = append(WholeRecord, record)
	}
	return WholeRecord, lineCount
}

// ConvertByteToStr = ConvertByteToStr
func ConvertByteToStr(b []byte, bi big.Int) string {
	//var bi big.Int
	var z float64
	var neg bool
	var i int

	neg, i = DecodeDecimal(b, &bi)
	z = BigIntToFloat(neg, &bi, i)
	//record = append(record, fmt.Sprintf("%.4f", z))
	return fmt.Sprintf("%.4f", z)
}
