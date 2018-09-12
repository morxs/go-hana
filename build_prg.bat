@echo off
echo "Build main program"
go build -ldflags "-s -w" ekko.go 
go build -ldflags "-s -w" ekpo.go
go build -ldflags "-s -w" lfa1.go
go build -ldflags "-s -w" mara.go
go build -ldflags "-s -w" t024.go
go build -ldflags "-s -w" t024e.go
go build -ldflags "-s -w" tcurr.go
go build -ldflags "-s -w" zstxl.go

echo "Build other utilities"
go build -ldflags "-s -w" dl_consolpack_rtemplate.go
go build -ldflags "-s -w" dl_gl_consol_pack_map.go
go build -ldflags "-s -w" gen_code_ddf.go
go build -ldflags "-s -w" upload_gl_consol_pack_map.go