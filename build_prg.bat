echo "Build main program"
go build -ldflags "-s -w" cmd\ekko\ekko.go 
go build -ldflags "-s -w" cmd\ekpo\ekpo.go
go build -ldflags "-s -w" cmd\lfa1\lfa1.go
go build -ldflags "-s -w" cmd\mara\mara.go
go build -ldflags "-s -w" cmd\t024\t024.go
go build -ldflags "-s -w" cmd\t024e\t024e.go
go build -ldflags "-s -w" cmd\tcurr\tcurr.go
go build -ldflags "-s -w" cmd\zstxl\zstxl.go

echo "Build other utilities"
go build -ldflags "-s -w" cmd\dl_consolpack_rtemplate\dl_consolpack_rtemplate.go
go build -ldflags "-s -w" cmd\dl_gl_consol_pack_map\dl_gl_consol_pack_map.go
go build -ldflags "-s -w" cmd\gen_code_ddf\gen_code_ddf.go
go build -ldflags "-s -w" cmd\upload_gl_consol_pack_map\upload_gl_consol_pack_map.go