@echo off
echo "Compressing binary using UPX"
exes\upx.exe -k --brute lfa1.exe ekko.exe ekpo.exe t024.exe t024e.exe tcurr.exe zstxl.exe mara.exe 

echo "Compressing utilities using UPX"
exes\upx.exe -k --brute upload_gl_consol_pack_map.exe gen_code_ddf.exe dl_gl_consol_pack_map.exe dl_consolpack_rtemplate.exe