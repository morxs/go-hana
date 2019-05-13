echo "Build zest program"
go build -ldflags "-s -w" cmd\zest_block\zest_block.go 
go build -ldflags "-s -w" cmd\zest_block2\zest_block2.go
go build -ldflags "-s -w" cmd\zest_blockb\zest_blockb.go
go build -ldflags "-s -w" cmd\zest_blockh\zest_blockh.go
go build -ldflags "-s -w" cmd\zest_division\zest_division.go
go build -ldflags "-s -w" cmd\zest_estate\zest_estate.go
go build -ldflags "-s -w" cmd\zest_oil_pom\zest_oil_pom.go
go build -ldflags "-s -w" cmd\zest_rday\zest_rday.go