SELECT
Table__1."BUKRS",
Table__1."BUTXT",
Table__1."WERKS",  
Table__1."NAME1",  
Table__1."LGORT",  
Table__1."LGOBE",  
Table__1."MATNR",  
Table__1."MAKTX",
Table__1."WAERS",
sum(Table__1."CLBAL_BS"),
sum(Table__1."LMINB"),
Table__1."MEINS",
SUM(Table__1."VERPR_VARDEC"),
SUM(Table__1."CLBAL" * Table__1."VERPR_VARDEC"),
Table__1."DATE_SQL",
Table__1."DATE_SQL_NO_FILTER",
Table__1."MBLNR",
Table__1."MJAHR",
Table__1."BWART",
Table__1."BTEXT",
Table__1."BUDAT",
Table__1."MJAHR_ISSUE",
Table__1."BUDAT_ISSUE"
FROM
  ( 
    SELECT 
      "MANDT"
      ,"MATNR"
      , "BUKRS" 
      , "WERKS" 
      , "LGORT"
      , "BUDAT"
      , "VERPR" 
      , "VERPR_VARDEC" 
      , "WAERS" 
      , "LAND1" 
      , "MBLNR"
      , "MJAHR" 
      , "BWART"
      , "DATE_SQL" 
      , "BTEXT" 
      , "MAKTX" 
      , "MEINS"
      , "MATKL"
      , "MTART" 
      , "BUTXT" 
      , "NAME1" 
      , "LGOBE" 
      , "MTBEZ" 
      , "WGBEZ" 
      , "PEINH"  
      , "OPBAL"
      , "CLBAL" 
      , "LMINB"
      , "CLBAL_BS"
      , "MAT_TYPE"
      , "DATE_SQL_NO_FILTER"
      , "BUDAT_NO_FILTER"
      , "MBLNR_NO_FILTER"
      ,"BUDAT_ISSUE"
      , "MBLNR_ISSUE"
      , "MJAHR_ISSUE"
    from "_SYS_BIC"."wip-slowmove/CA_SLOW_MOVE8"
    ('PLACEHOLDER' = ('$$company$$', 'AN', 
                      '$$start_period$$', '20170101', 
                      '$$end_period$$', '20171231')) 
ORDER BY "BUKRS" , "WERKS" , "LGORT" , "MATNR"
)  Table__1
WHERE
  (
   Table__1."MAT_TYPE" = 'NT'
   AND
   Table__1."CLBAL_BS" > 0 
   AND
   Table__1."MANDT" = '777'
   AND
   Table__1."MTART" = 'SPAR'
  )
GROUP BY
  Table__1."BUKRS", 
  Table__1."BUTXT", 
  Table__1."WERKS", 
  Table__1."NAME1", 
  Table__1."LGORT", 
  Table__1."LGOBE", 
  Table__1."MATNR", 
  Table__1."MAKTX", 
  Table__1."WAERS", 
  Table__1."MEINS", 
  Table__1."DATE_SQL", 
  Table__1."DATE_SQL_NO_FILTER", 
  Table__1."MBLNR", 
  Table__1."MJAHR", 
  Table__1."BWART", 
  Table__1."BTEXT", 
  Table__1."BUDAT", 
  Table__1."MJAHR_ISSUE", 
  Table__1."BUDAT_ISSUE"