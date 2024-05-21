CREATE OR REPLACE PROCEDURE PTX_HIST_DB.PAYMATIX_DM_FRAMEWORK.STATS_TABLE_DATATYPE_V51()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE c1 CURSOR FOR select tc.DB_NAME as DB_NAME,tc.DB_SCHEMA as DB_SCHEMA, tc.tab_name as TABLE_NAME, 
tc.COL_NAME as COLUMN_NAME,tc. DATA_TYPE from PTX_HIST_DB.PAYMATIX_DM_FRAMEWORK.tab_col_list tc
INNER join PTX_HIST_DB.PAYMATIX_DM_FRAMEWORK.tab_list tl
on upper(tc.tab_name)=upper(tl.tab_name)
where 1=1 and upper(data_type) IN ('DATE','TIMESTAMP_LTZ','TIMESTAMP_NTZ','NUMBER','DECIMAL', 'INT', 'MONEY',
'NUMERIC','SMALLINT', 'SMALLMONEY','TINYINT','SMALLDATETIME','TIME','DATETIME')
and tl.status = 'Y'
-- AND upper(tc.tab_name) in ('CYCLEACCOUNT_MERGED')
;
SQL_STATEMENT STRING;
var_db_name STRING;
var_db_schema STRING;
var_table_name STRING;
var_column_name STRING;
var_data_type STRING;
rs RESULTSET;
tmp INT;
counter INTEGER DEFAULT 1;
maximum_count INTEGER default 5;
measure_name string;
measure_value int;
measure_name_date string;
measure_value_date string;
V_MIN INT;
V_MAX INT ;
V_AVG INT;
V_SUM INT; 
V_MIN_DATE STRING;
V_MAX_DATE STRING;
error varchar(1000);
insert_cmd STRING default 'INSERT INTO PTX_HIST_DB.PAYMATIX_DM_FRAMEWORK.RECON_RESULT (SCHEMA_NAME,tab_name ,col_name ,data_type,recon_parameter ,recon_result ) VALUES(?,?,?,?,?,?);';
BEGIN
    FOR record IN c1 DO
  var_db_name := record.DB_NAME;
       var_db_schema := record.DB_SCHEMA;
  var_table_name := record.TABLE_NAME;
  var_column_name := record.COLUMN_NAME;
  var_data_type := record.DATA_TYPE;
 
BEGIN
   if (upper(var_data_type) in ('NUMBER','DECIMAL', 'INT', 'MONEY','NUMERIC','SMALLINT', 'SMALLMONEY')) THEN
SQL_STATEMENT := 'SELECT  NVL(MIN(' || var_column_name || '),0) AS V_MIN, NVL(MAX(' || var_column_name || '),0) AS V_MAX,
NVL(AVG('|| var_column_name || '),0) AS V_AVG, NVL(SUM(' || var_column_name || '),0) AS V_SUM FROM PTX_HIST_DB.' 
|| var_db_name||'.'||var_table_name||' ;';
        else
SQL_STATEMENT := 'SELECT  nvl(MIN(' || var_column_name || '),''1900-01-01 22:02:38.789'') AS V_MIN, nvl(MAX(' || var_column_name || '),
''1900-01-01 22:02:38.789'') AS V_MAX FROM PTX_HIST_DB.'||var_db_name||'.'||var_table_name||';';
        END IF;
        -- RETURN SQL_STATEMENT;
         rs := (execute Immediate :SQL_STATEMENT);
         -- RETURN TABLE(rs);
FOR tbl_row IN rs DO  
            if (upper(var_data_type) in('NUMBER','DECIMAL', 'INT', 'MONEY','NUMERIC','SMALLINT', 'SMALLMONEY')) THEN
                V_MIN := tbl_row.V_MIN;
                V_MAX := tbl_row.V_MAX;
                V_AVG := tbl_row.V_AVG;
                V_SUM := tbl_row.V_SUM; 
                maximum_count :=4;
            else
                V_MIN_DATE := tbl_row.V_MIN;
                maximum_count :=2;
                V_MAX_DATE := tbl_row.V_MAX; 
            end if;
            FOR i IN 1 TO maximum_count DO
                if (upper(var_data_type) in('NUMBER','DECIMAL', 'INT', 'MONEY','NUMERIC','SMALLINT', 'SMALLMONEY')) THEN
                    measure_name := CASE 
					WHEN counter=1 then 'MIN Value' 
					WHEN counter=2 then 'MAX Value' 
					WHEN counter=3 THEN 'AVG Value' 
					WHEN counter=4 THEN 'SUM Value' END;
                    measure_value := CASE WHEN counter=1 then V_MIN WHEN counter=2 then V_MAX WHEN counter=3 THEN V_AVG WHEN counter=4 THEN V_SUM END;
   EXECUTE IMMEDIATE insert_cmd using(var_db_name,var_table_name,var_column_name,var_data_type,measure_name,measure_value);
                else
                    measure_name_date := CASE WHEN counter=1 then 'MIN_DATE' WHEN counter=2 then 'MAX_DATE' END;
                    measure_value_date := CASE WHEN counter=1 then V_MIN_DATE WHEN counter=2 then V_MAX_DATE END;
   EXECUTE IMMEDIATE insert_cmd using(var_db_name,var_table_name,var_column_name,var_data_type,measure_name_date,measure_value_date);
                end if;
                counter := counter + 1;
            END FOR;  
				counter := 1;   			
        END FOR;
   EXCEPTION  WHEN STATEMENT_ERROR THEN 
LET error :=  'TABLE_NAME:' || var_table_name ||'Error type : EXPRESSION_ERROR,SQLCODE :'|| SQLCODE ||',SQLERRM :'|| SQLERRM ||' ,SQLSTATE'|| SQLSTATE;
     INSERT INTO PTX_HIST_DB.PAYMATIX_DM_FRAMEWORK.AUDIT_LOG VALUES (:error,CURRENT_TIMESTAMP(),CURRENT_USER());                            
WHEN EXPRESSION_ERROR THEN
  LET error := 'TABLE_NAME:' || var_table_name ||'Error type : EXPRESSION_ERROR,SQLCODE :'|| SQLCODE ||',SQLERRM :'|| SQLERRM ||' ,SQLSTATE'|| SQLSTATE;
	 INSERT INTO PTX_HIST_DB.PAYMATIX_DM_FRAMEWORK.AUDIT_LOG VALUES (:error,CURRENT_TIMESTAMP(),CURRENT_USER());  
WHEN OTHER THEN
     LET error := 'TABLE_NAME:' || var_table_name ||'Error type : EXPRESSION_ERROR,SQLCODE :'|| SQLCODE ||',SQLERRM :'|| SQLERRM ||' ,SQLSTATE'|| SQLSTATE;
     INSERT INTO PTX_HIST_DB.PAYMATIX_DM_FRAMEWORK.AUDIT_LOG VALUES (:error,CURRENT_TIMESTAMP(),CURRENT_USER()); 	
END;	 
    end for;
    Return 'inserted values successfully..!';
 
END;