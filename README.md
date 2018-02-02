This is Aster SQL-MR UDF to execute pushdown query on teradata. 

How to build (I used Intellij IDEA)
1. File -> Project Structure -> Artifacts -> + -> JAR -> From modules with dependencies -> OK
2. Build -> Build Artifacts... -> Build
3. Please check run_on_teradata.jar file is generated under ./out/artifacts/run_on_teradata_jar directory
4. Run following command on terminal
```
zip run_on_teradata.zip lib/tdgssconfig.jar lib/terajdbc4.jar out/artifacts/run_on_teradata_jar/run_on_teradata.jar
```

Install to aster instance
1. Login to Aster with act
2. Install run_on_teradata.zip
```
\install run_on_teradata.zip
```

Execute run_on_teradata udf
```sql
select * from run_on_teradata ( on (select 1)
 hostname ('teradata.hostname.org')
 username ('dbc')
 password ('***')
 query ('drop table schema.table')
)
;
```