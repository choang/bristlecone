rem Bristlecone-@VERSION@
rem
rem (UNDER CONSTRUCTION--DOES NOT WORK YET!)
rem
rem Bristlecone Cluster Test Tools
rem (c) 2006-2007 Continuent, Inc.. All rights reserved.

set BHOME=C:\Program Files\continuent\bristlecone
set CP=%BHOME%\lib\bristlecone.jar
set CP=%CP%;%BHOME%\lib\pcluster-8.0-314.jdbc3-driver.jar
set CP=%CP%;%BHOME%\lib\hsqldb.jar
set CP=%CP%;%BHOME%\lib\log4j.jar
set CP=%CP%;%BHOME%\config
java -cp "%CP%" com.continuent.bristlecone.evaluator.Evaluator %1
