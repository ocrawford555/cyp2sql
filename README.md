# Cyp 2 SQL Translator Tool (v1)

Cyp2SQL v1 is a tool that will perform automatic translation of both graph schemas and graph queries in Neo4J and Cypher respectively, to a relational schema and relational query language (SQL). 

##Modules
- a schema converter from Neo4J to a relational database
- a Cypher parsing unit
- the query translator itself
- a module for comparing the outputs from both Neo4J and the relational DB
  
## Additional Notes
- Project for University of Cambridge Part II tripos.
- Written in Java.
- Uses ANTLR4 parsing tool

## Get dump from Neo4J console and store in a text file.
neo4jplay.bat -c dump  > testD.txt (then type dump into the console, followed by enter). When file stops growing, then can leave the command.

neo4jplay.bat = 
@echo off
java -classpath "C:\Program Files\Neo4j CE 3.0.6\bin\neo4j-desktop-3.0.6.jar" org.neo4j.shell.StartClient

