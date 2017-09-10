# Cypher to SQL Translator Tool - _Reagan_

>_version 4.0_

>Please note that this tool has been redevloped and now exists in a new repository, as a result of some additional research! Please visit https://github.com/DTG-FRESCO/cyp2sql. 

<details>
 <summary>Existing README.md for this repo.</summary>
_Reagan_ v4.0 is a tool that will perform automatic translation of both 
graph schemas and graph queries in Neo4J and Cypher respectively, 
to a relational schema and relational query language (SQL).
This is an individual project for the Part II Tripos of 
Computer Science at the University of Cambridge. 

## Modules
### Schema Converter
- Takes as input a dump from the Neo4J shell of an existing graph database.
- Dump needs to be obtained manually by the user.
- The current schema translator converts the dump to two relations at a minimum: nodes and edges. The translator will attempt to optimise, if possible, by creating smaller relations with fewer NULLs. A separate metadata file is also created containing all the possible labels of the node store.
 
### Cypher parsing unit
- Built using ANTLRv4 tool.
- Grammar: https://s3.amazonaws.com/artifacts.opencypher.org/Cypher.g4
- Produces a token list which can be parsed more in the Cyp2SQL tool.

### Query translator unit
- Maps the Cypher query input to an internal representation in Java.
- This is then used to build up the SQL piece by piece.

### Output Module
- Fills in two text files - one is the results from Neo4J based on the Cypher input; the other is the results from Postgres based on the SQL generated from the Cypher input.
- Program outputs true/false depending on whether or not the files match.
- NOTE: this is not wholly accurate due to the following issues: encoding differences, differences in ways that sorting occurs, treatment of NULL.
- Thus, the files also contain an indicator at the bottom of the file as to how many records were returned. This is a quick way of checking that the translator was successful.
- The times of execution of both databases is also displayed by the tool.


## Instructions for Running
The properties file (configC2S.properties) must first be set with the correct properties.

Run the .jar with the following parameters, depending on whether or not the schema needs to be translated first.

```bash
java -jar reaganV4_0.jar <-schema|-translate|-s|-t|-tc> <schemaFile|queriesFile> <databaseName> <-e|-p|-c>
```

First, convert the graph schema to Postgres:
```bash
java -jar reaganV4_0.jar -schema myDump.txt coolDatabase
java -jar reaganV4_0.jar -s myDump.txt coolDatabase
```

If successful, queries can now be translated:
```bash
java -jar reaganV4_0.jar -translate myQueries.txt coolDatabase
```

If you want the results of the queries to be outputted to a local file for inspection of the results, then use the -p flag:
```bash
java -jar Reagan.jar -t myQueries.txt coolDatabase -p
```

To have the results emailed back:
```bash
java -jar Reagan.jar -translate myQueries.txt coolDatabase -e
```

To run the tool using the transitive closure approach:
```bash
java -jar Reagan.jar -tc myQueries.txt coolDatabase
```

If running a query which will edit the database, use the -c flag (excluding this will run the query multiple times, leading to erroneous consequences):
```bash
java -jar Reagan.jar -t changeDatabaseQueries.txt coolDatabase -c
```

The "myQueries.txt" should have each Cypher query on ONE LINE - adding a comment marker "//" to the start of the line will skip that query when the application is launched:
```bash
MATCH (n) RETURN n;
MATCH (a:LabelA)-[:SOMETHING]->(b) RETURN COUNT(b);
// will not run THIS line, or the one below
// MATCH (n)-->(m) RETURN m;
```

###Notes
* Aliases may be used, but they cannot be the same as the field they are being
an alias for.
* Do not use the -p flag when the quantity of data being returned is large.
It will not only be very slow, but will generally not be very good for the machine.
* This tool has bugs! Be patient with it, stick to the queries listed above.


## Visual representation of the toolchain.
![Overview of toolchain](https://github.com/ocrawford555/cyp2sql/blob/master/Overview.png)
</details>





