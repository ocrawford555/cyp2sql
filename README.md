# Cyp 2 SQL Translator Tool (v1)

Cyp2SQL v1 is a tool that will perform automatic translation of both graph schemas and graph queries in Neo4J and Cypher respectively, to a relational schema and relational query language (SQL).
This is a project for the Part II Tripos of Computer Science at the University of Cambridge. 

## Modules
### Schema Converter
- Takes as input a dump from Neo4J shell.
- This can be part-automated by running the following command: neo4jplay.bat -c dump  > testD.txt (then type "dump" into the console, followed by enter). When the file "testD.txt" stops growing, the command prompt can be exited.
- neo4jplay.bat = @echo off java -classpath "C:\Program Files\Neo4j CE 3.0.6\bin\neo4j-desktop-3.0.6.jar" org.neo4j.shell.StartClient
- The current schema translator converts the dump to two relations: nodes and edges. A separate metadata file is also created containing all the possible labels of the node store.
- Future improvements: performance and speed analysis, create more than just the two relations currently being created, and decide at conversion time of a query which set of relations to use for the best performance.
 
### Cypher parsing unit
- Built using ANTLRv4 tool.
- Grammar: https://s3.amazonaws.com/artifacts.opencypher.org/Cypher.g4
- Produces a token list which can be parsed more in the Cyp2SQL tool.
- Future improvements: performance analysis, extend for more aspects of the language.

### Query translator unit
- Maps the Cypher query input to an internal representation in Java.
- This is then used to build up the SQL piece by piece.
- Types of queries that can be modelled currently shown below.
- Future improvements: extend for more of the Cypher language, refactor to cleaner looking code, allow for more complex queries to be mapped (such as allowing for more than one -[*]- relationship).

### Output Module
- Fills in two text files - one is the results from Neo4J based on the Cypher input; the other is the results from Postgres based on the SQL generated from the Cypher input.
- Program outputs true/false depending on whether or not the files match.
- NOTE: this is not wholly accurate due to the following issues: encoding differences, differences in ways that sorting occurs, treatment of NULL.
- Thus, the files also contain an indicator at the bottom of the file as to how many records were returned. This is a quick way of checking that the translator was successful.
- Future improvements: bug fixes only, if time allows then fix encoding issue, but this is not a major concern currently.
  
## Examples of queries that can be translated
|Queries|
|-------|
|MATCH (n:Movie) return n.title limit 100;|
|MATCH (u:User) return distinct u.name AS Reviewer;|
|MATCH (d:Director) return count(d) AS NumDirectors;|
|MATCH (d:User) return *;|
|MATCH (d:Director) return d.name AS FirstName, d.birthplace AS BornLocation;|
|MATCH (u:User) WHERE u.login <> "a999" RETURN u.name ORDER BY u.name DESC;|
|MATCH (a {runtime:100}) RETURN a ORDER BY a.title LIMIT 5;|
|MATCH (d:Director)-[*1..2]->(f) return f.title limit 75|
|MATCH (a)<-[*2..4]-(b) return a;|
|MATCH (a)<-[*]-(b:Actor {name:"Tom Hanks"}) return a;|
|match (n:Actor)-[:ACTS_IN]-(m:Movie) where n.name = "Ben Miller" return m.tagline order by m.title asc;|
|match (a:Actor)-[]->(m:Movie)<-[]-(d:Director)-[]->(j:Movie)<--(b:Actor) where a.name = "Miley Cyrus" return distinct b.name order by b.name asc;|
|match (n:Movie {title:"Love Actually"})<-[:ACTS_IN]-(x)-->(t) return t.studio order by t.releaseDate;|
|MATCH (a)-[c {comment:"Run Forrest, Run!"}]-(b) where c.stars = 5 return b;|


## Instructions for Running
The properties file (configC2S.properties) must first be set with the correct properties.

Run the .jar (to come in next commit!) with the following parameters, depending on whether or not the schema needs to be translated first.

```bash
java -jar <location of .jar file> <-schema|-translate|-s|-t> <schemaFile|queriesFile> <databaseName>
```

Thus, if wishing to first convert the schema:
```bash
java -jar c2sv1.jar -schema myDump.txt coolDatabase
java -jar c2sv1.jar -s myDump.txt coolDatabase
```

If successful, queries can now be translated:
```bash
java -jar c2sv1.jar -translate myQueries.txt coolDatabase
```

The "myQueries.txt" should have each Cypher query on ONE LINE - adding a comment marker "//" to the start of the line will skip that query when the application is launched:
```bash
MATCH (n) RETURN n;
\\ will not run this line
MATCH (n)-->(m) RETURN m;
```

##Visual representation of the toolchain.
![Overview of toolchain](https://github.com/ocrawford555/cyp2sql/blob/master/Overview.png)






