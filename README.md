# Cypher to SQL Translator Tool - _Apoc_

>_version 3.2_

_Apoc_ v3.2 is a tool that will perform automatic translation of both 
graph schemas and graph queries in Neo4J and Cypher respectively, 
to a relational schema and relational query language (SQL).
This is an individual project for the Part II Tripos of 
Computer Science at the University of Cambridge. 

## Modules
### Schema Converter
- Takes as input a dump from Neo4J shell.
- This can be part-automated by running the following command: neo4jplay.bat -c dump  > testD.txt (then type "dump" into the console, followed by enter). When the file "testD.txt" stops growing, the command prompt can be exited.
- neo4jplay.bat = `@echo off java -classpath "C:\Program Files\Neo4j CE 3.0.6\bin\neo4j-desktop-3.0.6.jar" org.neo4j.shell.StartClient`
- The current schema translator converts the dump to two relations at a minimum: nodes and edges. The translator will attempt to optimise, if possible, by creating smaller relations with fewer NULLs. A separate metadata file is also created containing all the possible labels of the node store.
 
### Cypher parsing unit
- Built using ANTLRv4 tool.
- Grammar: https://s3.amazonaws.com/artifacts.opencypher.org/Cypher.g4
- Produces a token list which can be parsed more in the Cyp2SQL tool.
- Future improvements: performance analysis, nicer code in Java.

### Query translator unit
- Maps the Cypher query input to an internal representation in Java.
- This is then used to build up the SQL piece by piece.
- Types of queries that can be modelled currently shown below.
- Future improvements: refactor to cleaner looking code, allow for different representations to be translated against.

### Output Module
- Fills in two text files - one is the results from Neo4J based on the Cypher input; the other is the results from Postgres based on the SQL generated from the Cypher input.
- Program outputs true/false depending on whether or not the files match.
- NOTE: this is not wholly accurate due to the following issues: encoding differences, differences in ways that sorting occurs, treatment of NULL.
- Thus, the files also contain an indicator at the bottom of the file as to how many records were returned. This is a quick way of checking that the translator was successful.
- Future improvements: bug fixes.

 
## Examples of queries that can be translated
|Queries|
|-------|
|MATCH (b) WHERE b.title = "Chicken Run" OR b.title = "Stuart Little" OR b.name = "Ben Stiller" and b.studio = "Aardman Animations" RETURN b|
|MATCH (b) WHERE b.genre = "Action" AND b.version = 250 OR b.version = 240 AND b.runtime > 140 RETURN b|
|MATCH (n:Person:User) RETURN n.password|
|MATCH (n:Movie {studio:"Fine Line Features"}) RETURN n|
|MATCH (n:Movie {studio:"Fine Line Features"}) RETURN count(n)|
|MATCH (n:Movie {studio:"Fine Line Features"}) RETURN collect(n)|
|MATCH (n:Person:Actor) WHERE n.name = "Natalie Portman" RETURN n.biography AS Bio|
|MATCH (n:Movie) RETURN DISTINCT n.genre ORDER BY n.genre ASC|
|MATCH (n:Person:Actor:Director) RETURN n.name UNION ALL MATCH (n:User) RETURN n.name|
|MATCH (u:User) WHERE u.login <> "a999" RETURN u.password ORDER BY u.password DESC SKIP 5 LIMIT 20|
|MATCH (a)-[*2..3]->(b) RETURN b|
|MATCH (a:User)-[*1..3]->(b:Movie) RETURN a.name, b.title|
|MATCH (a)-[*2..3]->(b) RETURN b.title, a.name|
|MATCH (a)-[*1..3]->(b) WHERE b.title = "Chicken Run" RETURN collect(a.name) AS AllNames|
|MATCH (a)-[*1..3]->(b) WHERE a.version < 200 AND b.title = "Chicken Run" OR b.title = "Stuart Little" RETURN a|
|MATCH (a)<-[*1..4]-(b) WHERE a.genre <> "Action" AND b.name = "Jason Statham" RETURN a|
|MATCH (aa:Actor)-[:ACTS_IN]->(bb:Movie)<-[:DIRECTED]-(cc:Director) WHERE bb.title = "The Matrix" OR bb.title = "Titanic" RETURN DISTINCT aa.name ORDER BY aa.name|
|MATCH (a:Actor)-[:ACTS_IN]->(m:Movie) RETURN a.name, count(m) AS movie_count LIMIT 10|
|MATCH (a:Actor)-[:ACTS_IN]->(m:Movie) WITH a, count(m) AS movie_count WHERE movie_count > 48 RETURN a.name, movie_count ORDER BY movie_count DESC|
|MATCH (n:Actor:Director)-[:ACTS_IN]->(m:Movie {genre:"Horror"}) RETURN n.name UNION MATCH (n:Actor:Director)-[:ACTS_IN]->(m:Movie {genre:"Drama"}) RETURN n.name UNION MATCH (n:Actor:Director)-[:ACTS_IN]->(m:Movie {genre:"Horror"}) RETURN n.name|
|MATCH (n:Actor:Director)-[:DIRECTED]->(m:Movie {genre:"Action"}) return m.title As TitleFilm, m.imdbId As IMDBRef|
|MATCH (a:Actor)-[r:ACTS_IN]->(b:Movie) WHERE b.title = "Love Actually" RETURN r.name AS Role|
|CREATE (n:Person:Actor:Director {name:"Oliver Crawford"})-[:ACTS_IN]->(fe:Movie {title:"Amazing Days", genre:"Comedy"})|
|MATCH (a:Person)-[:ACTS_IN]->(b:Movie {title:"Amazing Days"}) RETURN a.name AS NewActorDirector|
|MATCH (n:Person:Actor:Director {name:"Oliver Crawford"}) DETACH DELETE n|
|MATCH (n:Movie {title:"Amazing Days"}) DETACH DELETE n|
|MATCH (m:Movie {genre:"Poor comedy..."})<-[:ACTS_IN]-(:Actor {name:"Jennifer Aniston"}) WITH collect(m) AS ms FOREACH (x in ms | set x.genre = "Comedy")|
|MATCH (u:User {name:"Olliver"}) WITH collect(u) AS ms FOREACH (x in ms | set x.name = "Oliver")|
|MATCH (u:User {name:"Olliver"}) WITH collect(u) AS ms FOREACH (x in ms | set x.name = "Oliver")|
|MATCH (eng {language:"en"}) WITH collect(eng) AS ms FOREACH (x in ms | set x.language = "English")|


## Instructions for Running
The properties file (configC2S.properties) must first be set with the correct properties.

Run the .jar with the following parameters, depending on whether or not the schema needs to be translated first.

```bash
java -jar <location of .jar file> <-schema|-translate|-s|-t|-t2> <schemaFile|queriesFile> <databaseName> <-p>
```

Thus, if wishing to first convert the schema:
```bash
java -jar apoc.jar -schema myDump.txt coolDatabase
java -jar apoc.jar -s myDump.txt coolDatabase
```

If successful, queries can now be translated:
```bash
java -jar apoc.jar -translate myQueries.txt coolDatabase
```

If you want the results of the queries to be outputted to a local file for inspection of the results, then use the -p flag:
```bash
java -jar apoc.jar -translate myQueries.txt coolDatabase -p
```

The "myQueries.txt" should have each Cypher query on ONE LINE - adding a comment marker "//" to the start of the line will skip that query when the application is launched:
```bash
MATCH (n) RETURN n;
\\ will not run THIS line
MATCH (n)-->(m) RETURN m;
```

###Notes
* Aliases may be used, but they cannot be the same as the field they are being
an alias for.
* Do not use the -p flag when the quantity of data being returned is large.
It will not only be very slow, and take up excess space, but will generally
not be very good for the machine.
* This tool has bugs! Be patient with it, stick to the queries listed above.


## Visual representation of the toolchain.
![Overview of toolchain](https://github.com/ocrawford555/cyp2sql/blob/master/Overview.png)






