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

## Examples of query translations
 |                                                                    Cypher                                                                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       SQL                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
 |--------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
 |MATCH (n:Movie) return n.title limit 100;                                                                                                         | SELECT n.title FROM movie n LIMIT 100; |
 |MATCH (u:User) return distinct u.name AS Reviewer;                                                                                                | SELECT DISTINCT n.name AS reviewer FROM person_user n; |
 |MATCH (d:Director) return count(d) AS NumDirectors;                                                                                               | SELECT count(n) FROM nodes n WHERE n.label LIKE '%director%' ; |
 |MATCH (d:User) return *;                                                                                                                          | SELECT * FROM person_user n WHERE n.label LIKE '%user%' ; |
 |MATCH (d:Director) return d.name AS FirstName, d.birthplace AS BornLocation;                                                                      | SELECT n.name AS firstname, n.birthplace AS bornlocation FROM nodes n WHERE n.label LIKE '%director%'  AND n.label LIKE '%director%' ; |
 |MATCH (u:User) WHERE u.login <> "a999" RETURN u.name ORDER BY u.name DESC;                                                                        | SELECT n.name FROM person_user n WHERE n.login <> 'a999' ORDER BY n.name desc; |
 |MATCH (a {runtime:100}) RETURN a.genre ORDER BY a.title LIMIT 5;                                                                                  | SELECT n.genre FROM movie n WHERE n.runtime = '100' ORDER BY n.title asc LIMIT 5; |
 |MATCH (d:Director)-[*1..2]->(f) return f.title limit 75                                                                                           | CREATE TEMP VIEW zerostep AS SELECT id from nodes WHERE label LIKE '%director%' ; CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr as x FROM tClosure JOIN zerostep on idl = zerostep.id JOIN nodes as n on idr = n.id where depth <= 2 AND depth >= 1) SELECT * from graphT); SELECT n.title FROM movie n JOIN step on x = n.id LIMIT 75; |
 |MATCH (a)<-[*2..4]-(b) return a;                                                                                                                  | CREATE TEMP VIEW zerostep AS SELECT id from nodes; CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr as x FROM tClosure JOIN zerostep on idl = zerostep.id JOIN nodes as n on idr = n.id where depth <= 4 AND depth >= 2) SELECT * from graphT); SELECT * FROM nodes n JOIN step on x = n.id; |
 |MATCH (a)<-[*]-(b:Actor {name:"Tom Hanks"}) return a;                                                                                             | CREATE TEMP VIEW zerostep AS SELECT id from nodes WHERE name = 'tom hanks' AND label LIKE '%actor%' ; CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr as x FROM tClosure JOIN zerostep on idl = zerostep.id JOIN nodes as n on idr = n.id where depth <= 100 AND depth >= 1) SELECT * from graphT); SELECT * FROM nodes n JOIN step on x = n.id; |
 |match (n:Actor)-[:ACTS_IN]-(m:Movie) where n.name = "Ben Miller" return m.tagline order by m.title asc;                                           | WITH a AS (SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 on n1.id = e1.idl INNER JOIN movie n2 on e1.idr = n2.id WHERE n1.name = 'ben miller' AND n1.label LIKE '%actor%'  AND n2.label LIKE '%movie%'  AND e1.type = 'acts_in' UNION ALL SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 on n1.id = e1.idr INNER JOIN movie n2 on e1.idl = n2.id WHERE n1.name = 'ben miller' AND n1.label LIKE '%actor%'  AND n2.label LIKE '%movie%'  AND e1.type = 'acts_in') SELECT n.tagline FROM movie n, a  WHERE n.id = a.a2 ORDER BY n.title asc; |
 |match (a:Actor)-[]->(m:Movie)<-[]-(d:Director)-[]->(j:Movie)<--(b:Actor) where a.name = "Miley Cyrus" return distinct b.name order by b.name asc; | WITH a AS (SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 on n1.id = e1.idl INNER JOIN movie n2 on e1.idr = n2.id WHERE n1.name = 'miley cyrus' AND n1.label LIKE '%actor%'  AND n2.label LIKE '%movie%' ), b AS (SELECT n1.id AS b1, n2.id AS b2, e2.* FROM movie n1 INNER JOIN edges e2 on n1.id = e2.idr INNER JOIN nodes n2 on e2.idl = n2.id WHERE n1.label LIKE '%movie%'  AND n2.label LIKE '%director%' ), c AS (SELECT n1.id AS c1, n2.id AS c2, e3.* FROM nodes n1 INNER JOIN edges e3 on n1.id = e3.idl INNER JOIN movie n2 on e3.idr = n2.id WHERE n1.label LIKE '%director%'  AND n2.label LIKE '%movie%' ), d AS (SELECT n1.id AS d1, n2.id AS d2, e4.* FROM movie n1 INNER JOIN edges e4 on n1.id = e4.idr INNER JOIN nodes n2 on e4.idl = n2.id WHERE n1.label LIKE '%movie%'  AND n2.label LIKE '%actor%' ) SELECT DISTINCT n.name FROM nodes n, a, b, c, d  WHERE a.a2 = b.b1 AND b.b2 = c.c1 AND c.c2 = d.d1 AND n.id = d.d2 AND a.a1 != b.b2 AND a.a2 != c.c2 AND b.b2 != d.d2 ORDER BY n.name asc; |
 |match (n:Movie {title:"Love Actually"})<-[:ACTS_IN]-(x)-->(t) return t.studio order by t.releaseDate;                                             | WITH a AS (SELECT n1.id AS a1, n2.id AS a2, e1.* FROM movie n1 INNER JOIN edges e1 on n1.id = e1.idr INNER JOIN nodes n2 on e1.idl = n2.id WHERE n1.title = 'love actually' AND n1.label LIKE '%movie%'  AND e1.type = 'acts_in'), b AS (SELECT n1.id AS b1, n2.id AS b2, e2.* FROM nodes n1 INNER JOIN edges e2 on n1.id = e2.idl INNER JOIN nodes n2 on e2.idr = n2.id) SELECT n.studio FROM movie n, a, b  WHERE a.a2 = b.b1 AND n.id = b.b2 AND a.a1 != b.b2 ORDER BY n.releasedate asc; |
 |MATCH (a)-[c {comment:"Run Forrest, Run!"}]-(b) where c.stars = 5 return b;                                                                       | WITH a AS (SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 on n1.id = e1.idl INNER JOIN nodes n2 on e1.idr = n2.id WHERE e1.comment = 'run forrest, run!' AND e1.stars = '5' UNION ALL SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 on n1.id = e1.idr INNER JOIN nodes n2 on e1.idl = n2.id WHERE e1.comment = 'run forrest, run!' AND e1.stars = '5') SELECT n.* FROM nodes n, a  WHERE n.id = a.a2; |
 |MATCH (n:Movie) where n.runtime > 140 and n.studio = "Warner Bros. Pictures" return n.title order by n.title asc;                                 | SELECT n.title FROM movie n WHERE n.runtime > '140' AND n.studio = 'warner bros. pictures' ORDER BY n.title asc; |

## Visual representation of the toolchain.
![Overview of toolchain](https://github.com/ocrawford555/cyp2sql/blob/master/Overview.png)






