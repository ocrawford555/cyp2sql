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

## Types of queries that can be converted
- match (n:Film)-[:HAS_VEHICLE]->(m:Vehicle {Brand:"Alfa"}) return n;
- match (n:Film)-[:HAS_VEHICLE]->(m:Vehicle) where n.Name = 'Die Another Day' return m;
- match (n)-->(m:Film) return m order by m.Name limit 20;
- match (n:Film)-[:HAS_VEHICLE]->(m:Vehicle) where n.Name <> 'Die Another Day' return m;
- match (n:Film)<-[]->(p:People) return *;
- match (n:Film)-[:HAS_VEHICLE]->(m:Vehicle) where n.Name <> 'Die Another Day' return distinct m.Brand as Things;
- match (n:Film)-[:HAS_VEHICLE]->(m:Vehicle) where n.Name = 'Die Another Day' return m order by m.Brand DESC;
- match (n)-->(m:Film) return m order by m.Name skip 20 limit 40;

MORE INFO AND COMMITS TO COME!
