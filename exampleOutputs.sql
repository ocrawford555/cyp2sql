cypher                                                                       | SQL
---------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
MATCH (n:Movie) RETURN n.title LIMIT 100;
| SELECT n.title FROM movie n LIMIT 100;
MATCH (u:USER ) RETURN DISTINCT u.name AS Reviewer;
| SELECT DISTINCT n.name AS reviewer FROM person_user n;
MATCH (d:Director) RETURN count(d) AS NumDirectors;
| SELECT count(n) FROM nodes n WHERE n.label LIKE '%director%';
MATCH (d:USER ) RETURN *;
| SELECT * FROM person_user n WHERE n.label LIKE '%user%';
MATCH (d:Director) RETURN d.name AS FirstName, d.birthplace AS BornLocation;
| SELECT n.name AS firstname, n.birthplace AS bornlocation FROM nodes n WHERE n.label LIKE '%director%' AND n.label LIKE '%director%';
MATCH (u:USER ) WHERE u.login <> "a999" RETURN u.name ORDER BY u.name DESC;
| SELECT n.name FROM person_user n WHERE n.login <> 'a999' ORDER BY n.name DESC;
MATCH (a {runtime:100}) RETURN a.genre ORDER BY a.title LIMIT 5;
| SELECT n.genre FROM movie n WHERE n.runtime = '100' ORDER BY n.title ASC LIMIT 5;
MATCH (d:Director)-[*1..2]->(f) RETURN f.title LIMIT 75                                                                                           | CREATE TEMP VIEW zerostep AS
  SELECT id
  FROM nodes
  WHERE label LIKE '%director%';
CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr AS x
                                          FROM tClosure
                                            JOIN zerostep ON idl = zerostep.id
                                            JOIN nodes AS n ON idr = n.id
                                          WHERE depth <= 2 AND depth >= 1) SELECT *
                                                                           FROM graphT);
SELECT n.title
FROM movie n
  JOIN step ON x = n.id
LIMIT 75;
MATCH (a)<-[*2..4]-(b) RETURN a;
| CREATE TEMP VIEW zerostep AS SELECT id FROM nodes;
CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr AS x
                                          FROM tClosure
                                            JOIN zerostep ON idl = zerostep.id
                                            JOIN nodes AS n ON idr = n.id
                                          WHERE depth <= 4 AND depth >= 2) SELECT *
                                                                           FROM graphT);
SELECT *
FROM nodes n
  JOIN step ON x = n.id;
MATCH (a)<-[*]-(b:Actor {NAME :"Tom Hanks"}) RETURN a;
| CREATE TEMP VIEW zerostep AS SELECT id FROM nodes WHERE NAME = 'tom hanks' AND LABEL LIKE '%actor%';
CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr AS x
                                          FROM tClosure
                                            JOIN zerostep ON idl = zerostep.id
                                            JOIN nodes AS n ON idr = n.id
                                          WHERE depth <= 100 AND depth >= 1) SELECT *
                                                                             FROM graphT);
SELECT *
FROM nodes n
  JOIN step ON x = n.id;
MATCH (n:Actor)-[:ACTS_IN]-(m:Movie) WHERE n.name = "Ben Miller" RETURN m.tagline ORDER BY m.title ASC;
| WITH a AS ( SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 ON n1.id = e1.idl INNER JOIN movie n2 ON e1.idr = n2.id WHERE n1.name = 'ben miller' AND n1.label LIKE '%actor%' AND n2.label LIKE '%movie%' AND e1.type = 'acts_in' UNION ALL SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 ON n1.id = e1.idr INNER JOIN movie n2 ON e1.idl = n2.id WHERE n1.name = 'ben miller' AND n1.label LIKE '%actor%' AND n2.label LIKE '%movie%' AND e1.type = 'acts_in') SELECT n.tagline FROM movie n, a WHERE n.id = a.a2 ORDER BY n.title ASC;
MATCH (a:Actor)-[]->(m:Movie)<-[]-(d:Director)-[]->(j:Movie)<--(b:Actor) where a.name = "Miley Cyrus" return distinct b.name order by b.name asc; | WITH a AS (SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 on n1.id = e1.idl INNER JOIN movie n2 on e1.idr = n2.id WHERE n1.name = 'miley cyrus' AND n1.label LIKE '%actor%'  AND n2.label LIKE '%movie%' ), b AS (SELECT n1.id AS b1, n2.id AS b2, e2.* FROM movie n1 INNER JOIN edges e2 on n1.id = e2.idr INNER JOIN nodes n2 on e2.idl = n2.id WHERE n1.label LIKE '%movie%'  AND n2.label LIKE '%director%' ), c AS (SELECT n1.id AS c1, n2.id AS c2, e3.* FROM nodes n1 INNER JOIN edges e3 on n1.id = e3.idl INNER JOIN movie n2 on e3.idr = n2.id WHERE n1.label LIKE '%director%'  AND n2.label LIKE '%movie%' ), d AS (SELECT n1.id AS d1, n2.id AS d2, e4.* FROM movie n1 INNER JOIN edges e4 on n1.id = e4.idr INNER JOIN nodes n2 on e4.idl = n2.id WHERE n1.label LIKE '%movie%'  AND n2.label LIKE '%actor%' ) SELECT DISTINCT n.name FROM nodes n, a, b, c, d  WHERE a.a2 = b.b1 AND b.b2 = c.c1 AND c.c2 = d.d1 AND n.id = d.d2 AND a.a1 != b.b2 AND a.a2 != c.c2 AND b.b2 != d.d2 ORDER BY n.name asc;
MATCH (n:Movie {title:"Love Actually"})<-[:ACTS_IN]-(x)-->(t) return t.studio order by t.releaseDate;                                             | WITH a AS (SELECT n1.id AS a1, n2.id AS a2, e1.* FROM movie n1 INNER JOIN edges e1 on n1.id = e1.idr INNER JOIN nodes n2 on e1.idl = n2.id WHERE n1.title = 'love actually' AND n1.label LIKE '%movie%'  AND e1.type = 'acts_in'), b AS (SELECT n1.id AS b1, n2.id AS b2, e2.* FROM nodes n1 INNER JOIN edges e2 on n1.id = e2.idl INNER JOIN nodes n2 on e2.idr = n2.id) SELECT n.studio FROM movie n, a, b  WHERE a.a2 = b.b1 AND n.id = b.b2 AND a.a1 != b.b2 ORDER BY n.releasedate asc;
MATCH (a)-[C {COMMENT :"Run Forrest, Run!"}]-(b) WHERE C.stars = 5 RETURN b;
| WITH a AS ( SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 ON n1.id = e1.idl INNER JOIN nodes n2 ON e1.idr = n2.id WHERE e1.comment = 'run forrest, run!' AND e1.stars = '5' UNION ALL SELECT n1.id AS a1, n2.id AS a2, e1.* FROM nodes n1 INNER JOIN edges e1 ON n1.id = e1.idr INNER JOIN nodes n2 ON e1.idl = n2.id WHERE e1.comment = 'run forrest, run!' AND e1.stars = '5') SELECT n.* FROM nodes n, a WHERE n.id = a.a2;
MATCH (n:Movie) WHERE n.runtime > 140 AND n.studio = "Warner Bros. Pictures" RETURN n.title ORDER BY n.title ASC;
| SELECT n.title FROM movie n WHERE n.runtime > '140' AND n.studio = 'warner bros. pictures' ORDER BY n.title ASC;