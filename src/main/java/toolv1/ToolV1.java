package toolv1;

import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import database.SQLDriver;
import query_translation.InterToSQL;
import query_translation.InterToSQLNodesEdges;

public class ToolV1 {
    public static void main(String[] args) throws Exception {
        //Map<String, String> schemaToBuild =
//                SchemaTranslate.readFile(
//                        "C:/Users/ocraw/Documents/Year 3 Cambridge/Project/CypherStuff/database/dump4.txt",
//                        true,
//                        2);
        try {
            DbUtil.createConnection("testa");

            String cyherQuery = "match (n)-[]-(a:Player) where a.name = \"Wayne Rooney\" return count(*)";

            String plsWork = "match (a:NationalTeam)--(b)--(n:Club) return distinct n order by n.name asc;";

            String rooneyQuery = "match (n:Player {name:\"Wayne Rooney\"})-->(x) return x;";

            String twoDirQuery = "match (n:Player {name:\"Wayne Rooney\"})-->(x)<--(p:Player) return p";

            String threeDirWithDistinct = "match (n:Player {name:\"Wayne Rooney\"})-->(x)<--(p:Player)-->(a:Club) " +
                    "return distinct a;";

            String otherQuery = "match (n:Player {name:\"Wayne Rooney\"})-->(x)<--(b)-->(c:NationalTeam) " +
                    "return c;";

            String moreTesting = "match (n:Club)<--(x)-->(b:NationalTeam)<--(a:Player {name:\"Gareth Bale\"}) " +
                    "return distinct n order by n.name asc skip 2 limit 3;";

            String biDir = "match(n:Club)--(x:Player) return n";

//            WITH a AS (SELECT n1.id AS a1, n2.id AS a2 FROM nodes n1
//                    INNER JOIN edges e1 on n1.id = e1.idl
//                    INNER JOIN nodes n2 on e1.idr = n2.id
//                    WHERE n1.NAME='Wayne Rooney'),
//                    b AS (SELECT n1.id AS b1, n2.id AS b2 FROM nodes n1
//                    INNER JOIN edges e1 on n1.id = e1.idr
//                    INNER JOIN nodes n2 on e1.idl = n2.id)
//
//            SELECT n.* from nodes n, b,a where a.a2 = b.b1 and n.id = b.b2 and n.id != a.a1;

            //select * from nodes n1 inner join edges e1 on n1.id = e1.idl inner join nodes n2 on e1.idr = n2.id
            // where n2.name = 'Manchester United' and e1.type = 'PLAYS_FOR';

            //CypherDriver.run(cyherQuery);

            DecodedQuery decodedQuery = CypherTokenizer.decode(cyherQuery);

            int typeSchemaRunningAgainst = 2;
            String sqlFromCypher = null;

            switch (typeSchemaRunningAgainst) {
                case 1:
                    sqlFromCypher = InterToSQL.translate(decodedQuery);
                    break;
                case 2:
                    sqlFromCypher = InterToSQLNodesEdges.translate(decodedQuery);
                    break;
            }

            sqlFromCypher = sqlFromCypher.replace("Nationalteam", "NationalTeam");
            System.out.println(sqlFromCypher);
            //SQLDriver.run(sqlFromCypher);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DbUtil.closeConnection();
        }
    }
}
