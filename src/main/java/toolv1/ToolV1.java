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

            String cyherQuery = "match (n:Player)-[:PLAYS_FOR]->(x:Club {name:\"Manchester United\"}) return n limit 20";

            //select * from nodes n1 inner join edges e1 on n1.id = e1.idl inner join nodes n2 on e1.idr = n2.id
            // where n2.name = 'Manchester United' and e1.type = 'PLAYS_FOR';

            CypherDriver.run(cyherQuery);

            DecodedQuery decodedQuery = CypherTokenizer.decode(cyherQuery);

            int typeSchemaRunningAgainst = 1;
            String sqlFromCypher = null;
            switch (typeSchemaRunningAgainst) {
                case 1:
                    sqlFromCypher = InterToSQL.translate(decodedQuery);
                    break;
                case 2:
                    sqlFromCypher = InterToSQLNodesEdges.translate(decodedQuery);
                    break;
            }
            System.out.println(sqlFromCypher);
            SQLDriver.run(sqlFromCypher);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DbUtil.closeConnection();
        }
    }
}
