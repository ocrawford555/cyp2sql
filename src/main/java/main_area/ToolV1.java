package main_area;

import clauseObjects.DecodedQuery;
import database.DbUtil;
import query_translation.InterToSQLNodesEdges;
import toolv1.CypherTokenizer;

public class ToolV1 {
    public static void main(String[] args) throws Exception {
//        Map<String, String> schemaToBuild =
//                SchemaTranslate.readFile(
//                        "C:/Users/ocraw/Documents/Year 3 Cambridge/Project/CypherStuff/database/dump3.txt",
//                        true,
//                        2);

        String cQuery = "match (n:Club {name:\"Sunderland AFC\"})-[*1..2]-(x) return x order by x.name asc";
        DecodedQuery dQ = CypherTokenizer.decode(cQuery);
        String sqlFromCypher = InterToSQLNodesEdges.translate(dQ);
        System.out.println(sqlFromCypher);

        DbUtil.createConnection("testa");
        String seperateQueries[] = sqlFromCypher.split(";");
        for (String s : seperateQueries) {
            s = s.trim() + ";";
            if (s.startsWith("CREATE TEMP VIEW")) {
                DbUtil.executeCreateView(s);
            } else {
                DbUtil.select(s, "testa");
            }
        }
        DbUtil.closeConnection();
//
//        DbUtil.createConnection("testa");
//        DbUtil.select(sqlFromCypher);
//        DbUtil.closeConnection();

//        try {
//            DbUtil.createConnection("testa");
//
//            //DbUtil.createAndInsert(schemaToBuild);
//
//            String cyherQuery = "Match (x:Club)<-[:PLAYS_FOR]-(a:Player) return x, count(a);";
//
//            if (args.length != 0)
//                cyherQuery = args[0];
//
////            WITH a AS (SELECT n1.id AS a1, n2.id AS a2 FROM nodes n1
////                    INNER JOIN edges e1 on n1.id = e1.idl
////                    INNER JOIN nodes n2 on e1.idr = n2.id
////                    WHERE n1.NAME='Wayne Rooney'),
////                    b AS (SELECT n1.id AS b1, n2.id AS b2 FROM nodes n1
////                    INNER JOIN edges e1 on n1.id = e1.idr
////                    INNER JOIN nodes n2 on e1.idl = n2.id)
////
////            SELECT n.* from nodes n, b,a where a.a2 = b.b1 and n.id = b.b2 and n.id != a.a1;
//
//            //select * from nodes n1 inner join edges e1 on n1.id = e1.idl inner join nodes n2 on e1.idr = n2.id
//            // where n2.name = 'Manchester United' and e1.type = 'PLAYS_FOR';
//
//            //CypherDriver.run(cyherQuery);
//
//            DecodedQuery decodedQuery = CypherTokenizer.decode(rooneyQuery);
//
//            int typeSchemaRunningAgainst = 2;
//            String sqlFromCypher = null;
//
//            switch (typeSchemaRunningAgainst) {
//                case 1:
//                    sqlFromCypher = InterToSQL.translate(decodedQuery);
//                    break;
//                case 2:
//                    sqlFromCypher = InterToSQLNodesEdges.translate(decodedQuery);
//                    break;
//            }
//
//            sqlFromCypher = sqlFromCypher.replace("Nationalteam", "NationalTeam");
//            System.out.println(sqlFromCypher);
//            DbUtil.select(sqlFromCypher);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            DbUtil.closeConnection();
//        }
    }
}
