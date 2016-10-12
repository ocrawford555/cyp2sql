package toolv1;

import java.util.Map;

public class ToolV1 {
    public static void main(String[] args) throws Exception {
//        try {
        Map<String, String> schemaToBuild =
                SchemaTranslate.readFile(
                        "C:/Users/ocraw/Documents/Year 3 Cambridge/Project/CypherStuff/database/dump4.txt",
                        true,
                        2);
        DbUtil.createConnection("testa");

        DbUtil.createAndInsert(schemaToBuild);
        DbUtil.closeConnection();

//            String cypherQuery = "match (n:People)-[:AS_BOND_IN]->(x:Film)<-[:DIRECTOR_OF]-(d:People)-[:DIRECTOR_OF]->" +
//                    "(x2:Film)<-[:AS_BOND_IN]-(n) return n";
//
//            String qWithOrder = "match (n:Player)-[:PLAYS_FOR]->(x:Club) where x.name = \"Manchester United\" " +
//                    "return n order by n.name";
//
////        String qWithOrder = "match (n:Player)-[:PLAYS_FOR]->(x:Club {name:\"Manchester City\"}) " +
////                "return n order by n.name skip 2 limit 1";
//
//            CypherDriver.run(qWithOrder);
//
//            String sqlFromCypher = CypherTokenizer.decode(qWithOrder);
//            System.out.println(sqlFromCypher);
//            SQLDriver.run(sqlFromCypher);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            DbUtil.closeConnection();
//            System.exit(0);
//        }
    }
}
