package toolv1;

import java.sql.SQLException;
import java.util.Map;

public class ToolV1 {
    public static void main(String[] args) {
//        Map<String, String> schemaToBuild =
//               ExtractFromFile.readFile("C:/Users/ocraw/Documents/CypherStuff/Database/dump4.txt", true);
        DbUtil.createConnection("testa");

        //DbUtil.createAndInsert(schemaToBuild);
        //DbUtil.closeConnection();

        String cypherQuery = "match (n:People)-[:AS_BOND_IN]->(x:Film)<-[:DIRECTOR_OF]-(d:People)-[:DIRECTOR_OF]->" +
                "(x2:Film)<-[:AS_BOND_IN]-(n) return n";

        String qWithOrder = "match (n:Player)-[:PLAYS_FOR]->(x:Club {name:\"Manchester City\"}) return n order by n.name desc";

//
//        WITH a AS (
//                SELECT dB309b356.id AS a1, dB7607059.id AS a2
//                FROM PEOPLE dB309b356
//                INNER JOIN AS_BOND_IN dBde779c0 ON dB309b356.id = dBde779c0.idl
//                INNER JOIN FILM dB7607059 ON dBde779c0.idr = dB7607059.id
//        ),
//                b AS (
//                SELECT dB7607059.id AS b1, dB22d8dB1.id AS b2
//                FROM PEOPLE dB22d8dB1
//                INNER JOIN DIRECTOR_OF dBc69dBb6 ON dB22d8dB1.id = dBc69dBb6.idl
//                INNER JOIN FILM dB7607059 ON dBc69dBb6.idr = dB7607059.id
//        ),
//                c AS (
//                SELECT dB22d8dB1.id AS c1, dB3f4fbab.id AS c2
//                FROM PEOPLE dB22d8dB1
//                INNER JOIN DIRECTOR_OF dBafadB91 ON dB22d8dB1.id = dBafadB91.idl
//                INNER JOIN FILM dB3f4fbab ON dBafadB91.idr = dB3f4fbab.id
//        ),
//                d AS (
//                SELECT dB3f4fbab.id AS d1, dBdf8119b.id AS d2
//                FROM PEOPLE dBdf8119b
//                INNER JOIN AS_BOND_IN dBad37cfc ON dBdf8119b.id = dBad37cfc.idl
//                INNER JOIN FILM dB3f4fbab ON dBad37cfc.idr = dB3f4fbab.id
//        )
//        SELECT dB309b356.* FROM PEOPLE dB309b356, a, b, c, d
//        WHERE a.a2 = b.b1
//        AND b.b2 = c.c1
//        AND c.c2 = d.d1
//        AND dB309b356.id = a.a1
//        and a.a1 = d.d2
//        and a.a2 != d.d1;

        CypherDriver.run(qWithOrder);

        try {
            String sqlFromCypher = CypherTokenizer.decode(qWithOrder);
            System.out.println(sqlFromCypher);
            SQLDriver.run(sqlFromCypher);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DbUtil.closeConnection();
            System.exit(0);
        }
    }


}
