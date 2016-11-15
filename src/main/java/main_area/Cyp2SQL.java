package main_area;

import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import query_translation.InterToSQLNodesEdges;
import toolv1.CypherTokenizer;

import java.sql.SQLException;

public class Cyp2SQL {
    public static String convertQuery(String q) {
        try {
            DecodedQuery decodedQuery = CypherTokenizer.decode(q, false);
            return InterToSQLNodesEdges.translate(decodedQuery);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void cypherOutputToTextFile(String cypherQuery) throws Exception {
        CypherDriver.run(cypherQuery);
    }

    public static void printPostgresToTextFile(String sql) throws SQLException {
        DbUtil.createConnection("testa");
        String seperateQueries[] = sql.split(";");
        for (String s : seperateQueries) {
            s = s.trim() + ";";
            if (s.startsWith("CREATE TEMP VIEW")) {
                DbUtil.executeCreateView(s, "testa");
            } else {
                DbUtil.select(s, "testa");
            }
        }
        DbUtil.closeConnection();
    }
}
