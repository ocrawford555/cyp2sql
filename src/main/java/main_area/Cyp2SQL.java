package main_area;

import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import query_translation.InterToSQL;
import query_translation.InterToSQLNodesEdges;
import toolv1.CypherTokenizer;

import java.sql.SQLException;

class Cyp2SQL {
    static String convertQuery(String q) {
        try {
            DecodedQuery decodedQuery = CypherTokenizer.decode(q);

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
            return sqlFromCypher;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    static void cypherOutputToTextFile(String cypherQuery) throws Exception {
        CypherDriver.run(cypherQuery);
    }

    public static void printPostgresToTextFile(String sql) throws SQLException {
        DbUtil.createConnection("testa");
        DbUtil.select(sql);
        DbUtil.closeConnection();
    }
}
