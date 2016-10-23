package main_area;

import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import query_translation.InterToSQL;
import query_translation.InterToSQLNodesEdges;
import toolv1.CypherTokenizer;

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
            return sqlFromCypher;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    static void cypherOutputToTextFile(String cypherQuery) throws Exception {
        CypherDriver.run(cypherQuery);
    }

    static void runSQL(String sql) throws Exception {
        DbUtil.select(sql);
    }
}
