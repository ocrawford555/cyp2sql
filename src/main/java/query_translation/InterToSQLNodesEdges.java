package query_translation;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import toolv1.GenerateAlias;

import java.util.Map;
import java.util.Set;

public class InterToSQLNodesEdges {
    private static final char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static String translate(DecodedQuery decodedQuery) throws Exception {
        StringBuilder sql = new StringBuilder();

        sql = obtainMatchAndReturn(decodedQuery.getMc(), decodedQuery.getRc(), sql);

        sql = obtainOrderByClause(decodedQuery.getMc(), decodedQuery.getOc(), sql);

        int skipAmount = decodedQuery.getSkipAmount();
        int limitAmount = decodedQuery.getLimitAmount();

        if (skipAmount != -1) sql.append(" OFFSET ").append(skipAmount);
        if (limitAmount != -1) sql.append(" LIMIT ").append(limitAmount);

        sql.append(";");

        return sql.toString();
    }

    private static StringBuilder obtainMatchAndReturn(MatchClause matchC, ReturnClause returnC,
                                                      StringBuilder sql) throws Exception {
        if (returnC.getItems() == null)
            throw new Exception("NOTHING SPECIFIED TO RETURN");

        if (matchC.getRels().isEmpty()) {
            // no relationships, just return some nodes
            sql = obtainSelectClause(returnC, matchC, sql);

            sql = obtainFromClause(matchC, returnC, sql);

            sql = obtainWhereClause(matchC, sql, false, returnC);

            return sql;
        } else {
            // there are relationships to deal with, so use the WITH structure
            sql = obtainWithClause(sql, matchC);

            sql = obtainSelectClause(returnC, matchC, sql);

            sql = obtainFromClause(matchC, returnC, sql);

            sql = obtainWhereClause(matchC, sql, true, returnC);

            return sql;
        }
    }

    private static StringBuilder obtainWithClause(StringBuilder sql, MatchClause matchC) {
        sql.append("WITH ");
        int indexRel = 0;

        for (CypRel cR : matchC.getRels()) {
            String withAlias = String.valueOf(alphabet[indexRel]);
            sql.append(withAlias).append(" AS (SELECT ");

            boolean includesWhere = false;
            int posOfRel = cR.getPosInClause();
            String relGenID = GenerateAlias.gen();

            CypNode leftNode = obtainNode(matchC, posOfRel);
            JsonObject leftProps = leftNode.getProps();
            CypNode rightNode = obtainNode(matchC, posOfRel + 1);
            JsonObject rightProps = rightNode.getProps();

            String[] tableA = leftNode.getAlias();
            String[] tableB = rightNode.getAlias();

            sql.append(tableA[1]).append(".id AS ").
                    append(withAlias).append(1).append(", ");

            sql.append(tableB[1]).append(".id AS ").
                    append(withAlias).append(2).append(" FROM ");

            if (cR.getDirection().equals("left")) {
                // flip the tables around to suit the direction
                String tempTable[] = tableB;
                tableB = tableA;
                tableA = tempTable;
            }

            sql.append(tableA[0]).append(" ").append(tableA[1]);

            sql.append(" INNER JOIN ");
            sql.append(cR.getType()).append(" ").append(relGenID).append(" ON ");
            sql.append(tableA[1]).append(".id = ").append(relGenID).append(".idl INNER JOIN ");
            sql.append(tableB[0]).append(" ").append(tableB[1]);
            sql.append(" ON ").append(relGenID).append(".idr = ").append(tableB[1]).append(".id");

            if (leftProps != null) {
                sql.append(" WHERE ");
                includesWhere = true;

                String table;

                if (cR.getDirection().equals("left"))
                    table = tableB[1];
                else
                    table = tableA[1];

                Set<Map.Entry<String, JsonElement>> entrySet = leftProps.entrySet();
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    sql.append(table).append(".").append(entry.getKey());
                    sql.append("='").append(entry.getValue().getAsString());
                    sql.append("' AND ");
                }
            }

            if (rightProps != null) {
                if (!includesWhere) {
                    sql.append(" WHERE ");
                    includesWhere = true;
                }

                String table;

                if (cR.getDirection().equals("left"))
                    table = tableA[1];
                else
                    table = tableB[1];

                Set<Map.Entry<String, JsonElement>> entrySet = rightProps.entrySet();
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    sql.append(table).append(".").append(entry.getKey());
                    sql.append("='").append(entry.getValue().getAsString());
                    sql.append("' AND ");
                }
            }

            if (includesWhere) sql.setLength(sql.length() - 5);
            sql.append("), ");
            indexRel++;
        }

        sql.setLength(sql.length() - 2);
        sql.append(" ");
        return sql;
    }

    private static StringBuilder obtainSelectClause(ReturnClause returnC, MatchClause matchC,
                                                    StringBuilder sql)
            throws Exception {
        sql.append("SELECT ");
        for (CypReturn cR : returnC.getItems()) {
            String prop = cR.getField();
            String table[] = obtainTable(cR.getNodeID(), matchC);
            sql.append(table[1]).append(".");
            if (prop != null)
                sql.append(prop);
            else
                sql.append("*");
            sql.append(", ");
        }

        sql.setLength(sql.length() - 2);
        return sql;
    }

    private static StringBuilder obtainFromClause(MatchClause matchC, ReturnClause returnC,
                                                  StringBuilder sql)
            throws Exception {
        sql.append(" FROM ");

        for (CypReturn ret : returnC.getItems()) {
            String table[] = obtainTable(ret.getNodeID(), matchC);
            sql.append(table[0]).append(" ").append(table[1]).append(", ");
        }

        if (!matchC.getRels().isEmpty()) {
            // deal with any relationships
            int numRels = matchC.getRels().size();
            for (int i = 0; i < numRels; i++)
                sql.append(alphabet[i]).append(", ");
        }

        sql.setLength(sql.length() - 2);

        return sql;
    }

    private static StringBuilder obtainWhereClause(MatchClause matchC, StringBuilder sql, boolean hasRel,
                                                   ReturnClause returnC) throws Exception {
        if (hasRel) {
            sql.append(" WHERE ");
            int numRels = matchC.getRels().size();
            for (int i = 0; i < numRels - 1; i++) {
                sql.append(alphabet[i]).append(".").append(alphabet[i]).append(2);
                sql.append(" = ");
                sql.append(alphabet[i + 1]).append(".").append(alphabet[i + 1]).append(1);
                sql.append(" AND ");
            }

            for (CypReturn cR : returnC.getItems()) {
                String table[] = obtainTable(cR.getNodeID(), matchC);
                int posInRel = obtainPos(matchC, cR.getNodeID());
                if (posInRel != -1) {
                    sql.append(table[1]).append(".id = ");
                    if (posInRel == 1) {
                        sql.append("a.a1 AND ");
                    } else {
                        posInRel -= 2;
                        sql.append(alphabet[posInRel]).append(".").append(alphabet[posInRel]).append(2);
                        sql.append(" AND ");
                    }
                }
            }

            sql.setLength(sql.length() - 5);

        } else {
            boolean whereClause = false;

            for (CypNode cN : matchC.getNodes()) {
                if (cN.getProps() != null) {
                    if (!whereClause) {
                        sql.append(" WHERE ");
                        whereClause = true;
                    }

                    JsonObject p = cN.getProps();
                    String table = cN.getAlias()[1];

                    Set<Map.Entry<String, JsonElement>> entrySet = p.entrySet();
                    for (Map.Entry<String, JsonElement> entry : entrySet) {
                        sql.append(table).append(".").append(entry.getKey());
                        sql.append("='").append(entry.getValue().getAsString());
                        sql.append("' AND ");
                    }
                }
            }


            if (whereClause) sql.setLength(sql.length() - 5);
        }

        return sql;
    }

    private static StringBuilder obtainOrderByClause(MatchClause matchC, OrderClause orderC,
                                                     StringBuilder sql) throws Exception {
        sql.append(" ");
        sql.append("ORDER BY ");
        for (CypOrder cO : orderC.getItems()) {
            String entity = validateNodeID(cO.getNodeID(), matchC);

            if (entity != null) {
                sql.append(entity).append(".").append(cO.getField()).append(" ").append(cO.getAscOrDesc());
                sql.append(", ");
            } else
                throw new Exception("ORDER BY CLAUSE INCORRECT");
        }
        sql.setLength(sql.length() - 2);
        return sql;
    }

    private static String validateNodeID(String nodeID, MatchClause matchC) {
        for (CypNode cN : matchC.getNodes()) {
            if (nodeID.equals(cN.getId()))
                return cN.getAlias()[1];
        }
        return null;
    }

    private static int obtainPos(MatchClause matchC, String cR) {
        for (CypNode c : matchC.getNodes()) {
            if (c.getId().equals(cR)) {
                return c.getPosInClause();
            }
        }
        return -1;
    }

    private static CypNode obtainNode(MatchClause matchC, int i) {
        for (CypNode c : matchC.getNodes()) {
            if (c.getPosInClause() == i) {
                return c;
            }
        }
        return null;
    }

    private static String[] obtainTable(String nodeID, MatchClause matchC)
            throws Exception {
        for (CypNode cN : matchC.getNodes()) {
            if (nodeID.equals(cN.getId())) {
                return cN.getAlias();
            }
        }
        throw new Exception("MATCH AND RETURN CLAUSES INCOMPATIBLE");
    }

}
