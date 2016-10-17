package query_translation;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

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
            sql = obtainSelectAndFromClause(returnC, matchC, sql);

            sql = obtainWhereClause(matchC, sql, false, returnC);

            return sql;
        } else {
            // there are relationships to deal with, so use the WITH structure
            sql = obtainWithClause(sql, matchC);

            sql = obtainSelectAndFromClause(returnC, matchC, sql);

            sql = obtainWhereClause(matchC, sql, true, returnC);

            return sql;
        }
    }

    private static StringBuilder obtainWithClause(StringBuilder sql, MatchClause matchC) {
        sql.append("WITH ");
        int indexRel = 0;

        for (CypRel cR : matchC.getRels()) {
            String withAlias = String.valueOf(alphabet[indexRel]);
            sql.append(withAlias).append(" AS ");
            sql.append("(SELECT n1.id AS ").append(withAlias).append(indexRel + 1);
            sql.append(" FROM nodes n1 " +
                    "INNER JOIN edges e1 on n1.id = e1.idl " +
                    "INNER JOIN nodes n2 on e1.idr = n2.id ");

            boolean includesWhere = false;
            int posOfRel = cR.getPosInClause();

            CypNode leftNode = obtainNode(matchC, posOfRel);
            JsonObject leftProps = leftNode.getProps();
            CypNode rightNode = obtainNode(matchC, posOfRel + 1);
            JsonObject rightProps = rightNode.getProps();
            String typeRel = cR.getType();

            if (leftProps != null) {
                sql.append(" WHERE ");
                includesWhere = true;

                Set<Map.Entry<String, JsonElement>> entrySet = leftProps.entrySet();
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    sql.append("n1").append(".").append(entry.getKey());
                    sql.append("='").append(entry.getValue().getAsString());
                    sql.append("' AND ");
                }
            }

            if (rightProps != null) {
                if (!includesWhere) {
                    sql.append(" WHERE ");
                    includesWhere = true;
                }

                Set<Map.Entry<String, JsonElement>> entrySet = rightProps.entrySet();
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    sql.append("n2").append(".").append(entry.getKey());
                    sql.append("='").append(entry.getValue().getAsString());
                    sql.append("' AND ");
                }
            }

            if (typeRel != null) {
                if (!includesWhere) {
                    sql.append(" WHERE ");
                    includesWhere = true;
                }

                sql.append("e1.type = '").append(typeRel);
                sql.append("' AND ");
            }

            if (includesWhere) sql.setLength(sql.length() - 5);
            sql.append("), ");
            indexRel++;
        }

        sql.setLength(sql.length() - 2);
        sql.append(" ");
        return sql;
    }

    private static StringBuilder obtainSelectAndFromClause(ReturnClause returnC, MatchClause matchC,
                                                           StringBuilder sql)
            throws Exception {
        sql.append("SELECT ");
        boolean usesNodesTable = false;
        boolean usesRelsTable = false;
        for (CypReturn cR : returnC.getItems()) {
            boolean isNode = false;
            for (CypNode cN : matchC.getNodes()) {
                if (cR.getNodeID().equals(cN.getId())) {
                    String prop = cR.getField();
                    if (prop != null) {
                        sql.append("n").append(prop).append(", ");
                    } else {
                        sql.append("n.*").append(", ");
                    }
                    isNode = true;
                    usesNodesTable = true;
                    break;
                }
            }

            if (!isNode) {
                for (CypRel cRel : matchC.getRels()) {
                    //TODO: sort out returning relationships.
                }
            }
        }

        sql.setLength(sql.length() - 2);

        sql.append(" FROM ");
        if (usesNodesTable) sql.append("nodes n, ");
        int numRels = matchC.getRels().size();
        for (int i = 0; i < numRels; i++)
            sql.append(alphabet[i]).append(", ");

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
