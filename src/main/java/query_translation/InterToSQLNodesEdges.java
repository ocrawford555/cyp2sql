package query_translation;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.text.WordUtils;

import java.util.Map;
import java.util.Set;

public class InterToSQLNodesEdges {
    private static final char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static String translate(DecodedQuery decodedQuery) throws Exception {
        StringBuilder sql = new StringBuilder();

        sql = obtainMatchAndReturn(decodedQuery.getMc(), decodedQuery.getRc(), sql,
                decodedQuery.getCypherAdditionalInfo().hasDistinct());

        if (decodedQuery.getOc() != null)
            sql = obtainOrderByClause(decodedQuery.getMc(), decodedQuery.getOc(), sql);

        int skipAmount = decodedQuery.getSkipAmount();
        int limitAmount = decodedQuery.getLimitAmount();

        if (skipAmount != -1) sql.append(" OFFSET ").append(skipAmount);
        if (limitAmount != -1) sql.append(" LIMIT ").append(limitAmount);

        sql.append(";");

        return sql.toString();
    }

    private static StringBuilder obtainMatchAndReturn(MatchClause matchC, ReturnClause returnC,
                                                      StringBuilder sql, boolean hasDistinct) throws Exception {
        if (returnC.getItems() == null)
            throw new Exception("NOTHING SPECIFIED TO RETURN");

        if (matchC.getRels().isEmpty()) {
            // no relationships, just return some nodes
            sql = obtainSelectAndFromClause(returnC, matchC, sql, hasDistinct);

            sql = obtainWhereClause(sql, returnC, matchC);

            return sql;
        } else {
            // there are relationships to deal with, so use the WITH structure
            sql = obtainWithClause(sql, matchC);

            sql = obtainSelectAndFromClause(returnC, matchC, sql, hasDistinct);

            sql = obtainWhereClause(sql, returnC, matchC);

            return sql;
        }
    }

    private static StringBuilder obtainWithClause(StringBuilder sql, MatchClause matchC) {
        sql.append("WITH ");
        int indexRel = 0;

        for (CypRel cR : matchC.getRels()) {
            String withAlias = String.valueOf(alphabet[indexRel]);
            sql.append(withAlias).append(" AS ");
            sql.append("(SELECT n1.id AS ").append(withAlias).append(1).append(", ");
            sql.append("n2.id AS ").append(withAlias).append(2);

            switch (cR.getDirection()) {
                case "right":
                    sql.append(" FROM nodes n1 " +
                            "INNER JOIN edges e1 on n1.id = e1.idl " +
                            "INNER JOIN nodes n2 on e1.idr = n2.id");
                    break;
                case "left":
                    sql.append(" FROM nodes n1 " +
                            "INNER JOIN edges e1 on n1.id = e1.idr " +
                            "INNER JOIN nodes n2 on e1.idl = n2.id");
                    break;
                case "none":
                    //TODO: complete thinking for this section
                    //i.e. actually do something with logic.
            }


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
                    sql.append("='").append(WordUtils.capitalizeFully(entry.getValue().getAsString()));
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
                    sql.append("='").append(WordUtils.capitalizeFully(entry.getValue().getAsString()));
                    sql.append("' AND ");
                }
            }

            if (leftNode.getType() != null) {
                if (!includesWhere) {
                    sql.append(" WHERE ");
                    includesWhere = true;
                }
                sql.append("n1.label = '");
                sql.append(WordUtils.capitalizeFully(leftNode.getType())).append("' AND ");
            }

            if (rightNode.getType() != null) {
                if (!includesWhere) {
                    sql.append(" WHERE ");
                    includesWhere = true;
                }
                sql.append("n2.label = '");
                sql.append(WordUtils.capitalizeFully(rightNode.getType())).append("' AND ");
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
                                                           StringBuilder sql, boolean hasDistinct)
            throws Exception {
        sql.append("SELECT ");
        if (hasDistinct) sql.append("DISTINCT ");
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

    private static StringBuilder obtainWhereClause(StringBuilder sql, ReturnClause returnC, MatchClause matchC) throws Exception {
        boolean hasWhereKeyword = false;

        int numRels = matchC.getRels().size();
        if (numRels > 0) {
            sql.append(" WHERE ");
            hasWhereKeyword = true;
            for (int i = 0; i < numRels - 1; i++) {
                sql.append(alphabet[i]).append(".").append(alphabet[i]).append(2);
                sql.append(" = ");
                sql.append(alphabet[i + 1]).append(".").append(alphabet[i + 1]).append(1);
                sql.append(" AND ");
            }
        }

        for (CypReturn cR : returnC.getItems()) {
            switch (cR.getType()) {
                case "node":
                    if (!hasWhereKeyword) {
                        sql.append(" WHERE ");
                        hasWhereKeyword = true;
                    }

                    sql.append("n.id = ");
                    int posInClause = cR.getPosInClause();
                    if (posInClause == 1) {
                        sql.append("a.a1");
                        sql.append(" AND ");
                    } else if (posInClause == 2) {
                        sql.append("b.b1");
                        sql.append(" AND ");
                    } else if (posInClause == 3) {
                        sql.append("b.b2");
                        sql.append(" AND ");
                    } else if (posInClause == 4) {
                        sql.append("c.c2");
                        sql.append(" AND ");
                    }
            }
        }

//        if (numRels > 1) {
//            sql.append("n.id != a.a1");
//            sql.append(" AND ");
//        }

        if (hasWhereKeyword) sql.setLength(sql.length() - 5);

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
