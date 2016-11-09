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

        sql = obtainMatchAndReturn(decodedQuery.getMc(), decodedQuery.getRc(), sql,
                decodedQuery.getCypherAdditionalInfo().hasDistinct(),
                decodedQuery.getCypherAdditionalInfo().getAliasMap());

        if (decodedQuery.getCypherAdditionalInfo().hasCount())
            sql = obtainGroupByClause(sql);

        if (decodedQuery.getOc() != null)
            sql = obtainOrderByClause(decodedQuery.getOc(), sql);

        int skipAmount = decodedQuery.getSkipAmount();
        int limitAmount = decodedQuery.getLimitAmount();

        if (skipAmount != -1) sql.append(" OFFSET ").append(skipAmount);
        if (limitAmount != -1) sql.append(" LIMIT ").append(limitAmount);

        sql.append(";");

        System.out.println(sql.toString());

        return sql.toString();
    }

    private static StringBuilder obtainGroupByClause(StringBuilder sql) {
        sql.append(" GROUP BY ");
        sql.append("n").append(".").append("id, ");
        sql.append("n").append(".").append("name, ");
        sql.append("n").append(".").append("label");
        return sql;
    }

    private static StringBuilder obtainMatchAndReturn(MatchClause matchC, ReturnClause returnC,
                                                      StringBuilder sql, boolean hasDistinct,
                                                      Map<String, String> alias) throws Exception {
        if (returnC.getItems() == null)
            throw new Exception("NOTHING SPECIFIED TO RETURN");

        if (matchC.getRels().isEmpty()) {
            // no relationships, just return some nodes
            sql = obtainSelectAndFromClause(returnC, matchC, sql, hasDistinct, alias);

            sql = obtainWhereClauseOnlyNodes(sql, returnC, matchC);
            return sql;
        } else {
            // there are relationships to deal with, so use the WITH structure

            // furthermore, if there ae variable path ones, do something clever.
            if (matchC.isVarRel()) {
                // assuming only one car relation for time being.
                sql = obtainVarRel(sql, matchC, returnC);
                return sql;
            }

            sql = obtainWithClause(sql, matchC);

            sql = obtainSelectAndFromClause(returnC, matchC, sql, hasDistinct, alias);

            sql = obtainWhereClause(sql, returnC, matchC);

            return sql;
        }
    }

    private static StringBuilder obtainVarRel(StringBuilder sql, MatchClause matchC, ReturnClause returnC) {
        sql.append(getTClosureQuery()).append(" ");
        String direction = "none";
        int amount = 0;
        for (CypRel cR : matchC.getRels()) {
            if (cR.getDirection().contains("var")) {
                String dirAndAmount[] = cR.getDirection().split("#");
                direction = dirAndAmount[1];
                amount = Integer.parseInt(dirAndAmount[0].split("-")[1]);
            }
        }
        CypNode cN;
        if (direction.equals("left")) {
            cN = matchC.getNodes().get(1);
            sql.append(getZeroStep(cN));
        }
        if (direction.equals("right")) {
            cN = matchC.getNodes().get(0);
            sql.append(getZeroStep(cN));
        }
        sql.append(" ");
        sql = createStepView(sql, amount);
        return sql;
    }

    private static StringBuilder createStepView(StringBuilder sql, int amount) {
        sql.append("CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr as x");
        sql.append(" FROM tClosure JOIN zerostep on idl = zerostep.id");
        sql.append(" JOIN nodes as n on idr = n.id where depth <=");
        sql.append(amount).append(") SELECT * from graphT); ");
        sql.append("SELECT * FROM nodes JOIN step on x = id");
        return sql;
    }

    private static StringBuilder obtainWhereClauseOnlyNodes(StringBuilder sql, ReturnClause returnC, MatchClause matchC) {
        boolean hasWhere = false;
        for (CypReturn cR : returnC.getItems()) {
            if (cR.getNodeID() == null && cR.getField().equals("*")) {
                CypNode cN = matchC.getNodes().get(0);
                hasWhere = true;
                sql.append(" WHERE n.label = '").append(cN.getType()).append("' AND ");
                if (cN.getProps() != null) {
                    JsonObject obj = cN.getProps();
                    Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
                    for (Map.Entry<String, JsonElement> entry : entries) {
                        sql.append("n.").append(entry.getKey()).append(" = '");
                        sql.append(entry.getValue().getAsString()).append("' AND ");
                    }
                }
                break;
            }

            CypNode cN = null;
            for (CypNode c : matchC.getNodes()) {
                if (c.getId().equals(cR.getNodeID()))
                    cN = c;
            }

            if (cN != null) {
                if (cN.getType() != null) {
                    if (!hasWhere) {
                        sql.append(" WHERE ");
                        hasWhere = true;
                    }
                    sql.append("n.label = '").append(cN.getType()).append("' AND ");
                    if (cN.getProps() != null) {
                        JsonObject obj = cN.getProps();
                        Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
                        for (Map.Entry<String, JsonElement> entry : entries) {
                            sql.append("n.").append(entry.getKey()).append(" = '");
                            sql.append(entry.getValue().getAsString()).append("' AND ");
                        }
                    }
                }
            }
        }

        if (hasWhere) {
            sql.setLength(sql.length() - 5);
        }
        return sql;
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
                    sql = obtainWhereInWithClause(cR, matchC, sql, false);
                    break;
                case "left":
                    sql.append(" FROM nodes n1 " +
                            "INNER JOIN edges e1 on n1.id = e1.idr " +
                            "INNER JOIN nodes n2 on e1.idl = n2.id");
                    sql = obtainWhereInWithClause(cR, matchC, sql, false);
                    break;
                case "none":
                    sql.append(" FROM nodes n1 " +
                            "INNER JOIN edges e1 on n1.id = e1.idl " +
                            "INNER JOIN nodes n2 on e1.idr = n2.id");
                    sql = obtainWhereInWithClause(cR, matchC, sql, true);
                    sql.append("SELECT n1.id AS ").append(withAlias).append(1).append(", ");
                    sql.append("n2.id AS ").append(withAlias).append(2);
                    sql.append(" FROM nodes n1 " +
                            "INNER JOIN edges e1 on n1.id = e1.idr " +
                            "INNER JOIN nodes n2 on e1.idl = n2.id");
                    sql = obtainWhereInWithClause(cR, matchC, sql, false);
                    break;
            }

            indexRel++;
        }

        sql.setLength(sql.length() - 2);
        sql.append(" ");
        return sql;
    }

    private static StringBuilder obtainWhereInWithClause(CypRel cR, MatchClause matchC, StringBuilder sql,
                                                         boolean isBiDirectional) {
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
                sql = addWhereClause(sql, entry);
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
                sql = addWhereClause(sql, entry);
            }
        }

        if (leftNode.getType() != null) {
            if (!includesWhere) {
                sql.append(" WHERE ");
                includesWhere = true;
            }
            sql.append("n1.label = '");
            sql.append(leftNode.getType()).append("' AND ");
        }

        if (rightNode.getType() != null) {
            if (!includesWhere) {
                sql.append(" WHERE ");
                includesWhere = true;
            }
            sql.append("n2.label = '");
            sql.append(rightNode.getType()).append("' AND ");
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
        if (isBiDirectional) {
            sql.append(" UNION ALL ");
        } else {
            sql.append("), ");
        }
        return sql;
    }

    private static StringBuilder addWhereClause(StringBuilder sql, Map.Entry<String, JsonElement> entry) {
        String value = entry.getValue().getAsString();
        if (value.startsWith("<#") && value.endsWith("#>")) {
            sql.append(" <> ");
            value = value.substring(2, value.length() - 2);
            System.out.println(value);
        } else {
            sql.append(" = ");
        }
        sql.append("'").append(value.replace("'", ""));
        sql.append("' AND ");
        return sql;
    }

    private static StringBuilder obtainSelectAndFromClause(ReturnClause returnC, MatchClause matchC,
                                                           StringBuilder sql, boolean hasDistinct,
                                                           Map<String, String> alias)
            throws Exception {
        sql.append("SELECT ");
        if (hasDistinct) sql.append("DISTINCT ");

        boolean usesNodesTable = false;
        boolean usesRelsTable = false;

        for (CypReturn cR : returnC.getItems()) {
            boolean isNode = false;

            if (cR.getNodeID() == null && cR.getField().equals("*")) {
                sql.append("*  ");
                usesNodesTable = true;
                break;
            }

            for (CypNode cN : matchC.getNodes()) {
                if (cR.getNodeID().equals(cN.getId())) {
                    String prop = cR.getField();
                    if (prop != null) {
                        sql.append("n").append(".").append(prop).append(useAlias(cR.getNodeID(), alias)).append(", ");
                    } else {
                        sql.append("n.*").append(useAlias(cR.getNodeID(), alias)).append(", ");
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

    private static String useAlias(String nodeID, Map<String, String> alias) {
        System.out.println("NODE ID IN ALIAS : " + nodeID);
        if (alias.isEmpty()) {
            return "";
        } else {
            for (String s : alias.keySet()) {
                String id = s.split("\\.")[0];
                if (id.equals(nodeID)) {
                    return (" AS " + alias.get(s));
                }
            }
        }
        return "";
    }

    private static StringBuilder obtainWhereClause(StringBuilder sql,
                                                   ReturnClause returnC, MatchClause matchC) throws Exception {
        boolean hasWhereKeyword = false;
        int numRels = matchC.getRels().size();

        if (numRels > 0) {
            if ((numRels == 1) && matchC.getRels().get(0).getDirection().equals("none")) {
                int posInCl = returnC.getItems().get(0).getPosInClause();
                if (posInCl == 1) return sql.append(" WHERE n.id = a.a1");
                else return sql.append("WHERE n.id = a.a2");
            }

            sql.append(" WHERE ");
            hasWhereKeyword = true;

            for (int i = 0; i < numRels - 1; i++) {
                sql.append(alphabet[i]).append(".").append(alphabet[i]).append(2);
                sql.append(" = ");
                sql.append(alphabet[i + 1]).append(".").append(alphabet[i + 1]).append(1);
                sql.append(" AND ");
            }
        }

        for (CypReturn cR : returnC.getItems())
            if (cR.getType() != null)
                switch (cR.getType()) {
                    case "node":
                        if (!hasWhereKeyword) {
                            sql.append(" WHERE ");
                            hasWhereKeyword = true;
                        }

                        int posInClause = cR.getPosInClause();
                        sql.append("n.id = ");
                        if (posInClause == 1) {
                            sql.append("a.a1");
                            sql.append(" AND ");
                        } else if (posInClause == 2) {
                            sql.append("a.a2");
                            sql.append(" AND ");
                        } else if (posInClause == 3) {
                            sql.append("b.b2");
                            sql.append(" AND ");
                        } else if (posInClause == 4) {
                            sql.append("c.c2");
                            sql.append(" AND ");
                        }
                        break;
                    default:
                        break;
                }

        if (numRels > 1) {
            for (int i = 0; i < numRels - 1; i++) {
                if (i == 0) {
                    sql.append("a.a1 != b.b2");
                } else {
                    sql.append(alphabet[i - 1]).append(".").append(alphabet[i - 1]).append(2);
                    sql.append(" != ");
                    sql.append(alphabet[i + 1]).append(".").append(alphabet[i + 1]).append(2);
                }
                sql.append(" AND ");
            }
        }

        if (sql.toString().endsWith(" AND ")) sql.setLength(sql.length() - 5);

        return sql;
    }

    private static StringBuilder obtainOrderByClause(OrderClause orderC, StringBuilder sql) {
        sql.append(" ");
        sql.append("ORDER BY ");
        for (CypOrder cO : orderC.getItems()) {
            sql.append("n").append(".").append(cO.getField()).append(" ").append(cO.getAscOrDesc());
            sql.append(", ");
        }
        sql.setLength(sql.length() - 2);
        return sql;
    }

    private static CypNode obtainNode(MatchClause matchC, int i) {
        for (CypNode c : matchC.getNodes()) {
            if (c.getPosInClause() == i) {
                return c;
            }
        }
        return null;
    }

    private static String getTClosureQuery() {
        return "CREATE TEMP VIEW tclosure AS(WITH RECURSIVE search_graph(idl, idr, depth, path, cycle) AS (" +
                "SELECT e.idl, e.idr, 1," +
                " ARRAY[e.idl]," +
                " false" +
                " FROM edges e" +
                " UNION ALL" +
                " SELECT sg.idl, e.idr, sg.depth + 1," +
                " path || e.idl," +
                " e.idl = ANY(sg.path)" +
                " FROM edges e, search_graph sg" +
                " WHERE e.idl = sg.idr AND NOT cycle" +
                ") " +
                "SELECT * FROM search_graph);";
    }

    public static String getZeroStep(CypNode cN) {
        String getZStep = "CREATE TEMP VIEW zerostep AS" +
                " (SELECT id from nodes";
        JsonObject obj = cN.getProps();
        if (obj != null) {
            getZStep += " WHERE ";
            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                getZStep = getZStep + entry.getKey() + " = ";
                getZStep = getZStep + "'" + entry.getValue().getAsString() + "'";
                getZStep = getZStep + " AND ";
            }
            getZStep = getZStep.substring(0, getZStep.length() - 5);
        }

        getZStep = getZStep + ");";
        System.out.println(getZStep);
        return getZStep;
    }
}
