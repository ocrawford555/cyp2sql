package query_translation;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class InterToSQLNodesEdges {
    private static final char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private static ArrayList<CypNode> varRelNodes = new ArrayList<>();

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
                sql = obtainViews(sql, matchC);
                sql.append(" SELECT name FROM nodes n inner join x on z = id ");
                return sql;
            }

            sql = obtainWithClause(sql, matchC);

            sql = obtainSelectAndFromClause(returnC, matchC, sql, hasDistinct, alias);

            sql = obtainWhereClause(sql, returnC, matchC);

            return sql;
        }
    }

    private static StringBuilder obtainViews(StringBuilder sql, MatchClause matchC) {
        int varRelsIndex = 0;

        for (CypRel r : matchC.getRels()) {
            if (r.getDirection().contains("var")) {
                int highPathSearch;
                String varDirection = "none";

                if (r.getDirection().contains("-")) {
                    String[] parts = r.getDirection().split("-");
                    //lowerPathSearch = Integer.parseInt(parts[0].substring(3));
                    String[] partsB = parts[1].split("#");
                    highPathSearch = Integer.parseInt(partsB[0]);
                    varDirection = partsB[1];
                } else {
                    String[] parts = r.getDirection().split("#");
                    highPathSearch = Integer.parseInt(parts[0].substring(3));
                    varDirection = parts[1];
                }

                System.out.println(varDirection);

                int posOfRelInClause = r.getPosInClause();
                CypNode n1 = matchC.getNodes().get(posOfRelInClause - 1);
                CypNode n2 = matchC.getNodes().get(posOfRelInClause);

                // need to choose the best node to initially expand to make the queries faster and
                // not destroy my laptop.

                int valueOfNode1 = ((n1.getProps() != null) ? 2 : 0) + ((n1.getType() != null) ? 1 : 0);
                int valueOfNode2 = ((n2.getProps() != null) ? 2 : 0) + ((n2.getType() != null) ? 1 : 0);

                if (valueOfNode2 > valueOfNode1) {
                    CypNode nTemp = n2;
                    int tempValue = valueOfNode2;
                    valueOfNode2 = valueOfNode1;
                    valueOfNode1 = tempValue;
                    n2 = n1;
                    n1 = nTemp;
                    if (varDirection.equals("right")) varDirection = "left";
                    else if (varDirection.equals("left")) varDirection = "right";
                }

                // add the other node to this global variable so that it can be accessed
                // later on in the query (if it will be useful, hence the equality check
                // against 0).
                if (valueOfNode2 != 0) varRelNodes.add(n2);

                int j = 0;

                sql = getGraphRecursiveQuery(sql);
                sql = getInitialVarView(j, n1, sql);

                while (j < highPathSearch) {
                    j++;
                    sql.append(" CREATE TEMP VIEW v").append(j).append(" AS (WITH a AS (SELECT ");

                    if (varDirection.equals("none") || varDirection.equals("right")) sql.append("idr ");
                    else sql.append("idl ");

                    sql.append("as z FROM gMain WHERE ");

                    if (varDirection.equals("none") || varDirection.equals("right")) sql.append("idl ");
                    else sql.append("idr ");

                    sql.append("in (select * from v").append(j - 1).append(") ");
                    if (j == 1) sql.append("and depth = 1");

                    if (varDirection.equals("none")) {
                        sql.append(" UNION ALL SELECT idl as z from gMain");
                        sql.append(" WHERE idr in (select * from v").append(j - 1).append(")");
                        if (j == 1) sql.append(" AND depth = 1");
                    }

                    sql.append(") SELECT * from a where z != (select * from v0));");
                }

                sql.append(" WITH x AS (SELECT z from v1");

                for (int k = 0; k < j - 1; k++) {
                    sql.append(" UNION SELECT z FROM v").append(k + 2);
                }
                sql.append(")");
            }
        }

        return sql;
    }

    private static StringBuilder getGraphRecursiveQuery(StringBuilder sql) {
        return sql.append("CREATE TEMP VIEW gMain AS (WITH RECURSIVE search_graph(idl, idr, depth, path, cycle) AS (" +
                " SELECT e.idl, e.idr, 1," +
                " ARRAY[e.idl]," +
                " false" +
                " FROM edges e" +
                " UNION ALL" +
                " SELECT e.idl, e.idr, sg.depth + 1," +
                " path || e.idl," +
                " e.idl = ANY(path)" +
                " FROM edges e, search_graph sg" +
                " WHERE e.idl = sg.idr AND NOT cycle" +
                ") " +
                "SELECT * FROM search_graph); ");
    }

    private static StringBuilder getInitialVarView(int j, CypNode n1, StringBuilder sql) {
        sql.append("CREATE TEMP VIEW v").append(j).append(" as (SELECT id FROM Nodes n");
        if (n1.getType() != null) {
            sql.append(" WHERE n.label='").append(n1.getType()).append("'");
        }
        if (n1.getProps() != null) {
            if (n1.getType() != null) sql.append(" AND ");
            else sql.append(" WHERE ");

            JsonObject obj = n1.getProps();
            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                sql.append("n.").append(entry.getKey()).append(" = '");
                sql.append(entry.getValue().getAsString()).append("' AND ");
            }

            sql.setLength(sql.length() - 5);
        }

        sql.append(");");
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
}
