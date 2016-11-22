package query_translation;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import production.c2sqlV1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * MAIN TRANSLATION UNIT FROM INTERMEDIATE TRANSLATION TO SQL.
 * <p>
 * Read individual method documentation for more understanding.
 */
public class InterToSQLNodesEdges {
    // alphabet allows for a consistent and logical naming approach to the intermediate parts
    // of the queries. Assumption is that # of relationships in a query has an upper bound of 26.
    private static final char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    /**
     * Translate calls other methods, stitching the results together into one SQL query.
     *
     * @param decodedQuery All the intermediate data gatahered about the original Cypher query.
     * @return SQL string that maps to the original Cypher command.
     * @throws Exception
     */
    public static String translate(DecodedQuery decodedQuery) throws Exception {
        // SQL built up from a StringBuilder object.
        StringBuilder sql = new StringBuilder();

        sql = obtainMatchAndReturn(decodedQuery.getMc(), decodedQuery.getRc(), sql,
                decodedQuery.getCypherAdditionalInfo().hasDistinct(),
                decodedQuery.getCypherAdditionalInfo().getAliasMap());

        // hasCount refers to the possibility that the Cypher query wishes to return the
        // count of something. For example, ...RETURN count(a)...
        if (decodedQuery.getCypherAdditionalInfo().hasCount())
            sql = obtainGroupByClause(decodedQuery.getRc(), sql);

        if (decodedQuery.getOc() != null)
            sql = obtainOrderByClause(decodedQuery.getOc(), sql);

        int skipAmount = decodedQuery.getSkipAmount();
        int limitAmount = decodedQuery.getLimitAmount();

        if (skipAmount != -1) sql.append(" OFFSET ").append(skipAmount);
        if (limitAmount != -1) sql.append(" LIMIT ").append(limitAmount);

        sql.append(";");
        return sql.toString();
    }

    /**
     * Appends GROUP BY clause to query. This is needed if COUNT is used.
     *
     * @param rc  Return Clause of the Cypher query.
     * @param sql Query before GROUP BY
     * @return Query after GROUP BY
     * @throws IOException
     */
    private static StringBuilder obtainGroupByClause(ReturnClause rc, StringBuilder sql) throws IOException {
        sql.append(" GROUP BY ");

        for (CypReturn cR : rc.getItems()) {
            if (cR.getField() != null && !cR.getField().startsWith("count")) {
                String prop = cR.getField();
                if (prop != null) {
                    sql.append("n").append(".").append(prop).append(", ");
                }
            } else {
                FileInputStream fis = new FileInputStream(c2sqlV1.workspaceArea + "/meta.txt");
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String line;
                while ((line = br.readLine()) != null) {
                    sql.append("n").append(".").append(line).append(", ");
                }
            }
        }

        sql.setLength(sql.length() - 2);
        return sql;
    }

    /**
     * Generates bulk of the query, mostly by calling other methods and collating results.
     *
     * @param matchC      Match clause from Cypher.
     * @param returnC     Return clause from Cypher.
     * @param sql         Existing SQL.
     * @param hasDistinct Do we need to select DISTINCT or not?
     * @param alias       Does the Cypher query use aliasing when returning data?
     * @return New SQL statement.
     * @throws Exception
     */
    private static StringBuilder obtainMatchAndReturn(MatchClause matchC, ReturnClause returnC,
                                                      StringBuilder sql, boolean hasDistinct,
                                                      Map<String, String> alias) throws Exception {
        if (returnC.getItems() == null) throw new Exception("NOTHING SPECIFIED TO RETURN");

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
                sql = obtainVarRel(sql, matchC);
                return sql;
            }

            sql = obtainWithClause(sql, matchC);
            sql = obtainSelectAndFromClause(returnC, matchC, sql, hasDistinct, alias);
            sql = obtainWhereClause(sql, returnC, matchC);
            return sql;
        }
    }

    /**
     * Generates SQL for when there is a relationship of type -[*a..b]
     * @param sql Existing SQL
     * @param matchC Match Clause of Cypher.
     * @return New SQL.
     */
    private static StringBuilder obtainVarRel(StringBuilder sql, MatchClause matchC) {
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

        CypNode cN1;
        CypNode cN2 = null;

        if (direction.equals("left")) {
            cN1 = matchC.getNodes().get(1);
            cN2 = matchC.getNodes().get(0);
            sql.append(getZeroStep(cN1));
        } else if (direction.equals("right")) {
            cN1 = matchC.getNodes().get(0);
            cN2 = matchC.getNodes().get(1);
            sql.append(getZeroStep(cN1));
        }

        sql.append(" ");
        sql = createStepView(sql, amount);

        if (cN2 != null) {
            if (cN2.getType() != null || cN2.getProps() != null) {
                sql = addWhereToSelectForVarRels(sql, cN2);
            }
        }

        return sql;
    }

    /**
     * Adds WHERE clause to SQL when there is a variable relationship to match.
     *
     * @param sql  Existing SQL statement.
     * @param node If node being queried has additional properties, add these to the clause.
     * @return New SQL.
     */
    private static StringBuilder addWhereToSelectForVarRels(StringBuilder sql, CypNode node) {
        sql.append(" WHERE ");
        if (node.getType() != null) {
            sql.append("n.label LIKE ").append(genLabelLike(node)).append(" AND ");
        }
        if (node.getProps() != null) {
            JsonObject obj = node.getProps();
            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                sql.append("n.").append(entry.getKey()).append(" = '");
                sql.append(entry.getValue().getAsString()).append("' AND ");
            }
        }
        sql.setLength(sql.length() - 5);
        return sql;
    }

    /**
     * The query for the binding the results of the variable relationship
     * query to the nodes relation, thus returning the correct results.
     * @param sql Existing SQL query.
     * @param amount Depth user wishes to search.
     * @return New SQL.
     */
    private static StringBuilder createStepView(StringBuilder sql, int amount) {
        sql.append("CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr as x");
        sql.append(" FROM tClosure JOIN zerostep on idl = zerostep.id");
        sql.append(" JOIN nodes as n on idr = n.id where depth <=");
        sql.append(amount).append(") SELECT * from graphT); ");
        sql.append("SELECT * FROM nodes n JOIN step on x = n.id");
        return sql;
    }

    /**
     * Obtain WHERE clause of SQL when there are no relationships to deal with.
     * @param sql Existing SQL.
     * @param returnC Return Clause of original Cypher query.
     * @param matchC Match Clause of original Cypher query.
     * @return New SQL.
     */
    private static StringBuilder obtainWhereClauseOnlyNodes(StringBuilder sql, ReturnClause returnC, MatchClause matchC) {
        boolean hasWhere = false;

        for (CypReturn cR : returnC.getItems()) {
            if (cR.getNodeID() == null && cR.getField().equals("*")) {
                CypNode cN = matchC.getNodes().get(0);
                hasWhere = true;
                sql.append(" WHERE n.label LIKE ").append(genLabelLike(cN)).append(" AND ");
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
                if (!hasWhere) {
                    sql.append(" WHERE ");
                    hasWhere = true;
                }

                if (cN.getType() != null)
                    sql.append("n.label LIKE ").append(genLabelLike(cN)).append(" AND ");

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

        if (hasWhere) {
            sql.setLength(sql.length() - 5);
        }
        return sql;
    }

    /**
     * Obtain WITH clause (Common Table Expression) for query with relationships.
     * @param sql Existing SQL
     * @param matchC Match Clause of the original Cypher query.
     * @return New SQL.
     */
    private static StringBuilder obtainWithClause(StringBuilder sql, MatchClause matchC) {
        sql.append("WITH ");
        int indexRel = 0;

        for (CypRel cR : matchC.getRels()) {
            String withAlias = String.valueOf(alphabet[indexRel]);
            sql.append(withAlias).append(" AS ");
            sql.append("(SELECT n1.id AS ").append(withAlias).append(1).append(", ");
            sql.append("n2.id AS ").append(withAlias).append(2);
            sql.append(", e").append(indexRel + 1).append(".*");

            switch (cR.getDirection()) {
                case "right":
                    sql.append(" FROM nodes n1 " + "INNER JOIN edges e").append(indexRel + 1)
                            .append(" on n1.id = e").append(indexRel + 1).append(".idl ")
                            .append("INNER JOIN nodes n2 on e").append(indexRel + 1).append(".idr = n2.id");
                    sql = obtainWhereInWithClause(cR, matchC, sql, false, indexRel);
                    break;
                case "left":
                    sql.append(" FROM nodes n1 " + "INNER JOIN edges e").append(indexRel + 1)
                            .append(" on n1.id = e").append(indexRel + 1).append(".idr ")
                            .append("INNER JOIN nodes n2 on e").append(indexRel + 1).append(".idl = n2.id");
                    sql = obtainWhereInWithClause(cR, matchC, sql, false, indexRel);
                    break;
                case "none":
                    sql.append(" FROM nodes n1 " + "INNER JOIN edges e").append(indexRel + 1)
                            .append(" on n1.id = e").append(indexRel + 1).append(".idl ")
                            .append("INNER JOIN nodes n2 on e").append(indexRel + 1).append(".idr = n2.id");
                    sql = obtainWhereInWithClause(cR, matchC, sql, true, indexRel);
                    sql.append("SELECT n1.id AS ").append(withAlias).append(1).append(", ");
                    sql.append("n2.id AS ").append(withAlias).append(2);
                    sql.append(", e").append(indexRel + 1).append(".*");
                    sql.append(" FROM nodes n1 " + "INNER JOIN edges e").append(indexRel + 1)
                            .append(" on n1.id = e").append(indexRel + 1).append(".idr ")
                            .append("INNER JOIN nodes n2 on e").append(indexRel + 1).append(".idl = n2.id");
                    sql = obtainWhereInWithClause(cR, matchC, sql, false, indexRel);
                    break;
            }

            indexRel++;
        }

        sql.setLength(sql.length() - 2);
        sql.append(" ");
        return sql;
    }

    /**
     * Obtains the WHERE within each WITH CTE.
     * @param cR Relationship to map.
     * @param matchC Match Clause of Cypher.
     * @param sql Existing SQL.
     * @param isBiDirectional Does the Cypher relationship map both directions (i.e. -[]-)
     * @param indexRel Where is the relationship in the whole context of the match clause (index starts at 1)
     * @return New SQL.
     */
    private static StringBuilder obtainWhereInWithClause(CypRel cR, MatchClause matchC, StringBuilder sql,
                                                         boolean isBiDirectional, int indexRel) {
        boolean includesWhere = false;
        int posOfRel = cR.getPosInClause();

        CypNode leftNode = obtainNode(matchC, posOfRel);
        JsonObject leftProps = leftNode.getProps();
        CypNode rightNode = obtainNode(matchC, posOfRel + 1);
        JsonObject rightProps = rightNode.getProps();
        String typeRel = cR.getType();
        JsonObject o = cR.getProps();

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
            sql.append("n1.label LIKE ");
            sql.append(genLabelLike(leftNode)).append(" AND ");
        }

        if (rightNode.getType() != null) {
            if (!includesWhere) {
                sql.append(" WHERE ");
                includesWhere = true;
            }
            sql.append("n2.label LIKE ");
            sql.append(genLabelLike(rightNode)).append(" AND ");
        }

        if (typeRel != null) {
            if (!includesWhere) {
                sql.append(" WHERE ");
                includesWhere = true;
            }

            sql.append("e").append(indexRel + 1).append(".type = '").append(typeRel);
            sql.append("' AND ");
        }

        if (o != null) {
            if (!includesWhere) {
                sql.append(" WHERE ");
                includesWhere = true;
            }

            Set<Map.Entry<String, JsonElement>> entries = o.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                sql.append("e").append(indexRel + 1).append(".").append(entry.getKey()).append(" = '");
                sql.append(entry.getValue().getAsString()).append("' AND ");
            }
        }

        if (includesWhere) sql.setLength(sql.length() - 5);
        if (isBiDirectional) {
            sql.append(" UNION ALL ");
        } else {
            sql.append("), ");
        }
        return sql;
    }

    /**
     *
     * @param sql
     * @param entry
     * @return
     */
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

    /**
     *
     * @param returnC
     * @param matchC
     * @param sql
     * @param hasDistinct
     * @param alias
     * @return
     * @throws Exception
     */
    private static StringBuilder obtainSelectAndFromClause(ReturnClause returnC, MatchClause matchC,
                                                           StringBuilder sql, boolean hasDistinct,
                                                           Map<String, String> alias)
            throws Exception {
        sql.append("SELECT ");
        if (hasDistinct) sql.append("DISTINCT ");

        for (CypReturn cR : returnC.getItems()) {
            boolean isNode = false;

            if (cR.getNodeID() == null && cR.getField().equals("*")) {
                sql.append("*  ");
                break;
            }

            if (cR.getField() != null && cR.getField().startsWith("count")) {
                String toAdd;
                int posInCluase = cR.getPosInClause();
                if (posInCluase == 1) toAdd = "a1";
                else toAdd = "a2";
                sql.append("count(").append(toAdd).append(")");
                sql.append(useAlias(cR.getField(), alias)).append(", ");
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
                    break;
                }
            }

            if (!isNode) {
                for (CypRel cRel : matchC.getRels()) {
                    if (cRel.getId() != null && cRel.getId().equals(cR.getNodeID())) {
                        String prop = cR.getField();
                        int relPos = cRel.getPosInClause();
                        String idRel = (relPos == 1) ? "a" : (relPos == 2) ? "b" : (relPos == 3) ? "c" : "a";
                        if (prop != null) {
                            sql.append(idRel).append(".").append(prop).append(useAlias(cR.getNodeID(), alias)).append(", ");
                        } else {
                            sql.append(idRel).append(".*").append(useAlias(cR.getNodeID(), alias)).append(", ");
                        }
                        break;
                    }
                }
            }
        }

        sql.setLength(sql.length() - 2);

        sql.append(" FROM nodes n, ");

        int numRels = matchC.getRels().size();
        for (int i = 0; i < numRels; i++)
            sql.append(alphabet[i]).append(", ");

        sql.setLength(sql.length() - 2);
        sql.append(" ");
        return sql;
    }

    /**
     *
     * @param nodeID
     * @param alias
     * @return
     */
    private static String useAlias(String nodeID, Map<String, String> alias) {
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

    /**
     *
     * @param sql
     * @param returnC
     * @param matchC
     * @return
     * @throws Exception
     */
    private static StringBuilder obtainWhereClause(StringBuilder sql,
                                                   ReturnClause returnC, MatchClause matchC) throws Exception {
        sql.append(" WHERE ");
        int numRels = matchC.getRels().size();

        for (int i = 0; i < numRels - 1; i++) {
            sql.append(alphabet[i]).append(".").append(alphabet[i]).append(2);
            sql.append(" = ");
            sql.append(alphabet[i + 1]).append(".").append(alphabet[i + 1]).append(1);
            sql.append(" AND ");
        }

        if ((numRels == 1)) {
            if (returnC.getItems().size() == 1 && returnC.getItems().get(0).getType().equals("rel")) {
                if (matchC.getRels().get(0).getDirection().equals("left")) {
                    sql.append(" a.a1 = n.idr AND a.a2 = n.idl ");
                } else if (matchC.getRels().get(0).getDirection().equals("right")) {
                    sql.append(" a.a1 = n.idl AND a.a2 = n.idr ");
                } else
                    sql.append(" (a.a1 = n.idl AND a.a2 = n.idr) OR (a.a1 = n.idr AND a.a2 = n.idl) ");
            } else if (matchC.getRels().get(0).getDirection().equals("none")) {
                int posInCl = returnC.getItems().get(0).getPosInClause();
                if (posInCl == 1) return sql.append(" n.id = a.a1");
                else return sql.append("n.id = a.a2");
            }
        }

        ArrayList<String> nodeIDS = new ArrayList<>();
        ArrayList<String> crossResults = new ArrayList<>();

        for (CypNode cN : matchC.getNodes()) {
            if (cN.getId() != null) {
                if (nodeIDS.contains(cN.getId())) {
                    crossResults.add(nodeIDS.indexOf(cN.getId()) + "," +
                            (nodeIDS.size() + crossResults.size()));
                } else nodeIDS.add(cN.getId());
            }
        }

        for (CypReturn cR : returnC.getItems()) {
            if (cR.getType() != null) {
                switch (cR.getType()) {
                    case "node":
                        int posInClause = cR.getPosInClause();
                        sql.append("n.id = ");

                        if (posInClause == 1) {
                            sql.append("a.a1");
                            sql.append(" AND ");
                        } else {
                            sql.append(alphabet[posInClause - 2]).append(".").append(alphabet[posInClause - 2])
                                    .append(2);
                            sql.append(" AND ");
                        }
                        break;
                    default:
                        break;
                }
            }
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

        if (!crossResults.isEmpty()) {
            for (String s : crossResults) {
                String[] t = s.split(",");
                int[] indices = new int[2];
                indices[0] = Integer.parseInt(t[0]);
                indices[1] = Integer.parseInt(t[1]);
                String a;
                String b;
                if (indices[0] == 0) a = "a.a1";
                else a = alphabet[indices[0] - 1] + "." + alphabet[indices[0] - 1] + "2";
                b = alphabet[indices[1] - 1] + "." + alphabet[indices[1] - 1] + "2";
                sql.append(a).append(" = ").append(b).append(" AND ");
            }
        }

        if (sql.toString().endsWith(" AND ")) sql.setLength(sql.length() - 5);

        return sql;
    }

    /**
     *
     * @param orderC
     * @param sql
     * @return
     */
    private static StringBuilder obtainOrderByClause(OrderClause orderC, StringBuilder sql) {
        sql.append(" ");
        sql.append("ORDER BY ");
        for (CypOrder cO : orderC.getItems()) {
            if (cO.getField().startsWith("count")) {
                sql.append("count(n) ").append(cO.getAscOrDesc()).append(", ");
                break;
            }
            sql.append("n").append(".").append(cO.getField()).append(" ").append(cO.getAscOrDesc());
            sql.append(", ");
        }
        sql.setLength(sql.length() - 2);
        return sql;
    }

    /**
     *
     * @param matchC
     * @param i
     * @return
     */
    private static CypNode obtainNode(MatchClause matchC, int i) {
        for (CypNode c : matchC.getNodes()) {
            if (c.getPosInClause() == i) {
                return c;
            }
        }
        return null;
    }

    /**
     *
     * @return
     */
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
                "SELECT * FROM search_graph where (not cycle OR not idr = ANY(path)));";
    }

    /**
     *
     * @param cN
     * @return
     */
    public static String getZeroStep(CypNode cN) {
        String getZStep = "CREATE TEMP VIEW zerostep AS" +
                " (SELECT id from nodes";
        boolean hasWhere = false;
        JsonObject obj = cN.getProps();
        if (obj != null) {
            getZStep += " WHERE ";
            hasWhere = true;
            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                getZStep = getZStep + entry.getKey() + " = ";
                getZStep = getZStep + "'" + entry.getValue().getAsString() + "'";
                getZStep = getZStep + " AND ";
            }
            getZStep = getZStep.substring(0, getZStep.length() - 5);
        }

        if (cN.getType() != null) {
            if (!hasWhere) {
                getZStep += " WHERE ";
            } else {
                getZStep += " AND ";
            }
            getZStep = getZStep + "label LIKE " + genLabelLike(cN);
        }

        getZStep = getZStep + ");";
        System.out.println(getZStep);
        return getZStep;
    }

    /**
     *
     * @param cN
     * @return
     */
    private static String genLabelLike(CypNode cN) {
        String label = cN.getType();
        String stmt = "'%";
        String[] labels = label.split(", ");
        for (String l : labels) {
            stmt = stmt + l + "%' AND n.label LIKE '%";
        }
        stmt = stmt.substring(0, stmt.length() - 19);
        System.out.println(stmt);
        return stmt;
    }
}
