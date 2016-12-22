package query_translation;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;
import java.util.Set;

/**
 * Translating Cypher queries where structure is of form (a)-[*b..c]->(d).
 * Note - only can translate ONE of these types of queries, i.e. cannot string
 * them together in the case of for example (a)-[*1..2]->(b)-[*3..4]->(c).
 * Note - direction of relationship must be specified.
 */
class SingleVarRel {
    static StringBuilder translate(StringBuilder sql, DecodedQuery decodedQuery) {
        MatchClause matchC = decodedQuery.getMc();

        String direction = "none";
        int amountLow = 0;
        int amountHigh = 0;

        // work out direction of query and upper and lower bound on number of edges
        // the query is allowed to traverse.
        for (CypRel cR : matchC.getRels()) {
            if (cR.getDirection().contains("var")) {
                String dirAndAmount[] = cR.getDirection().split("#");
                direction = dirAndAmount[1];
                amountLow = Integer.parseInt(dirAndAmount[0].split("-")[0].substring(3));
                amountHigh = Integer.parseInt(dirAndAmount[0].split("-")[1]);
            }
        }

        CypNode cN1;
        CypNode cN2 = null;

        // build up query in same direction as relationship is going.
        if (direction.equals("left")) {
            cN1 = matchC.getNodes().get(1);
            cN2 = matchC.getNodes().get(0);
            sql.append(getZeroStep(cN1, decodedQuery.getRc()));
        } else if (direction.equals("right")) {
            cN1 = matchC.getNodes().get(0);
            cN2 = matchC.getNodes().get(1);
            sql.append(getZeroStep(cN1, decodedQuery.getRc()));
        }

        sql.append(" ");
        // create the query that goes along all the paths, based on the transitive
        // closure view in SQL.
        sql = createStepView(sql, amountLow, amountHigh, decodedQuery.getRc(),
                decodedQuery.getCypherAdditionalInfo().getAliasMap());

        // final part to add to the SQL statement is to select the data that matches
        // the properties desired (such as a film title or certain director etc.)
        if (cN2 != null) {
            if (cN2.getType() != null || cN2.getProps() != null) {
                sql = addWhereToSelectForVarRels(sql, cN2, decodedQuery.getWc());
            }
        }

        return sql;
    }

    /**
     * Adds WHERE clause to SQL when there is a variable relationship to match.
     *
     * @param sql  Existing SQL statement.
     * @param node If node being queried has additional properties, add these to the clause.
     * @param wc   Where Clause to examine and add.
     * @return New SQL.
     */
    private static StringBuilder addWhereToSelectForVarRels(StringBuilder sql, CypNode node, WhereClause wc) {
        boolean usedWhere = false;
        if (node.getType() != null) {
            sql.append(" WHERE n.label LIKE ").append(TranslateUtils.genLabelLike(node, "n"));
            usedWhere = true;
        }
        if (node.getProps() != null) {
            if (!usedWhere) sql.append(" WHERE ");
            else sql.append(" AND ");
            sql = TranslateUtils.getWholeWhereClause(sql, node, wc);
        }
        return sql;
    }

    /**
     * The query for the binding the results of the variable relationship
     * query to the nodes relation, thus returning the correct results.
     *
     * @param sql        Existing SQL query.
     * @param amountLow  Lower bound on depth of links to search.
     * @param amountHigh Upper bound on depth of links to search.
     * @param returnC    @return New SQL.
     * @param alias
     */
    private static StringBuilder createStepView(StringBuilder sql, int amountLow,
                                                int amountHigh, ReturnClause returnC, Map<String, String> alias) {
        sql.append("CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr as x, idl as y");
        sql.append(" FROM tClosure JOIN zerostep on idl = zerostep.id");
        sql.append(" JOIN nodes as n on idr = n.id where depth <= ");
        sql.append(amountHigh).append(" AND depth >= ").append(amountLow);
        sql.append(") SELECT * from graphT); ");

        sql.append("SELECT ");

        boolean joinZeroStep = false;

        // return only the correct things
        for (CypReturn cR : returnC.getItems()) {
            if (cR.getCollect()) sql.append("array_agg(");
            if (cR.getCount()) sql.append("count(");
            if (cR.getField() == null) {
                sql.append("*");
            } else {
                if (cR.getPosInClause() == 1) {
                    sql.append("z.").append(cR.getField());
                    joinZeroStep = true;
                } else {
                    sql.append("n.").append(cR.getField());
                }
            }
            if (cR.getCollect() || cR.getCount()) {
                sql.append(") ").append(TranslateUtils.useAlias(cR.getNodeID(), cR.getField(), alias)).append(", ");
            } else {
                sql.append(TranslateUtils.useAlias(cR.getNodeID(), cR.getField(), alias)).append(", ");
            }
        }

        String table = TranslateUtils.getTable(returnC);

        if (sql.toString().endsWith(", ")) {
            sql.setLength(sql.length() - 2);
        }
        sql.append(" ");
        sql.append("FROM ").append(table).append(" n JOIN step on x = n.id");
        if (joinZeroStep) sql.append(" JOIN zerostep z on z.id = y");
        return sql;
    }

    /**
     * @param cN Node of the variable query that is being included in the first step query.
     * @param rc Return clause of the original query - if need to return values from the node that
     *           is being searched from, then need to include those fields in the initial zerostep.
     * @return SQL view of this first step in the variable relationship.
     */
    private static String getZeroStep(CypNode cN, ReturnClause rc) {
        StringBuilder getZStep = new StringBuilder();
        getZStep.append("CREATE TEMP VIEW zerostep AS SELECT id");

        for (CypReturn cR : rc.getItems()) {
            if (cR.getPosInClause() == 1) {
                getZStep.append(", ").append(cR.getField());
            }
        }

        getZStep.append(" from nodes");

        boolean hasWhere = false;
        JsonObject obj = cN.getProps();

        if (obj != null) {
            getZStep.append(" WHERE ");
            hasWhere = true;
            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();

            for (Map.Entry<String, JsonElement> entry : entries) {
                getZStep.append(entry.getKey());
                getZStep = TranslateUtils.addWhereClause(getZStep, entry.getValue().getAsString());
            }

        }

        if (cN.getType() != null) {
            if (!hasWhere) {
                getZStep.append(" WHERE ");
            } else {
                getZStep.append(" AND ");
            }
            getZStep.append("label LIKE ").append(TranslateUtils.genLabelLike(cN, null));
        }

        getZStep.append(";");
        return getZStep.toString();
    }

}
