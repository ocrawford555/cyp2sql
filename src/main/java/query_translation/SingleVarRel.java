package query_translation;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;
import java.util.Set;

class SingleVarRel {
    static StringBuilder translate(StringBuilder sql, DecodedQuery decodedQuery) {
        MatchClause matchC = decodedQuery.getMc();

        String direction = "none";
        int amountLow = 0;
        int amountHigh = 0;

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
        sql = createStepView(sql, amountLow, amountHigh, decodedQuery.getRc());

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
            sql.append("n.label LIKE ").append(TranslateUtils.genLabelLike(node)).append(" AND ");
        }
        if (node.getProps() != null) {
            JsonObject obj = node.getProps();
            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                sql.append("n.").append(entry.getKey());
                sql = TranslateUtils.addWhereClause(sql, entry);
            }
        }
        sql.setLength(sql.length() - 5);
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
     */
    private static StringBuilder createStepView(StringBuilder sql, int amountLow,
                                                int amountHigh, ReturnClause returnC) {
        sql.append("CREATE TEMP VIEW step AS (WITH graphT AS (SELECT idr as x");
        sql.append(" FROM tClosure JOIN zerostep on idl = zerostep.id");
        sql.append(" JOIN nodes as n on idr = n.id where depth <= ");
        sql.append(amountHigh).append(" AND depth >= ").append(amountLow);
        sql.append(") SELECT * from graphT); ");

        sql.append("SELECT ");

        // return only the correct things
        for (CypReturn cR : returnC.getItems()) {
            if (cR.getField() == null) {
                sql.append("*");
            } else {
                sql.append("n.").append(cR.getField()).append(", ");
            }
        }

        String table = TranslateUtils.getTable(returnC);

        if (sql.toString().endsWith(", ")) {
            sql.setLength(sql.length() - 2);
        }
        sql.append(" ");
        sql.append("FROM ").append(table).append(" n JOIN step on x = n.id");
        return sql;
    }

    /**
     * @param cN Node of the variable query that is being included in the first step query.
     * @return SQL view of this first step in the variable relationship.
     */
    private static String getZeroStep(CypNode cN) {
        StringBuilder getZStep = new StringBuilder();
        getZStep.append("CREATE TEMP VIEW zerostep AS SELECT id from nodes");

        boolean hasWhere = false;
        JsonObject obj = cN.getProps();

        if (obj != null) {
            getZStep.append(" WHERE ");
            hasWhere = true;
            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();

            for (Map.Entry<String, JsonElement> entry : entries) {
                getZStep.append(entry.getKey());
                getZStep = TranslateUtils.addWhereClause(getZStep, entry);
            }
            getZStep.setLength(getZStep.length() - 5);
        }

        if (cN.getType() != null) {
            if (!hasWhere) {
                getZStep.append(" WHERE ");
            } else {
                getZStep.append(" AND ");
            }
            getZStep.append("label LIKE ").append(TranslateUtils.genLabelLike(cN));
        }

        getZStep.append(";");
        return getZStep.toString();
    }

}
