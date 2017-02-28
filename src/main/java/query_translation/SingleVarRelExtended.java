package query_translation;

import clauseObjects.CypNode;
import clauseObjects.DecodedQuery;
import clauseObjects.MatchClause;

class SingleVarRelExtended {
    private static char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    static StringBuilder translate(StringBuilder sql, DecodedQuery decodedQuery,
                                   String direction, int amountLow, int amountHigh, MatchClause matchC) {
        CypNode cN1;
        CypNode cN2 = null;

        // build up query in same direction as relationship is going.
        if (direction.equals("left")) {
            cN1 = matchC.getNodes().get(1);
            cN2 = matchC.getNodes().get(0);
            sql.append(SingleVarRel.getZeroStep(cN1, decodedQuery.getRc()));
        } else if (direction.equals("right")) {
            cN1 = matchC.getNodes().get(0);
            cN2 = matchC.getNodes().get(1);
            sql.append(SingleVarRel.getZeroStep(cN1, decodedQuery.getRc()));
        }

        sql = SingleVarRel.getStepView(sql, amountLow, 5);

        sql.append("CREATE TEMP VIEW cr AS (");
        sql = SingleVarRel.getFinalSelect(sql, decodedQuery.getRc(),
                decodedQuery.getCypherAdditionalInfo().getAliasMap(), true);

        sql.append(");").append(" ");

        int numberExtendedJoins = amountHigh - 5;

        for (int index = 1; index <= numberExtendedJoins; index++) {
            sql.append("WITH ").append(alphabet[index - 1]).append(" AS (");
            sql.append("SELECT n1.id AS a1, n2.id AS a2, e1.* FROM ");
            if (index == 1) sql.append("cr");
            else sql.append("nodes");
            sql.append(" n").append(index).append(" INNER JOIN edges e1 on");
            sql.append(" n").append(index).append(" = e1.");
            if (direction.equals("right")) sql.append("idl");
            else sql.append("idr");
            sql.append(" INNER JOIN nodes n").append(index + 1).append(" on e1.");
            if (direction.equals("right")) sql.append("idr");
            else sql.append("idl");
            sql.append(" = n").append(index + 1).append("), ");
        }

        sql.setLength(sql.length() - 2);

        sql = getFinalSelectExtended(sql);
        return sql;
    }

    private static StringBuilder getFinalSelectExtended(StringBuilder sql) {
        sql.append("SELECT ");
        return sql;
    }
}
