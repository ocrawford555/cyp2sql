package query_translation;

import clauseObjects.*;

import java.util.Map;

public class SQLShortestPath {
    private static char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static StringBuilder translate(DecodedQuery dQMainPath) {
        StringBuilder shortPath = new StringBuilder();
        shortPath.append("WITH a AS(SELECT unnest(rightnode) AS xx, 1 AS Depth, ARRAY[id] AS Path, id AS Start ");
        shortPath.append("FROM adjList_from INNER JOIN ");

        MatchClause matchC = dQMainPath.getMc();
        String direction = "none";
        int amountHigh = 0;

        // work out direction of query and upper and lower bound on number of edges
        // the query is allowed to traverse.
        for (CypRel cR : matchC.getRels()) {
            if (cR.getDirection().contains("var")) {
                String dirAndAmount[] = cR.getDirection().split("#");
                direction = dirAndAmount[1];
                amountHigh = Integer.parseInt(dirAndAmount[0].split("-")[1]);
            }
        }

        CypNode cN1 = null;
        CypNode cN2 = null;

        // build up query in same direction as relationship is going.
        if (direction.equals("left")) {
            cN1 = matchC.getNodes().get(1);
            cN2 = matchC.getNodes().get(0);
        } else if (direction.equals("right")) {
            cN1 = matchC.getNodes().get(0);
            cN2 = matchC.getNodes().get(1);
        }

        shortPath.append(getFirstStep(cN1, dQMainPath.getWc()));
        shortPath.append("), ");

        int lastIndex = 1;

        for (int i = 1; i < amountHigh; i++) {
            shortPath.append(alphabet[i]).append(" AS (SELECT unnest(rightnode) AS xx, ");
            shortPath.append(i + 1).append(" AS Depth, ");
            shortPath.append(alphabet[i - 1]).append(".Path || ARRAY[xx] AS Path, ");
            shortPath.append(alphabet[i - 1]).append(".Path[1] AS Start FROM adjList_from INNER JOIN ");
            shortPath.append(alphabet[i - 1]).append(" ON leftnode = xx), ");
            lastIndex = i;
        }

        if (!shortPath.toString().endsWith(", ")) {
            shortPath.append(", ");
        }

        lastIndex++;

        shortPath.append(joinViewsTogether(lastIndex, amountHigh));

        shortPath.append(getFinalSelect(lastIndex, cN2, dQMainPath.getRc(),
                dQMainPath.getCypherAdditionalInfo().getAliasMap(), dQMainPath.getWc()));

        if (dQMainPath.getOc() != null)
            shortPath = SQLTranslate.obtainOrderByClause(dQMainPath.getOc(), shortPath);

        int skipAmount = dQMainPath.getSkipAmount();
        int limitAmount = dQMainPath.getLimitAmount();

        if (skipAmount != -1) shortPath.append(" OFFSET ").append(skipAmount);
        if (limitAmount != -1) shortPath.append(" LIMIT ").append(limitAmount);

        shortPath.append(";");

        return shortPath;
    }

    private static String joinViewsTogether(int lastIndex, int amountHigh) {
        StringBuilder sql = new StringBuilder();
        sql.append(alphabet[lastIndex]).append(" AS (SELECT * FROM a");
        for (int i = 1; i < amountHigh; i++) {
            sql.append(" UNION ALL SELECT * FROM ").append(alphabet[i]);
        }
        sql.append("), ");
        return sql.toString();
    }

    private static String getFirstStep(CypNode cN1, WhereClause wc) {
        StringBuilder sql = new StringBuilder();

        sql.append("nodes q");

        sql.append(" ON leftnode = id");

        boolean hasWhere = false;

        if (cN1.getType() != null) {
            sql.append(" WHERE label LIKE ");
            hasWhere = true;
            sql.append(TranslateUtils.genLabelLike(cN1, "q"));
        }

        if (cN1.getProps() != null) {
            if (hasWhere) sql.append(" AND ");
            else sql.append(" WHERE ");
            sql = TranslateUtils.getWholeWhereClause(sql, cN1, wc, "q");
        }

        return sql.toString();
    }

    private static String getFinalSelect(int lastIndex, CypNode cN2, ReturnClause rc,
                                         Map<String, String> alias, WhereClause wc) {
        StringBuilder sql = new StringBuilder();

        String thingsToGroupBy = "";

        sql.append("finStep AS (SELECT ");

        // return only the correct things
        for (CypReturn cR : rc.getItems()) {
            //if (cR.getCollect()) sql.append("array_agg(");
            //if (cR.getCount()) sql.append("count(");
            if (cR.getField() == null) {
                sql.append("*");
            } else {
                sql.append("n01.").append(cR.getField());
            }
//            if (cR.getCollect() || cR.getCount()) {
//                sql.append(") ").append(TranslateUtils.useAlias(cR.getNodeID(), cR.getField(), alias)).append(", ");
//            } else {
//                sql.append(TranslateUtils.useAlias(cR.getNodeID(), cR.getField(), alias)).append(", ");
//            }
            sql.append(TranslateUtils.useAlias(cR.getNodeID(), cR.getField(), alias)).append(", ");
            if (cR.getField() != null) {
                thingsToGroupBy = thingsToGroupBy + cR.getField() + ", ";
            }
        }

        String table = TranslateUtils.getTable(rc);

        if (sql.toString().endsWith(", ")) {
            sql.setLength(sql.length() - 2);
        }
        sql.append(", min(Depth), xx, Start ");
        sql.append("FROM ").append(table).append(" n01 INNER JOIN ").append(alphabet[lastIndex]);
        sql.append(" ON xx = id");

        boolean hasWhere = false;

        if (cN2.getType() != null) {
            sql.append(" WHERE label LIKE ");
            hasWhere = true;
            sql.append(TranslateUtils.genLabelLike(cN2, "n01"));
        }

        if (cN2.getProps() != null) {
            if (hasWhere) sql.append(" AND ");
            else sql.append(" WHERE ");
            sql = TranslateUtils.getWholeWhereClause(sql, cN2, wc, "n01");
        }

        thingsToGroupBy = thingsToGroupBy.substring(0, thingsToGroupBy.length() - 2);
        sql.append(" GROUP BY ").append(thingsToGroupBy).append(", xx, Start) SELECT ");

        // return only the correct things
        for (CypReturn cR : rc.getItems()) {
            if (cR.getCollect()) sql.append("array_agg(");
            if (cR.getCount()) sql.append("count(");
            if (cR.getField() == null) {
                sql.append("*");
            } else {
                sql.append("n01.").append(cR.getField());
            }
            if (cR.getCollect() || cR.getCount()) {
                sql.append(") ").append(TranslateUtils.useAlias(cR.getNodeID(), cR.getField(), alias)).append(", ");
            } else {
                sql.append(TranslateUtils.useAlias(cR.getNodeID(), cR.getField(), alias)).append(", ");
            }
        }

        if (sql.toString().endsWith(", ")) {
            sql.setLength(sql.length() - 2);
        }

        sql.append(" FROM finStep n01");
        return sql.toString();
    }
}
