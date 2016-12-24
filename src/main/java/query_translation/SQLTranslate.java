package query_translation;

import clauseObjects.*;
import production.c2sqlV2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * MAIN TRANSLATION UNIT FROM INTERMEDIATE TRANSLATION TO SQL.
 * <p>
 * Read individual methods documentation for more understanding.
 * - MultipleRel
 * - NoRels
 * - SingleVarRel
 * <p>
 * Agnostic to the methods above is appending the ORDER BY, GROUP BY, LIMIT and SKIP elements.
 */
public class SQLTranslate {
    /**
     * Translate calls other methods, stitching the results together into one SQL query.
     *
     * @param decodedQuery All the intermediate data gathered about the original Cypher query.
     * @return SQL string that maps to the original Cypher command.
     * @throws Exception
     */
    public static String translateRead(DecodedQuery decodedQuery) throws Exception {
        // SQL built up from a StringBuilder object.
        StringBuilder sql = new StringBuilder();

        if (decodedQuery.getMc().getNodes().isEmpty()) throw new Exception("MATCH CLAUSE INVALID");
        if (decodedQuery.getRc().getItems() == null) throw new Exception("RETURN CLAUSE INVALID");

        if (decodedQuery.getMc().getRels().isEmpty()) {
            sql = NoRels.translate(sql, decodedQuery);
        } else if (decodedQuery.getMc().isVarRel() && decodedQuery.getMc().getRels().size() == 1) {
            sql = SingleVarRel.translate(sql, decodedQuery);
        } else {
            sql = MultipleRel.translate(sql, decodedQuery);

            if (decodedQuery.getCypherAdditionalInfo().hasCount() && decodedQuery.getRc().getItems().size() > 1)
                sql = obtainGroupByClause(decodedQuery.getRc(), sql);
        }

        if (decodedQuery.getOc() != null)
            sql = obtainOrderByClause(decodedQuery.getOc(), sql);

        int skipAmount = decodedQuery.getSkipAmount();
        int limitAmount = decodedQuery.getLimitAmount();

        if (skipAmount != -1) sql.append(" OFFSET ").append(skipAmount);
        if (limitAmount != -1) sql.append(" LIMIT ").append(limitAmount);

        sql.append(";");
        return sql.toString();
    }

    public static String translateInsertNodes(DecodedQuery decodedQuery) throws Exception {
        StringBuilder sql = new StringBuilder();
        MatchClause createC = decodedQuery.getMc();
        String relation = InsertUtils.findRelation(createC, 0);
        String[] colsAndValues = InsertUtils.findColsAndValues(createC, 0);

        sql.append("INSERT INTO nodes");
        sql.append("(");
        sql.append(colsAndValues[0]).append(", label) ");
        sql.append("VALUES (");
        sql.append(colsAndValues[1]).append(", '").append(relation.replace("_", ", ")).append("');");

        sql.append("INSERT INTO ");
        sql.append(relation).append("(");
        sql.append(colsAndValues[0]).append(", id, label) ");
        sql.append("VALUES (");
        sql.append(colsAndValues[1]).append(", (SELECT id FROM nodes WHERE ");

        String[] values = colsAndValues[1].split(", ");
        int i = 0;
        for (String col : colsAndValues[0].split(", ")) {
            sql.append(col).append(" = ").append(values[i++]).append(" AND ");
        }
        sql.setLength(sql.length() - 5);
        sql.append("), '").append(relation.replace("_", ", ")).append("');");

        System.out.println(sql.toString());
        return sql.toString();
    }

    public static String translateInsertRels(DecodedQuery decodedQuery) {
        StringBuilder sql = new StringBuilder();
        MatchClause createC = decodedQuery.getMc();
        String[] colsAndValues = InsertUtils.findColsAndValuesRels(
                decodedQuery.getCypherAdditionalInfo().getCreateClauseRel());
        sql.append("INSERT INTO edges (");

        if (!colsAndValues[0].equals("")) sql.append(colsAndValues[0]).append(", idl, idr, type) ");
        else sql.append("idl, idr, type) ");

        sql.append("VALUES ((");
        if (!colsAndValues[1].equals("")) sql.append(colsAndValues[1]).append(", ");

        String selectA = "SELECT id FROM " + InsertUtils.findRelation(createC, 0) + " WHERE ";
        String[] selectAColsAndValues = InsertUtils.findColsAndValues(createC, 0);
        String[] values = selectAColsAndValues[1].split(", ");
        int i = 0;

        for (String col : selectAColsAndValues[0].split(", ")) {
            selectA = selectA + col + " = " + values[i++].replace("eq#", "").replace("#qe", "") + " AND ";
        }
        selectA = selectA.substring(0, selectA.length() - 5);
        selectA = selectA + ")";


        String selectB = "SELECT id FROM " + InsertUtils.findRelation(createC, 1) + " WHERE ";
        String[] selectBColsAndValues = InsertUtils.findColsAndValues(createC, 1);
        values = selectBColsAndValues[1].split(", ");
        i = 0;
        for (String col : selectBColsAndValues[0].split(", ")) {
            selectB = selectB + col + " = " + values[i++].replace("eq#", "").replace("#qe", "") + " AND ";
        }
        selectB = selectB.substring(0, selectB.length() - 5);
        selectB = selectB + ")";

        sql.append(selectA).append(", (").append(selectB).append(", '").append(
                decodedQuery.getCypherAdditionalInfo().getCreateClauseRel().getRels().get(0).getType()).append("'");

        sql.append(");");
        System.out.println(sql.toString());
        return sql.toString();
    }


    public static String translateDelete(DecodedQuery decodedQuery) {
        StringBuilder sql = new StringBuilder();
        MatchClause deleteC = decodedQuery.getMc();
        String relation = InsertUtils.findRelation(deleteC, 0);
        String[] colsAndValues = InsertUtils.findColsAndValues(deleteC, 0);

        sql.append("DELETE FROM nodes");
        sql.append(" WHERE ");

        String[] values = colsAndValues[1].split(", ");
        int i = 0;
        for (String col : colsAndValues[0].split(", ")) {
            sql.append(col).append(" = ").append(values[i++]).append(" AND ");
        }

        if (sql.toString().endsWith(" AND ")) sql.setLength(sql.length() - 5);

        sql.append(";");

        sql.append("DELETE FROM ");
        sql.append(relation).append(" WHERE ");
        i = 0;
        for (String col : colsAndValues[0].split(", ")) {
            sql.append(col).append(" = ").append(values[i++]).append(" AND ");
        }

        if (sql.toString().endsWith(" AND ")) sql.setLength(sql.length() - 5);

        sql.append(";");

        System.out.println(sql.toString());
        return sql.toString();
    }

    /**
     * Append ORDER BY clause to the SQL statement.
     *
     * @param orderC Order Clause object generated during the translation process.
     * @param sql    Original SQL built up already.
     * @return New SQL StringBuilder object with ORDER BY clause appended to the end.
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
     * Appends GROUP BY clause to query. This is needed if COUNT is used.
     * Note - not entirely sure logic is correct for this method, needs more testing.
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
                FileInputStream fis = new FileInputStream(c2sqlV2.workspaceArea + "/meta.txt");
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
}
