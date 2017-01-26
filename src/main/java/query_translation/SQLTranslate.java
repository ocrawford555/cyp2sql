package query_translation;

import clauseObjects.*;
import production.Cyp2SQL_v3_Apoc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

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
    public static String translateRead(DecodedQuery decodedQuery, String typeTranslate) throws Exception {
        // SQL built up from a StringBuilder object.
        StringBuilder sql = new StringBuilder();

        if (decodedQuery.getMc().getNodes().isEmpty()) throw new Exception("MATCH CLAUSE INVALID");
        if (decodedQuery.getRc().getItems() == null) throw new Exception("RETURN CLAUSE INVALID");

        if (decodedQuery.getMc().getRels().isEmpty()) {
            sql = NoRels.translate(sql, decodedQuery);
        } else if (decodedQuery.getMc().isVarRel() && decodedQuery.getMc().getRels().size() == 1) {
            if (typeTranslate.equals("-t")) {
                sql = SingleVarRel.translate(sql, decodedQuery, false);
            } else {
                sql = SingleVarAdjList.translate(sql, decodedQuery);
            }
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

    public static String translateInsert(DecodedQuery decodedQuery) {
        StringBuilder sql = new StringBuilder();
        MatchClause createC = decodedQuery.getMc();

        sql = translateInsertNodes(sql, createC);
        sql = translateInsertEdges(sql, createC);

        System.out.println(sql.toString());
        return sql.toString();
    }

    private static StringBuilder translateInsertEdges(StringBuilder sql, MatchClause createC) {
        String[] colsAndValues;

        StringBuilder insertEdgesString = new StringBuilder();

        colsAndValues = InsertUtils.findColsAndValuesRels(createC.getRels().get(0));

        if (!colsAndValues[0].equals("")) insertEdgesString.append(colsAndValues[0]).append(", idl, idr, type) ");
        else insertEdgesString.append("idl, idr, type) ");

        insertEdgesString.append("VALUES ((");
        if (!colsAndValues[1].equals("")) insertEdgesString.append(colsAndValues[1]).append(", ");

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

        String relType = createC.getRels().get(0).getType();

        insertEdgesString.append(selectA).append(", (")
                .append(selectB).append(", '").append(relType).append("'");

        sql.append("INSERT INTO edges (").append(insertEdgesString.toString()).append("); ");
        sql.append("INSERT INTO e$").append(relType).append(" (").append(insertEdgesString.toString()).append(");");

        return sql;
    }

    private static StringBuilder translateInsertNodes(StringBuilder sql, MatchClause createC) {
        String[] colsAndValues;

        for (int i = 0; i < 2; i++) {
            String relation = InsertUtils.findRelation(createC, i);
            colsAndValues = InsertUtils.findColsAndValues(createC, i);

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
            int j = 0;
            for (String col : colsAndValues[0].split(", ")) {
                sql.append(col).append(" = ").append(values[j++]).append(" AND ");
            }
            sql.setLength(sql.length() - 5);
            sql.append("), '").append(relation.replace("_", ", ")).append("'); ");
        }

        return sql;
    }

    public static String translateDelete(DecodedQuery decodedQuery) {
        StringBuilder sql = new StringBuilder();
        MatchClause deleteC = decodedQuery.getMc();
        String relation = InsertUtils.findRelation(deleteC, 0);
        String[] colsAndValues = InsertUtils.findColsAndValues(deleteC, 0);

        // delete the relationships belonging to the node/nodes.
        sql = deleteFromEdgeRelations(sql, colsAndValues);

        sql.append("DELETE FROM nodes WHERE ");

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

    private static StringBuilder deleteFromEdgeRelations(StringBuilder sql, String[] colsAndValues) {
        StringBuilder whereString = new StringBuilder();

        sql.append("DELETE FROM edges WHERE idl in (SELECT id FROM nodes WHERE ");

        String[] values = colsAndValues[1].split(", ");
        int i = 0;
        for (String col : colsAndValues[0].split(", ")) {
            whereString.append(col).append(" = ").append(values[i++]).append(" AND ");
        }

        if (whereString.toString().endsWith(" AND ")) whereString.setLength(whereString.length() - 5);

        sql.append(whereString).append(") OR idr in (SELECT id FROM nodes WHERE ").append(whereString).append("); ");

        for (String s : Cyp2SQL_v3_Apoc.relsList) {
            sql.append(" DELETE FROM e$").append(s).append(" WHERE idl in (SELECT id FROM nodes WHERE ");
            sql.append(whereString).append("); ");
        }

        return sql;
    }

    /**
     * Append ORDER BY clause to the SQL statement.
     *
     * @param orderC Order Clause object generated during the translation process.
     * @param sql    Original SQL built up already.
     * @return New SQL StringBuilder object with ORDER BY clause appended to the end.
     */
    public static StringBuilder obtainOrderByClause(OrderClause orderC, StringBuilder sql) {
        sql.append(" ");
        sql.append("ORDER BY ");

        int nodeTableCount = 0;

        for (CypOrder cO : orderC.getItems()) {
            nodeTableCount++;
            if (cO.getField().startsWith("count")) {
                sql.append("count(n01) ").append(cO.getAscOrDesc()).append(", ");
                break;
            }
            sql.append("n0").append(nodeTableCount).append(".").append(cO.getField()).append(" ")
                    .append(cO.getAscOrDesc());
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
     * @throws IOException Error reading the associated metafile from the workarea location.
     */
    private static StringBuilder obtainGroupByClause(ReturnClause rc, StringBuilder sql) throws IOException {
        sql.append(" GROUP BY ");

        int nodeTableCount = 0;
        ArrayList<String> nodesSeenSoFar = new ArrayList<>();

        for (CypReturn cR : rc.getItems()) {
            if (!nodesSeenSoFar.contains(cR.getNodeID())) {
                nodesSeenSoFar.add(cR.getNodeID());
                nodeTableCount++;
            }

            if (cR.getField() != null && !cR.getCount()) {
                sql.append("n0").append(nodeTableCount).append(".").append(cR.getField()).append(", ");
            } else if (!cR.getCount()) {
                FileInputStream fis = new FileInputStream(Cyp2SQL_v3_Apoc.workspaceArea + "/meta.txt");
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String line;
                while ((line = br.readLine()) != null) {
                    sql.append("n0").append(nodeTableCount).append(".").append(line).append(", ");
                }
            }
        }

        sql.setLength(sql.length() - 2);
        return sql;
    }
}
