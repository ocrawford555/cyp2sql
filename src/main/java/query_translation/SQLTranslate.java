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
 * Read individual method documentation for more understanding.
 */
public class SQLTranslate {
    /**
     * Translate calls other methods, stitching the results together into one SQL query.
     *
     * @param decodedQuery All the intermediate data gathered about the original Cypher query.
     * @return SQL string that maps to the original Cypher command.
     * @throws Exception
     */
    public static String translate(DecodedQuery decodedQuery) throws Exception {
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

            if (decodedQuery.getCypherAdditionalInfo().hasCount())
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


    /**
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
