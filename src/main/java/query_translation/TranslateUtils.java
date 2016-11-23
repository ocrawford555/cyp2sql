package query_translation;


import clauseObjects.CypNode;
import com.google.gson.JsonElement;

import java.util.Map;

public class TranslateUtils {
    /**
     * Formats the WHERE part of the SQL query, depending on the boolean operator
     * in the original Cypher WHERE clause.
     *
     * @param sql
     * @param entry
     * @return
     */
    public static StringBuilder addWhereClause(StringBuilder sql, Map.Entry<String, JsonElement> entry) {
        String value = entry.getValue().getAsString();
        if (value.startsWith("<#") && value.endsWith("#>")) {
            sql.append(" <> ");
            value = value.substring(2, value.length() - 2);
        } else {
            sql.append(" = ");
        }
        sql.append("'").append(value.replace("'", ""));
        sql.append("' AND ");
        return sql;
    }

    /**
     * @param cN
     * @return
     */
    public static String genLabelLike(CypNode cN) {
        String label = cN.getType();
        String stmt = "'%";
        String[] labels = label.split(", ");
        for (String l : labels) {
            stmt = stmt + l + "%' AND n.label LIKE '%";
        }
        stmt = stmt.substring(0, stmt.length() - 19);
        return stmt;
    }
}
