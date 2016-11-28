package query_translation;


import clauseObjects.CypNode;
import com.google.gson.JsonElement;
import production.c2sqlV1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

class TranslateUtils {
    /**
     * Formats the WHERE part of the SQL query, depending on the boolean operator
     * in the original Cypher WHERE clause.
     *
     * @param sql
     * @param entry
     * @return
     */
    static StringBuilder addWhereClause(StringBuilder sql, Map.Entry<String, JsonElement> entry) {
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
    static String genLabelLike(CypNode cN) {
        String label = cN.getType();
        String stmt = "'%";
        String[] labels = label.split(", ");
        for (String l : labels) {
            stmt = stmt + l + "%' AND n.label LIKE '%";
        }
        stmt = stmt.substring(0, stmt.length() - 19);
        return stmt;
    }

    static String getLabelType(String type) {
        if (type == null)
            return "nodes";

        FileInputStream fis;

        // nodes is the default table with all the data inside it.
        String correctTable = "nodes";

        // integer count to indicate whether or not label type specific enough for this optimisation.
        int changed = 0;

        try {
            fis = new FileInputStream(c2sqlV1.workspaceArea + "/meta_tables.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains(type)) {
                    correctTable = line;
                    changed++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (changed > 1) correctTable = "nodes";

        return correctTable;
    }
}
