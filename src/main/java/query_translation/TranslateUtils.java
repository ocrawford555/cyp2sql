package query_translation;

import clauseObjects.CypNode;
import clauseObjects.CypReturn;
import clauseObjects.ReturnClause;
import com.google.gson.JsonElement;
import production.c2sqlV2;

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
            sql.append("'").append(value.replace("'", ""));
        } else if ((value.startsWith("lt#") && value.endsWith("#tl"))) {
            sql.append(" < ");
            value = value.substring(3, value.length() - 3);
            sql.append("'").append(value.replace("'", ""));
        } else if ((value.startsWith("gt#") && value.endsWith("#tg"))) {
            sql.append(" > ");
            value = value.substring(3, value.length() - 3);
            sql.append("'").append(value.replace("'", ""));
        } else if ((value.startsWith("lt#") && value.endsWith("#tg"))) {
            String prop = sql.toString().substring(sql.toString().lastIndexOf(" ") + 1);
            sql.append(" < '").append(value.substring(3, value.indexOf("#tl"))).append("' AND ");
            sql.append(prop).append(" > '").append(value.substring(value.indexOf("gt#") + 3, value.length() - 3));
        } else if ((value.startsWith("gt#") && value.endsWith("#tl"))) {
            String prop = sql.toString().substring(sql.toString().lastIndexOf(" ") + 1);
            sql.append(" > '").append(value.substring(3, value.indexOf("#tg"))).append("' AND ");
            sql.append(prop).append(" < '").append(value.substring(value.indexOf("lt#") + 3, value.length() - 3));
        } else {
            sql.append(" = ");
            sql.append("'").append(value.replace("'", ""));
        }
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
            fis = new FileInputStream(c2sqlV2.workspaceArea + "/meta_tables.txt");
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

    static String getTable(ReturnClause rc) {
        boolean possibleOpti = true;
        String table = "nodes";

        for (CypReturn cR : rc.getItems()) {
            if (!c2sqlV2.mapLabels.containsKey(cR.getField())) {
                possibleOpti = false;
                break;
            } else {
                String newTable = c2sqlV2.mapLabels.get(cR.getField());
                if (!table.equals(newTable) && !table.equals("nodes")) {
                    possibleOpti = false;
                    break;
                }
                table = newTable;
            }
        }

        if (!possibleOpti) table = "nodes";

        return table;
    }
}
