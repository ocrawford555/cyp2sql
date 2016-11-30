package query_translation;

import clauseObjects.CypNode;
import clauseObjects.CypReturn;
import clauseObjects.ReturnClause;
import clauseObjects.WhereClause;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import production.c2sqlV2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

class TranslateUtils {
    static StringBuilder getWholeWhereClause(StringBuilder sql, CypNode cN, WhereClause wc) {
        JsonObject obj = cN.getProps();
        Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
        String booleanOp = "";
        for (Map.Entry<String, JsonElement> entry : entries) {
            sql.append("n.").append(entry.getKey());
            // TODO: fix for more advanced WHERE clauses. Currently only working for one AND or OR clause.
            booleanOp = (wc == null) ? "and" : (wc.isHasOr()) ? "and" : (wc.isHasAnd()) ? "and" : "and";
            sql = TranslateUtils.addWhereClause(sql, entry, booleanOp);
        }
        sql.setLength(sql.length() - (booleanOp.length() + 1));
        return sql;
    }


    /**
     * Formats the WHERE part of the SQL query, depending on the boolean operator
     * in the original Cypher WHERE clause.
     *
     * @param sql
     * @param entry
     * @param booleanOp
     * @return
     */
    static StringBuilder addWhereClause(StringBuilder sql, Map.Entry<String, JsonElement> entry, String booleanOp) {
        String value = entry.getValue().getAsString();

        if (!value.contains("#")) value = "eq#" + value + "#qe";

        String prop = sql.toString().substring(sql.toString().lastIndexOf(" ") + 1);

        if (value.contains("~")) {
            sql.setLength(sql.length() - prop.length());
            sql.append("(");
            sql.append(prop);
        }

        sql = getProperWhereValue(value, sql);

        if (!value.contains("~")) {
            sql.append(booleanOp).append(" ");
            return sql;
        } else {
            sql.append(value.split("~")[1]).append(" ").append(prop);

            value = value.split("~")[2];

            sql = getProperWhereValue(value, sql);
            sql.append(")");
        }

        sql.append(booleanOp).append(" ");
        return sql;
    }

    private static StringBuilder getProperWhereValue(String value, StringBuilder sql) {
        String v = "";

        // presumes only one AND or OR per property.
        if (value.startsWith("eq#")) {
            sql.append(" = ");
            v = value.substring(3, value.indexOf("#qe"));
        } else if (value.startsWith("ne#")) {
            sql.append(" <> ");
            v = value.substring(3, value.indexOf("#en"));
        } else if (value.startsWith("lt#")) {
            sql.append(" < ");
            v = value.substring(3, value.indexOf("#tl"));
        } else if (value.startsWith("gt#")) {
            sql.append(" > ");
            v = value.substring(3, value.indexOf("#tg"));
        } else if (value.startsWith("le#")) {
            sql.append(" <= ");
            v = value.substring(3, value.indexOf("#el"));
        } else if (value.startsWith("ge#")) {
            sql.append(" >= ");
            v = value.substring(3, value.indexOf("#eg"));
        }

        sql.append("'").append(v.replace("'", "")).append("' ");

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
