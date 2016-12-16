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
        for (Map.Entry<String, JsonElement> entry : entries) {
            sql.append("n.").append(entry.getKey());
            // TODO: fix for more advanced WHERE clauses. Currently only working for one AND or OR clause
            sql = TranslateUtils.addWhereClause(sql, entry);
            sql.append(" and ");
        }
        sql.setLength(sql.length() - 4);
        return sql;
    }


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

        System.out.println(value);

        // format part of the where clause correctly for further parsing.
        if (!value.contains("#")) value = "eq#" + value + "#qe";

        String prop = sql.toString().substring(sql.toString().lastIndexOf(" ") + 1);
        if (value.contains("~")) {
            sql.setLength(sql.length() - prop.length());
            sql.append("(");
            sql.append(prop);
        }

        sql = getProperWhereValue(value, sql);

        boolean addClosingParen = false;

        while (value.contains("~")) {
            addClosingParen = true;
            sql.append(value.split("~")[1]).append(" ").append(prop);
            String[] valueSplit = value.split("~");
            value = "";
            for (int i = 2; i < valueSplit.length; i++) {
                value += valueSplit[i] + "~";
            }
            value = value.substring(0, value.length() - 1);
            sql = getProperWhereValue(value, sql);
        }

        if (addClosingParen) sql.append(")");
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

    static String addToRelsNeeded(String relsNeeded, String idRel) {
        if (relsNeeded.contains(idRel)) return relsNeeded;
        else return idRel + ", " + relsNeeded;
    }

    /**
     * @param nodeID
     * @param field
     * @param alias  @return
     */
    static String useAlias(String nodeID, String field, Map<String, String> alias) {
        if (alias.isEmpty()) {
            return "";
        } else {
            for (String s : alias.keySet()) {
                String key = s.split(" AS ")[0];
                if (key.startsWith("collect")) key = key.substring(8, key.length() - 1);
                if (field != null) {
                    if (key.equals((nodeID) + "." + (field))) {
                        return (" AS " + alias.get(s));
                    }
                } else {
                    if (key.equals(nodeID)) {
                        return (" AS " + alias.get(s));
                    }
                }
            }
        }
        return "";
    }
}
