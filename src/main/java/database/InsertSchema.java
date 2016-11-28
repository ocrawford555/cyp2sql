package database;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import production.c2sqlV1;
import schemaConversion.SchemaTranslate;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class InsertSchema {
    private static List<String> fieldsForMetaFile = new ArrayList<>();

    public static void executeSchemaChange(String database) {
        DbUtil.createConnection(database);
        String createAdditonalNodeTables = insertEachLabel();
        String sqlInsertNodes = insertNodes();
        String sqlInsertEdges = insertEdges();
        String createMappingQuery = "create table query_mapping (cypher TEXT, sql TEXT, object BYTEA);";

        try {
            DbUtil.createInsert(createAdditonalNodeTables);
            DbUtil.createInsert(sqlInsertNodes);
            DbUtil.createInsert(sqlInsertEdges);
            DbUtil.createInsert(createMappingQuery);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        FileOutputStream fos;
        try {
            fos = new FileOutputStream(c2sqlV1.workspaceArea + "/meta.txt");

            //Construct BufferedReader from InputStreamReader
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (String s : fieldsForMetaFile) {
                bw.write(s);
                bw.newLine();
            }
            bw.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        DbUtil.closeConnection();
    }

    private static String insertEachLabel() {
        StringBuilder sb = new StringBuilder();
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(c2sqlV1.workspaceArea + "/meta_tables.txt");
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (String label : SchemaTranslate.labelMappings.keySet()) {
                String tableLabel = label.replace(", ", "_");
                sb.append("CREATE TABLE ").append(tableLabel).append("(");
                sb.append(SchemaTranslate.labelMappings.get(label));
                sb.append("); ");
                bw.write(tableLabel);
                bw.newLine();
            }
            bw.close();
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return sb.toString();
    }

    private static StringBuilder insertDataForLabels(StringBuilder sb, String label, JsonObject o) {
        String tableLabel = label.replace(", ", "_");
        sb.append("INSERT INTO ").append(tableLabel).append("(");

        for (String prop : SchemaTranslate.labelMappings.get(label).split(", ")) {
            sb.append(prop.replace(" TEXT", "").replace(" INT", "")).append(", ");
        }

        sb.setLength(sb.length() - 2);
        sb.append(") VALUES(");

        for (String z : SchemaTranslate.labelMappings.get(label).split(", ")) {
            sb = getInsertString(z, sb, o);
        }

        sb.setLength(sb.length() - 2);
        sb.append("); ");

        return sb;
    }

    private static String insertNodes() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE nodes(");
        for (String x : SchemaTranslate.nodeRelLabels) {
            sb.append(x).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("); ");

        sb = insertTableDataNodes(sb);
        return sb.toString();
    }

    private static StringBuilder insertTableDataNodes(StringBuilder sb) {
        StringBuilder sbLabels = new StringBuilder();

        sb.append("INSERT INTO nodes (");

        for (String y : SchemaTranslate.nodeRelLabels) {
            sb.append(y.split(" ")[0]).append(", ");
            fieldsForMetaFile.add(y.split(" ")[0]);
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");

        sb.append(" VALUES ");

        try {
            FileInputStream fis = new FileInputStream(SchemaTranslate.nodesFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                JsonParser parser = new JsonParser();
                JsonObject o = (JsonObject) parser.parse(line);
                String label = o.get("label").getAsString();
                sbLabels = insertDataForLabels(sbLabels, label, o);
                sb.append("(");
                for (String z : SchemaTranslate.nodeRelLabels) {
                    sb = getInsertString(z, sb, o);
                }
                sb.setLength(sb.length() - 2);
                sb.append("), ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        sb.setLength(sb.length() - 2);
        sb.append("; ");
        sb.append(sbLabels.toString()).append(";");

        return sb;
    }

    private static String insertEdges() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE edges(");
        for (String x : SchemaTranslate.edgesRelLabels) {
            sb.append(x).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("); ");

        sb = insertTableDataEdges(sb);
        return sb.toString();
    }


    private static StringBuilder insertTableDataEdges(StringBuilder sb) {
        sb.append("INSERT INTO edges (");

        for (String y : SchemaTranslate.edgesRelLabels) {
            sb.append(y.split(" ")[0]).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");

        sb.append(" VALUES ");

        try {
            FileInputStream fis = new FileInputStream(SchemaTranslate.edgesFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                JsonParser parser = new JsonParser();
                JsonObject o = (JsonObject) parser.parse(line);
                sb.append("(");
                for (String z : SchemaTranslate.edgesRelLabels) {
                    sb = getInsertString(z, sb, o);
                }
                sb.setLength(sb.length() - 2);
                sb.append("), ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        sb.setLength(sb.length() - 2);
        sb.append(";");

        return sb;
    }

    private static StringBuilder getInsertString(String z, StringBuilder sb, JsonObject o) {
        try {
            if (z.endsWith("INT")) {
                int value = o.get(z.split(" ")[0]).getAsInt();
                sb.append(value).append(", ");
            } else {
                // is text
                String value = o.get(z.split(" ")[0]).getAsString();
                sb.append("'").append(value).append("', ");
            }
        } catch (NullPointerException npe) {
            sb.append("null, ");
        }
        return sb;
    }
}
