package database;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import production.c2sqlV2;
import schemaConversion.SchemaTranslate;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class containing all methods required to store results of schema conversion from Neo4J to
 * a relational backend database.
 */
public class InsertSchema {
    private static List<String> fieldsForMetaFile = new ArrayList<>();

    /**
     * Executing the various schema parts one by one to the relational backend.
     *
     * @param database Name of the database to store the new schema on.
     */
    public static void executeSchemaChange(String database) {
        DbUtil.createConnection(database);

        String createAdditonalNodeTables = insertEachLabel();
        String sqlInsertNodes = insertNodes();
        String sqlInsertEdges = insertEdges();
        String createMappingQuery = "create table query_mapping (cypher TEXT, sql TEXT, object BYTEA);";
        String createTClosure = "CREATE MATERIALIZED VIEW tclosure AS(WITH RECURSIVE search_graph(idl, idr, depth, path, cycle) " +
                "AS (SELECT e.idl, e.idr, 1, ARRAY[e.idl], false FROM edges e UNION ALL SELECT sg.idl, e.idr, " +
                "sg.depth + 1, path || e.idl, e.idl = ANY(sg.path) FROM edges e, search_graph sg WHERE e.idl = " +
                "sg.idr AND NOT cycle) SELECT * FROM search_graph where (not cycle OR not idr = ANY(path)));";

        try {
            DbUtil.createInsert(createAdditonalNodeTables);
            DbUtil.createInsert(sqlInsertNodes);
            DbUtil.createInsert(sqlInsertEdges);
            DbUtil.createInsert(createMappingQuery);
            DbUtil.createInsert(createTClosure);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        FileOutputStream fos;
        try {
            fos = new FileOutputStream(c2sqlV2.workspaceArea + "/meta.txt");

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

    /**
     * If there is a label which is applied to a node only ever on its own in isolation, then store this as a
     * relation to remove unnecessary NULLs which slow execution of SQL down.
     *
     * @return SQL to execute.
     */
    private static String insertEachLabel() {
        StringBuilder sb = new StringBuilder();
        FileOutputStream fos;
        FileOutputStream fos2;
        try {
            fos = new FileOutputStream(c2sqlV2.workspaceArea + "/meta_tables.txt");
            fos2 = new FileOutputStream(c2sqlV2.workspaceArea + "/meta_labels.txt");

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

            for (String label : SchemaTranslate.labelMappings.keySet()) {
                String tableLabel = label.replace(", ", "_");
                sb.append("CREATE TABLE ").append(tableLabel).append("(");
                sb.append(SchemaTranslate.labelMappings.get(label));
                sb.append("); ");
                bw2.write("*" + tableLabel + "*");
                bw2.newLine();
                for (String y : SchemaTranslate.labelMappings.get(label).replace(" TEXT", "")
                        .replace(" INT", "").split(", ")) {
                    bw2.write(y);
                    bw2.newLine();
                }
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


    /**
     * For each 'label' relation, insert the appropriate values.
     *
     * @param sb    Original SQL that will be appended to.
     * @param label Name of relation.
     * @param o     JSON object containing data to store.
     * @return New SQL statement with correct INSERT INTO statement.
     */
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

    /**
     * Insert all nodes into relational database.
     *
     * @return SQL to execute.
     */
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

    /**
     * Data/properties of the nodes to store.
     *
     * @param sb Original SQL to append data t0.
     * @return New SQL
     */
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

    /**
     * Insert relationships into SQL.
     *
     * @return SQL to execute.
     */
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


    /**
     * Insert properties of the relationships to SQL.
     *
     * @param sb Original SQL to append data to.
     * @return New SQL with data inserted into it.
     */
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

    /**
     * Get correct insert string based on whether data is INT or TEXT.
     *
     * @param z
     * @param sb
     * @param o
     * @return
     */
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
