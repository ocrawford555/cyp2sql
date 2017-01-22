package database;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import production.Cyp2SQL_v3_Apoc;
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
        String createAdditionalEdgesTables = insertEachRelType();

        long timeStartNodes = System.nanoTime();
        String sqlInsertNodes = insertNodes();
        long timeEndNodes = System.nanoTime();

        System.out.println("TIME TO CREATE NODES RELATION : " + ((timeEndNodes - timeStartNodes) / 1000000.0) + "ms.");

        long timeStartEdges = System.nanoTime();
        String sqlInsertEdges = insertEdges();
        long timeEndEdges = System.nanoTime();

        System.out.println("TIME TO CREATE EDGES RELATION : " + ((timeEndEdges - timeStartEdges) / 1000000.0) + "ms.");

        String createMappingQuery = "create table query_mapping (cypher TEXT, sql TEXT, object BYTEA, " +
                "neoT DOUBLE PRECISION, pgT DOUBLE PRECISION);";

        String createTClosure = "CREATE MATERIALIZED VIEW tclosure AS(WITH RECURSIVE search_graph(idl, idr, depth, " +
                "path, cycle) " +
                "AS (SELECT e.idl, e.idr, 1, ARRAY[e.idl], false FROM edges e UNION ALL SELECT sg.idl, e.idr, " +
                "sg.depth + 1, path || e.idl, e.idl = ANY(sg.path) FROM edges e, search_graph sg WHERE e.idl = " +
                "sg.idr AND NOT cycle) SELECT * FROM search_graph where (not cycle OR not idr = ANY(path)));";

        String forEachFunction = "CREATE FUNCTION doForEachFunc(int[], field TEXT, newV TEXT) RETURNS void AS $$ " +
                "DECLARE x int; r record; l text; BEGIN if array_length($1, 1) > 0 THEN FOREACH x SLICE 0 " +
                "IN ARRAY $1 LOOP FOR r IN SELECT label from nodes where id = x LOOP " +
                "EXECUTE 'UPDATE nodes SET ' || field || '=' || quote_literal(newV) || ' WHERE id = ' || x; " +
                "l := replace(r.label, ', ', '_'); " +
                "EXECUTE 'UPDATE ' || l || ' SET ' || field || '=' || quote_literal(newV) || ' WHERE id = ' || x; " +
                "END LOOP; END LOOP; END IF; END; $$ LANGUAGE plpgsql;";

        try {
            DbUtil.createInsert(createAdditonalNodeTables);
            DbUtil.createInsert(createAdditionalEdgesTables);
            DbUtil.createInsert(sqlInsertNodes);
            DbUtil.createInsert(sqlInsertEdges);
            DbUtil.createInsert(createMappingQuery);
            // tclosure can take up excessive disk space and time - do not run unless sure is ok!
            //DbUtil.createInsert(createTClosure);
            DbUtil.createInsert(forEachFunction);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        addFieldsToMetaFile();
        DbUtil.closeConnection();
    }

    private static void addFieldsToMetaFile() {
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(Cyp2SQL_v3_Apoc.workspaceArea + "/meta.txt");

            //Construct BufferedReader from InputStreamReader
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (String s : fieldsForMetaFile) {
                bw.write(s);
                bw.newLine();
            }
            bw.close();
            fos.close();

            fos = new FileOutputStream(Cyp2SQL_v3_Apoc.workspaceArea + "/meta_rels.txt");

            //Construct BufferedReader from InputStreamReader
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (String s : SchemaTranslate.relTypes) {
                bw.write(s);
                bw.newLine();
            }
            bw.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            fos = new FileOutputStream(Cyp2SQL_v3_Apoc.workspaceArea + "/meta_tables.txt");
            fos2 = new FileOutputStream(Cyp2SQL_v3_Apoc.workspaceArea + "/meta_labels.txt");

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

            for (String label : SchemaTranslate.labelMappings.keySet()) {
                String tableLabel = label.replace(", ", "_");
                sb.append("CREATE TABLE ").append(tableLabel).append("(");
                sb.append(SchemaTranslate.labelMappings.get(label));
                sb.append("); ");
                bw2.write("*" + tableLabel + "*");
                bw2.newLine();
                for (String y : SchemaTranslate.labelMappings.get(label).replace(" TEXT[]", "")
                        .replace(" BIGINT", "").replace(" INT", "").replace(" TEXT", "").split(", ")) {
                    bw2.write(y);
                    bw2.newLine();
                }
                bw.write(tableLabel);
                bw.newLine();
            }

            bw.close();
            bw2.close();
            fos.close();
            fos2.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return sb.toString();
    }

    private static String insertEachRelType() {
        StringBuilder sb = new StringBuilder();

        for (String rel : SchemaTranslate.relTypes) {
            // specific relationship types will be stored in the following
            // format, with the name of the relation being e${type of relationship}
            String relTableName = "e$" + rel;

            sb.append("CREATE TABLE ").append(relTableName).append("(");

            for (String x : SchemaTranslate.edgesRelLabels) {
                sb.append(x).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("); ");
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
            sb.append(prop.replace(" TEXT[]", "").replace(" BIGINT", "").replace(" TEXT", "").replace(" INT", ""))
                    .append(", ");
        }

        sb.setLength(sb.length() - 2);
        sb.append(") VALUES(");

        for (String z : SchemaTranslate.labelMappings.get(label).split(", ")) {
            sb.append(getInsertString(z, o));
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
                    sb.append(getInsertString(z, o));
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
        StringBuilder sbTypes = new StringBuilder();

        String columns = "";

        for (String y : SchemaTranslate.edgesRelLabels) {
            columns = columns + y.split(" ")[0] + ", ";
        }
        columns = columns.substring(0, columns.length() - 2);
        columns = columns + ")";
        sb.append(columns);

        sb.append(" VALUES ");

        try {
            FileInputStream fis = new FileInputStream(SchemaTranslate.edgesFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                JsonParser parser = new JsonParser();
                JsonObject o = (JsonObject) parser.parse(line);
                sb.append("(");
                String values = "";
                for (String z : SchemaTranslate.edgesRelLabels) {
                    String v = getInsertString(z, o);
                    values = values + v;
                    sb.append(v);
                }
                values = values.substring(0, values.length() - 2);
                sbTypes = addType(sbTypes, columns, o, values);
                sb.setLength(sb.length() - 2);
                sb.append("), ");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        sb.setLength(sb.length() - 2);
        sb.append(";").append(" ").append(sbTypes.toString());

        return sb;
    }

    private static StringBuilder addType(StringBuilder sbTypes, String columns, JsonObject o, String values) {
        sbTypes.append("INSERT INTO ");
        sbTypes.append("e$").append(o.get("type").getAsString()).append("(").append(columns);
        sbTypes.append(" VALUES (");
        sbTypes.append(values);
        sbTypes.append(");");
        return sbTypes;
    }

    /**
     * Get correct insert string based on whether data is INT or TEXT.
     *
     * @param z
     * @param o
     * @return
     */
    private static String getInsertString(String z, JsonObject o) {
        String temp;
        try {
            if (z.endsWith("BIGINT")) {
                long value = o.get(z.split(" ")[0]).getAsLong();
                temp = value + ", ";
            } else if (z.endsWith("INT")) {
                int value = o.get(z.split(" ")[0]).getAsInt();
                temp = value + ", ";
            } else if (z.endsWith("[]")) {
                // is text with list property
                JsonArray value = o.get(z.split(" ")[0]).getAsJsonArray();
                temp = "ARRAY" + value.toString().replace("\"", "'") + ", ";
            } else {
                // is just text
                String value = o.get(z.split(" ")[0]).getAsString();
                temp = "'" + value + "', ";
            }
        } catch (NullPointerException npe) {
            temp = "null, ";
        }
        return temp;
    }
}
