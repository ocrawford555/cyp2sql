package database;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import schemaConversion.SchemaTranslate;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;

public class DbUtil {
    private static Connection c = null;
    private static int numRecords = 0;
    private static boolean DB_OPEN = false;

    public static void createConnection(String dbName) {
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/" + dbName,
                            "postgres", "OJC9511abc");
            DB_OPEN = true;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void closeConnection() {
        try {
            c.close();
            DB_OPEN = false;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void executeCreateView(String query, String dbName) throws SQLException {
        if (!DB_OPEN) DbUtil.createConnection(dbName);
        Statement stmt = c.createStatement();
        stmt.executeUpdate(query);
        stmt.close();
    }

    public static void select(String query, String database) throws SQLException {
        if (!DB_OPEN) DbUtil.createConnection(database);
        Statement stmt;
        stmt = c.createStatement();

        ArrayList<ArrayList<String>> results = getQueryResult(query, stmt);
        ArrayList<String> colNames = results.get(0);
        results.remove(0);

        PrintWriter writer;
        try {
            String file_location_results = "C:/Users/ocraw/Desktop/pg_results.txt";
            writer = new PrintWriter(file_location_results, "UTF-8");
            for (ArrayList<String> as : results) {
                int i = 0;
                for (String column : colNames) {
                    if (!column.equals("id")) writer.println(column + " : " + as.get(i));
                    i++;
                }
            }
            writer.println();
            writer.println("NUM RECORDS : " + numRecords);
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        DbUtil.closeConnection();
    }

    private static ArrayList<ArrayList<String>> getQueryResult(String query, Statement stm) {
        ArrayList<ArrayList<String>> feedback = new ArrayList<>();
        ArrayList<String> feed;

        try {
            ResultSet rs = stm.executeQuery(query);

            ResultSetMetaData rsm = rs.getMetaData();

            feed = new ArrayList<>();
            for (int y = 0; y < rsm.getColumnCount(); y++) {
                feed.add(rsm.getColumnName(y + 1));
            }
            feedback.add(feed);

            numRecords = 0;

            while (rs.next()) {
                feed = new ArrayList<>();
                for (int i = 1; i <= rsm.getColumnCount(); i++) {
                    feed.add(rs.getString(i));
                }
                feedback.add(feed);
                numRecords++;
            }

            stm.close();
        } catch (SQLException e) {
            //handler
        }
        return feedback;
    }

    private static void createInsert(String query) throws SQLException {
        Statement stmt = c.createStatement();
        stmt.executeUpdate(query);
        stmt.close();
    }

    private static StringBuilder insertTableDataNodes(StringBuilder sb) {
        sb.append("INSERT INTO nodes (");

        for (String y : SchemaTranslate.nodeRelLabels) {
            sb.append(y.split(" ")[0]).append(", ");
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
        sb.append(";");

        return sb;
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


    public static void insertSchema(String database) {
        DbUtil.createConnection(database);
        String sqlInsertNodes = insertNodes();
        String sqlInsertEdges = insertEdges();

        try {
            DbUtil.createInsert(sqlInsertNodes);
            DbUtil.createInsert(sqlInsertEdges);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        DbUtil.closeConnection();
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
}
