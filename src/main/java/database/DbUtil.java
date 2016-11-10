package database;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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
                if (colNames.size() == 0) {
                    writer.println("" + " : " + as.get(i));
                } else {
                    for (String column : colNames) {
                        if (!column.equals("id")) writer.println(column + " : " + as.get(i));
                        i++;
                    }
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
            for (int y = 1; y < rsm.getColumnCount(); y++) {
                feed.add(rsm.getColumnName(y));
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

    private static StringBuilder insertTableData(Map<String, String> schemaToBuild, StringBuilder sb, String tableName,
                                                 Map<String, String> fields) {
        sb.append("INSERT INTO ");
        sb.append(tableName).append("(");
        for (String y : fields.keySet()) {
            sb.append(y).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");

        sb.append(" VALUES ");

        String[] jsonObjs = schemaToBuild.get(tableName).split(", ");

        for (String obj : jsonObjs) {
            JsonParser parser = new JsonParser();
            JsonObject o = parser.parse(obj).getAsJsonObject();
            sb.append("(");
            for (String z : fields.keySet()) {
                try {
                    if (fields.get(z).equals("INT")) {
                        int value = o.get(z).getAsInt();
                        sb.append(value).append(", ");
                    } else {
                        // is text
                        String value = o.get(z).getAsString();
                        sb.append("'").append(value).append("', ");
                    }
                } catch (NullPointerException npe) {
                    sb.append("null, ");
                }
            }
            sb.setLength(sb.length() - 2);
            sb.append("), ");
        }

        sb.setLength(sb.length() - 2);
        sb.append(";");

        return sb;
    }


    public static void insertSchema(Map<String, String> schemaToBuild, String database) {
        DbUtil.createConnection(database);
        try {
            // iterate over each table to create
            for (String s : schemaToBuild.keySet()) {
                String sql = obtainSQL(s, schemaToBuild);
                createInsert(sql);
            }
        } catch (SQLException sqle) {
            System.err.println("FAILED : could not execute SQL for schema - " +
                    sqle.getMessage());
        } finally {
            DbUtil.closeConnection();
        }
    }

    private static String obtainSQL(String s, Map<String, String> schemaToBuild) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        sb.append(s);

        JsonParser parser = new JsonParser();
        Map<String, String> fields = getFieldsForDatabase(s, parser, schemaToBuild);

        sb.append("(");
        for (String x : fields.keySet()) {
            sb.append(x).append(" ");
            sb.append(fields.get(x)).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("); ");

        // insert the data to the table
        sb = insertTableData(schemaToBuild, sb, s, fields);

        // turn the stringbuilder into a string
        return sb.toString();
    }

    private static Map<String, String> getFieldsForDatabase(String s, JsonParser parser,
                                                            Map<String, String> schemaToBuild) {
        String[] jsonObjs = schemaToBuild.get(s).split(", ");
        Map<String, String> fields = new LinkedHashMap<>();

        for (String jobj : jsonObjs) {
            JsonObject o = parser.parse(jobj).getAsJsonObject();
            Set<Map.Entry<String, JsonElement>> entries = o.entrySet();

            for (Map.Entry<String, JsonElement> entry : entries) {
                if (!fields.containsKey(entry.getKey()))
                    try {
                        Integer.parseInt(entry.getValue().getAsString());
                        fields.put(entry.getKey(), "INT");
                    } catch (NumberFormatException nfe) {
                        fields.put(entry.getKey(), "TEXT");
                    }
            }
        }

        return fields;
    }
}
