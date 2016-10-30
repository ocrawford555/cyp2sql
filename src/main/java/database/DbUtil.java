package database;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class DbUtil {
    private static Connection c = null;
    private static int numRecords = 0;

    public static void createConnection(String dbName) {
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/" + dbName,
                            "postgres", "OJC9511abc");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void closeConnection() {
        try {
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void executeCreateView(String query) throws SQLException {
        Statement stmt = c.createStatement();
        stmt.executeUpdate(query);
        stmt.close();
    }

    public static void select(String query) throws SQLException {
        Statement stmt;
        stmt = c.createStatement();

        //ResultSet rs = stmt.executeQuery((String) query[0]);
        //int countRecords = 0;

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
                    writer.println("name" + " : " + as.get(i));
                } else {
                    for (String column : colNames) {
                        if (!column.equals("id")) writer.println(column + " : " + as.get(i));
                        i++;
                    }
                }
            }
            System.out.println("\nNUM RECORDS : " + numRecords);
            writer.println();
            writer.println("NUM RECORDS : " + numRecords);
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
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
                                                 ArrayList<String> fields) {
        sb.append("INSERT INTO ");
        sb.append(tableName).append("(");
        for (String y : fields) {
            sb.append(y).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");

        sb.append(" VALUES ");

        String[] jsonObjs = schemaToBuild.get(tableName).split(", ");

        for (String x : jsonObjs) {
            JsonParser parser = new JsonParser();
            JsonObject o = parser.parse(x).getAsJsonObject();
            sb.append("(");
            for (String z : fields) {
                try {
                    String value = o.get(z).getAsString().replace("\\\"", "");
                    sb.append("'").append(value).append("', ");
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


    public static void createAndInsert(Map<String, String> schemaToBuild) {
        try {
            // iterate over each table to create
            for (String s : schemaToBuild.keySet()) {
                StringBuilder sb = new StringBuilder();

                sb.append("CREATE TABLE ");
                sb.append(s);

                String[] jsonObjs = schemaToBuild.get(s).split(", ");
                JsonParser parser = new JsonParser();
                ArrayList<String> fields = new ArrayList<>();

                for (String jobj : jsonObjs) {
                    JsonObject o = parser.parse(jobj).getAsJsonObject();
                    Set<Map.Entry<String, JsonElement>> entries = o.entrySet();

                    for (Map.Entry<String, JsonElement> entry : entries) {
                        if (!fields.contains(entry.getKey()))
                            fields.add(entry.getKey());
                    }
                }

                sb.append("(");
                for (String x : fields) {
                    sb.append(x);
                    // default type of each field is TEXT
                    sb.append(" TEXT, ");
                }
                sb.setLength(sb.length() - 2);
                sb.append("); ");

                // insert the data to the table
                sb = insertTableData(schemaToBuild, sb, s, fields);

                // turn the stringbuilder into a string
                String sbString = sb.toString();
                sbString = sbString.replace("\"", "");
                // debug output
                System.out.println(sbString);
                createInsert(sbString);
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            System.exit(0);
        }
    }
}
