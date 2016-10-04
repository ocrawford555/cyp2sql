package toolv1;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.text.WordUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

class DbUtil {
    static Connection c = null;

    static void createConnection(String dbName) {
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

    static void closeConnection() {
        try {
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void select(String query) throws SQLException {
        Statement stmt;
        stmt = c.createStatement();

        long startMillis = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery(query);
        long finMillis = System.currentTimeMillis();

        while (rs.next()) {
            String name = rs.getString("name");
            System.out.println("NAME : " + WordUtils.capitalizeFully(name));
        }

        stmt.close();
        System.out.println(finMillis - startMillis + " ms.");
    }

    static void createInsert(String query) throws SQLException {
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
                sb.append("'").append(o.get(z).getAsString().replace("\\\"","")).append("', ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("), ");
        }

        sb.setLength(sb.length() - 2);
        sb.append(";");

        return sb;
    }


    static void createAndInsert(Map<String, String> schemaToBuild) {
        try {
            // iterate over each table to create
            for (String s : schemaToBuild.keySet()) {
                StringBuilder sb = new StringBuilder();

                sb.append("CREATE TABLE ");
                sb.append(s);

                String[] jsonObjs = schemaToBuild.get(s).split(", ");

                JsonParser parser = new JsonParser();
                JsonObject o = parser.parse(jsonObjs[0]).getAsJsonObject();
                Set<Map.Entry<String, JsonElement>> entries = o.entrySet();
                ArrayList<String> fields = new ArrayList<String>();

                for (Map.Entry<String, JsonElement> entry : entries) {
                    fields.add(entry.getKey());
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
