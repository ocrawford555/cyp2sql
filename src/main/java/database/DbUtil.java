package database;

import production.c2sqlV2;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;

/**
 * Database driver for Postgres. Runs SQL, and parses result into appropriate text file.
 */
public class DbUtil {
    private static Connection c = null;
    private static int numRecords = 0;
    private static boolean DB_OPEN = false;

    /**
     * Create the initial connection to the database.
     *
     * @param dbName Name of the database to connect to.
     */
    public static void createConnection(String dbName) {
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/"
                            + dbName, c2sqlV2.postUN, c2sqlV2.postPW);
            DB_OPEN = true;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    /**
     * Close connection to the database.
     */
    public static void closeConnection() {
        try {
            c.close();
            DB_OPEN = false;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * If the SQL begins with a CREATE keyword, then this query should be executed
     * and committed first before other queries execute. This is needed when TEMP
     * views are created.
     *
     * @param query  SQL to execute (beginning with CREATE)
     * @param dbName Database name to execute on.
     * @throws SQLException
     */
    public static void executeCreateView(String query, String dbName) throws SQLException {
        if (!DB_OPEN) DbUtil.createConnection(dbName);
        Statement stmt = c.createStatement();
        stmt.executeUpdate(query);
        stmt.close();
    }

    /**
     * Execute standard read SQL statement.
     *
     * @param query      SQL statement
     * @param database   Database to execute statement on.
     * @param pg_results File to store the results.
     * @throws SQLException
     */
    public static void select(String query, String database, String pg_results) throws SQLException {
        if (!DB_OPEN) DbUtil.createConnection(database);
        Statement stmt;
        stmt = c.createStatement();

        // obtain the columns returned from the result.
        ArrayList<ArrayList<String>> results = getQueryResult(query, stmt);
        ArrayList<String> colNames = results.get(0);
        results.remove(0);

        PrintWriter writer;
        try {
            writer = new PrintWriter(pg_results, "UTF-8");

            for (ArrayList<String> as : results) {
                int i = 0;
                for (String column : colNames) {
                    if (!column.equals("id") && !column.equals("x") && !column.equals("label")) {
                        writer.println(column + " : " + as.get(i));
                    }
                    i++;
                }
            }

            writer.println();
            writer.println("NUM RECORDS : " + numRecords);
            writer.close();
            c2sqlV2.numResultsPost = numRecords;
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        DbUtil.closeConnection();
    }


    public static Object[] getMapping(String line, String database) throws SQLException, IOException,
            ClassNotFoundException {
        if (!DB_OPEN) DbUtil.createConnection(database);
        String query = "SELECT sql, object FROM query_mapping WHERE cypher = ?";
        PreparedStatement stmt = c.prepareStatement(query);
        stmt.setString(1, line);
        ResultSet rs = stmt.executeQuery();

        String sql = null;
        byte[] bytes = null;

        while (rs.next()) {
            sql = rs.getString(1);
            bytes = rs.getBytes(2);
        }

        ObjectInputStream objectIn = null;
        String[] toReturn = null;
        if (bytes != null) {
            objectIn = new ObjectInputStream(new ByteArrayInputStream(bytes));
            toReturn = (String[]) objectIn.readObject();
        }

        return new Object[]{sql, toReturn};
    }

    /**
     * Obtain results from the database (along with additional metadata such as the columns
     * returned).
     *
     * @param query SQL to execute.
     * @param stm   Internal JDBC statement.
     * @return List of the results and column names.
     */
    private static ArrayList<ArrayList<String>> getQueryResult(String query, Statement stm)
            throws SQLException {
        ArrayList<ArrayList<String>> feedback = new ArrayList<>();
        ArrayList<String> feed;

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
        return feedback;
    }

    public static void insertMapping(String cypher, String sql, Object obj, String dbName)
            throws SQLException, IOException {
        String preparedStatement = "INSERT INTO query_mapping(cypher, sql, object) VALUES (?, ?, ?)";
        if (!DB_OPEN) createConnection(dbName);
        PreparedStatement pstmt = c.prepareStatement(preparedStatement);
        pstmt.setString(1, cypher);
        pstmt.setString(2, sql);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        pstmt.setBytes(3, baos.toByteArray());

        pstmt.executeUpdate();
        pstmt.close();
    }

    static void createInsert(String query) throws SQLException {
        Statement stmt = c.createStatement();
        stmt.executeUpdate(query);
        stmt.close();
    }
}
