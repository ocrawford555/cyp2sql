package production;

import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import database.InsertSchema;
import org.apache.commons.io.FileUtils;
import query_translation.SQLTranslate;
import query_translation.SQLUnion;
import query_translation.SQLWith;
import schemaConversion.SchemaTranslate;
import translator.CypherTokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * This is version 1 of the translation tool.
 * Focuses mainly on the schema translation, and basic query translation.
 * Optimisations, and other features/extensions are not in this version.
 * Created by ojc37.
 * Deadline : 5th December 2016 (show to supervisor and upload to GitHub).
 */
public class c2sqlV1 {
    // work area for manipulating files if necessary
    // currently used when converting the schema.
    public static String workspaceArea;

    public static String postUN;
    public static String postPW;
    public static String neoUN;
    public static String neoPW;

    // used for comparing the outputs from both Neo4J and Postgres
    public static int numResultsNeo = 0;
    public static int numResultsPost = 0;

    // database name is given to the program as an argument.
    private static String dbName;

    private static DecodedQuery lastDQ = null;

    /**
     * Main method when application is launched.
     *
     * @param args arguments to the application.
     *             <-schema|-translate|-s|-t> <schemaFile|queriesFile> <databaseName>
     */
    public static void main(String args[]) {
        C2SProperties props = new C2SProperties();
        String[] fileLocations = props.setLocalProperties();

        String cypher_results = fileLocations[0];
        String pg_results = fileLocations[1];
        workspaceArea = fileLocations[2];
        postUN = fileLocations[3];
        postPW = fileLocations[4];
        neoUN = fileLocations[5];
        neoPW = fileLocations[6];

        if (args.length != 3) {
            System.err.println("Incorrect usage of Cyp2SQL v1 : <schema|translate> " +
                    "<schemaFile|queriesFile> <databaseName>");
            System.exit(1);
        } else {
            File f_cypher = new File(cypher_results);
            File f_pg = new File(pg_results);

            dbName = args[2];

            if (args[0].equals("-schema") || args[0].equals("-s")) {
                // perform the schema translation
                convertNeo4JToSQL(args[1]);
            } else if (args[0].equals("-translate") || args[0].equals("-t")) {
                try {
                    FileInputStream fis = new FileInputStream(args[1]);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                    String line;
                    String sql;

                    while ((line = br.readLine()) != null) {
                        // if line is commented out in the read queries file, then do not attempt to convert it
                        if (!line.startsWith("//")) {
                            long startTimeMillis = System.currentTimeMillis();
                            Object[] mapping = DbUtil.getMapping(line, dbName);

                            String[] returnItemsForCypher;
                            if (mapping[0] != null) {
                                sql = (String) mapping[0];
                                returnItemsForCypher = (String[]) mapping[1];
                            } else {
                                if (line.toLowerCase().contains(" with ")) {
                                    String changeLine = line.toLowerCase().replace("with", "return");
                                    String[] withParts = changeLine.toLowerCase().split("where");
                                    DecodedQuery dQ = convertCypherToSQL(withParts[0] + ";");

                                    String withTemp = null;
                                    if (dQ != null) {
                                        withTemp = SQLWith.genTemp(dQ.getSqlEquiv());
                                    }

                                    String sqlSelect = SQLWith.createSelect(withParts[1].trim(), dQ);
                                    sql = withTemp + " " + sqlSelect;
                                } else {
                                    sql = convertCypherToSQL(line).getSqlEquiv();
                                }

                                returnItemsForCypher = lastDQ.getCypherAdditionalInfo().
                                        getReturnClause().replace(" ", "").split(",");
                            }
                            long endTimeMillis = System.currentTimeMillis();

                            if (sql != null) {
                                executeSQL(sql, pg_results);
                            } else throw new Exception("Conversion of SQL failed");

                            CypherDriver.run(line, cypher_results, returnItemsForCypher);
                            System.out.println("\n**********\nCypher Input : " + line);
                            System.out.println("SQL Output: " + sql + "\nExact Result: " +
                                    FileUtils.contentEquals(f_cypher, f_pg) + "\nNumber of records from Neo4J: " +
                                    numResultsNeo + "\nNumber of results from PostG: " + numResultsNeo + "\nTime : " +
                                    (endTimeMillis - startTimeMillis) +
                                    "\n**********\n");
                            DbUtil.insertMapping(line, sql, returnItemsForCypher, dbName);
                        }
                    }
                    br.close();
                    fis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                System.err.println("Incorrect usage of Cyp2SQL v1 : <schema|translate> " +
                        "<schemaFile|queriesFile> <databaseName>");
                System.exit(1);
            }
        }
    }

    /**
     * Execute the SQL command on the database.
     * If query is a concatenation of multiple queries, then perform then
     * one by one in order that was passed to the method.
     * Type of method call depends on whether or not query begins with CREATE
     * or not.
     *
     * @param sql        SQL to execute.
     * @param pg_results File to store the results.
     */
    private static void executeSQL(String sql, String pg_results) {
        try {
            String indivSQL[] = sql.split(";");
            for (String q : indivSQL) {
                if (q.trim().startsWith("CREATE")) {
                    DbUtil.executeCreateView(q + ";", dbName);
                } else
                    DbUtil.select(q + ";", dbName, pg_results);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Convert Cypher queries to SQL
     *
     * @param cypher Original Cypher query to translate.
     * @return SQL that maps to the Cypher input.
     */
    private static DecodedQuery convertCypherToSQL(String cypher) {
        try {
            if (cypher.toLowerCase().contains(" union all ")) {
                String[] queries = cypher.toLowerCase().split(" union all ");
                ArrayList<String> unionSQL = new ArrayList<>();
                DecodedQuery dQ = null;
                for (String s : queries) {
                    dQ = CypherTokenizer.decode(s, false);
                    unionSQL.add(SQLTranslate.translate(dQ));
                }
                dQ.setSqlEquiv(SQLUnion.genUnion(unionSQL, "UNION ALL"));
                lastDQ = dQ;
                return dQ;
            } else if (cypher.toLowerCase().contains(" union ")) {
                String[] queries = cypher.toLowerCase().split(" union ");
                ArrayList<String> unionSQL = new ArrayList<>();
                DecodedQuery dQ = null;
                for (String s : queries) {
                    dQ = CypherTokenizer.decode(s, false);
                    unionSQL.add(SQLTranslate.translate(dQ));
                }
                dQ.setSqlEquiv(SQLUnion.genUnion(unionSQL, "UNION"));
                lastDQ = dQ;
                return dQ;
            } else {
                DecodedQuery dQ = CypherTokenizer.decode(cypher, false);
                dQ.setSqlEquiv(SQLTranslate.translate(dQ));
                lastDQ = dQ;
                return dQ;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Convert Neo4J schema to Postgres (relational schema)
     *
     * @param dumpFile Generated BY THE USER from Neo4J shell (see README)
     */
    private static void convertNeo4JToSQL(String dumpFile) {
        SchemaTranslate.translate(dumpFile);
        InsertSchema.executeSchemaChange(dbName);
    }
}
