package production;

import clauseObjects.CypForEach;
import clauseObjects.DecodedQuery;
import database.AddTClosure;
import database.CypherDriver;
import database.DbUtil;
import database.InsertSchema;
import org.apache.commons.io.FileUtils;
import query_translation.*;
import schemaConversion.SchemaTranslate;
import translator.CypherTokenizer;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This is version 3 of the translation tool.
 * This version aims to add additional extensions and features to v2, whilst
 * also improving the performance of the tool as much as possible, having more
 * thorough documentation, and of course, bug fixes.
 * - adding new representation to tool (adjacency lists)
 * - change to how transitive closure is computed/used possibly?
 * - move towards a hybrid tool that chooses graph or relational structure for
 * optimal performance
 * - time permitting, look at extending Cypher language...
 * Output of the whole tool to be cleaner and faster if possible.
 * Milestone end of version 3 should be a product that is at production standard
 * , and has probably performed most of the testing and evaluation for the
 * dissertation.
 * <p>
 * Created by ojc37.
 * Deadline : 19th February 2017 (show demo to supervisor and/or DoS).
 */
public class Cyp2SQL_v3_Apoc {
    // for optimisations based on the return clause of Cypher
    public static final Map<String, String> mapLabels = new HashMap<>();

    // for use in deleting relationships attached to nodes
    public static final ArrayList<String> relsList = new ArrayList<>();

    // work area for manipulating files if necessary
    // currently used when converting the schema.
    public static String workspaceArea;

    // some parameters for the database
    public static String postUN;
    public static String postPW;
    public static String neoUN;
    public static String neoPW;

    // used for comparing the outputs from both Neo4J and Postgres
    public static int numResultsNeo = 0;
    public static int numResultsPost = 0;

    // database name is given to the program as an argument.
    public static String dbName;

    // stores the last decoded query so that the Cypher results module can use it,
    // without having to rerun computation.
    private static DecodedQuery lastDQ = null;

    // variable set at the command line to turn on/off printing to a file the results of a read query.
    private static boolean printBool = false;

    /**
     * Main method when application is launched.
     *
     * @param args arguments to the application.
     *             <-schema|-translate|-s|-t|-t2> <schemaFile|queriesFile> <databaseName> <-p>
     *             or alternatively, one of the special commands,
     *             <-tc> <databaseName> : this is to perform the transitive closure for the graph and store it.
     *             View README for additional guidance.
     */
    public static void main(String args[]) {
        // obtain properties for the program from the properties file.
        C2SProperties props = new C2SProperties();
        String[] fileLocations = props.getLocalProperties();

        // set the properties
        String cypher_results = fileLocations[0];
        String pg_results = fileLocations[1];
        workspaceArea = fileLocations[2];
        postUN = fileLocations[3];
        postPW = fileLocations[4];
        neoUN = fileLocations[5];
        neoPW = fileLocations[6];

        if (args.length == 2 && args[0].equals("-tc")) {
            AddTClosure.addTClosure(args[1]);
        } else if (args.length != 3 && args.length != 4) {
            // error with the command line arguments
            System.err.println("Incorrect usage of Cyp2SQL v2 : " +
                    "<-schema|-translate|-s|-t> <schemaFile|queriesFile> <databaseName> <-p>");
            System.exit(1);
        } else {
            File f_cypher = new File(cypher_results);
            File f_pg = new File(pg_results);

            dbName = args[2];

            // used to fix with SSLEngine issues with Neo4J
            if (!dbName.equals(fileLocations[7])) {
                CypherDriver.resetSSLNeo4J();
                props.setDatabaseProperty(dbName);
            }

            if (args.length == 4 && args[3].equals("-p")) {
                printBool = true;
            }

            System.out.println("PRINT TO FILE : " + ((printBool) ? "enabled" : "disabled"));
            System.out.println("DATABASE RUNNING : " + dbName);

            switch (args[0]) {
                case "-schema":
                case "-s":
                    // perform the schema translation
                    convertNeo4JToSQL(args[1]);
                    break;
                case "-translate":
                case "-t":
                case "-t2":
                    // translate Cypher queries to SQL.
                    getLabelMapping();
                    if (!printBool) {
                        for (int i = -10; i <= 10; i++) {
                            if (i < 1) System.out.println("Warming up - iterations left : " + (i * -1));
                            translateCypherToSQL(args[1], f_cypher, f_pg, cypher_results, pg_results, i, args[0]);
                        }
                    } else translateCypherToSQL(args[1], f_cypher, f_pg, cypher_results,
                            pg_results, 1, args[0]);
                    break;
                default:
                    // error with the command line arguments
                    System.err.println("Incorrect usage of Cyp2SQL v2 : " +
                            "<-schema|-translate|-s|-t> <schemaFile|queriesFile> <databaseName> <-p>");
                    System.exit(1);
            }
        }
    }

    /**
     * Convert Neo4J schema to a relational schema.
     *
     * @param dumpFile Generated BY THE USER from Neo4J shell (see README)
     */
    private static void convertNeo4JToSQL(String dumpFile) {
        System.out.println("\n***CONVERTING THE SCHEMA***\n");
        SchemaTranslate.translate(dumpFile);
        System.out.println("\n***INSERTING THE SCHEMA TO THE DATABASE***\n");
        InsertSchema.executeSchemaChange(dbName);
    }

    /**
     * Read local meta files created from the schema translation, and obtain the information stored
     * within them to help the query translation tool. This includes reading all the possible values
     * for labels of the nodes (used by the output module), and all the possible types of
     * relationships (used when deleting nodes from the relational schema).
     */
    private static void getLabelMapping() {
        // open file and read in property keys, removing duplicates as they are of no use.
        FileInputStream fis;
        try {
            fis = new FileInputStream(Cyp2SQL_v3_Apoc.workspaceArea + "/meta_labels.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            String currentLabelType = null;

            // ArrayList to keep track of duplicates.
            ArrayList<String> dupProps = new ArrayList<>();

            while ((line = br.readLine()) != null) {
                if (line.startsWith("*")) {
                    // all the following lines in the file are properties belonging to this label type.
                    currentLabelType = line.substring(1, line.length() - 1);
                } else {
                    if (mapLabels.containsKey(line)) {
                        dupProps.add(line);
                    } else {
                        mapLabels.put(line, currentLabelType);
                    }
                }
            }

            for (String s : dupProps) {
                if (mapLabels.containsKey(s))
                    mapLabels.remove(s);
            }

            br.close();
            fis.close();

            fis = new FileInputStream(Cyp2SQL_v3_Apoc.workspaceArea + "/meta_rels.txt");
            br = new BufferedReader(new InputStreamReader(fis));
            while ((line = br.readLine()) != null) relsList.add(line);
            br.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Translating the queries in the file to SQL and executing them.
     *
     * @param translateFile  String file location containing a list of Cypher queries.
     * @param f_cypher       File object - the output from the Neo4J Java driver will be sent here.
     * @param f_pg           File object - the output from the JDBC driver will be sent here.
     * @param cypher_results String file location of f_cypher.
     * @param pg_results     String file location of f_pg.
     * @param typeTranslate
     */
    private static void translateCypherToSQL(String translateFile, File f_cypher, File f_pg, String cypher_results,
                                             String pg_results, int repeatCount, String typeTranslate) {
        try {
            FileInputStream fis = new FileInputStream(translateFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            String sql;

            while ((line = br.readLine()) != null) {
                // if line is commented out in the read queries file, then do not attempt to convert it.
                if (!line.startsWith("//") && !line.isEmpty()) {
                    //Object[] mapping = DbUtil.getMapping(line, dbName);
                    Object[] mapping = {null, null};
                    String[] returnItemsForCypher;

                    if (mapping[0] != null) {
                        sql = (String) mapping[0];
                        returnItemsForCypher = (String[]) mapping[1];
                    } else {
                        if (line.toLowerCase().contains(" foreach ")) {
                            sql = convertCypherForEach(line, typeTranslate);
                        } else if (line.toLowerCase().contains(" with ")) {
                            sql = convertCypherWith(line, typeTranslate);
                        } else if (line.toLowerCase().contains("allshortestpaths")) {
                            sql = convertCypherASP(line);
                        } else {
                            sql = convertCypherToSQL(line, typeTranslate).getSqlEquiv();
                        }

                        returnItemsForCypher = null;

                        if (sql != null && !sql.startsWith("INSERT") && !sql.startsWith("DELETE")) {
                            returnItemsForCypher = lastDQ.getCypherAdditionalInfo().getReturnClause()
                                    .replace(" ", "").split(",");
                        }
                    }

                    if (sql != null) {
                        executeSQL(sql, pg_results, printBool);
                    } else throw new Exception("Conversion of SQL failed");

                    CypherDriver.run(line, cypher_results, returnItemsForCypher, printBool);

                    try {
                        if (numResultsNeo != numResultsPost) throw new Exception();
                    } catch (Exception e) {
                        System.err.println("\n**********Statements do not appear to " +
                                "be logically correct - please check\n" + line + "\n" + sql + "\n***********");
                        printSummary(line, sql, f_cypher, f_pg);
                        System.exit(1);
                    }

                    if (repeatCount > 0) {
                        printSummary(line, sql, f_cypher, f_pg);
                        DbUtil.insertMapping(line, sql, returnItemsForCypher, dbName);
                    }

                    resetExecTimes();
                }
            }
            br.close();
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String convertCypherASP(String line) throws Exception {
        String path = line.substring(line.indexOf("(") + 1, line.indexOf("RETURN") - 2);
        String returnClause = line.substring(line.indexOf("RETURN"));
        String cypherPathQuery = "MATCH " + path + " " + returnClause;
        DecodedQuery dQMainPath = CypherTokenizer.decode(cypherPathQuery, false);
        lastDQ = dQMainPath;
        return SQLAllShortestPaths.translate(dQMainPath).toString();
    }

    /**
     * Print summary of the translation.
     *
     * @param line     Cypher query.
     * @param sql      SQL equivalent.
     * @param f_cypher File containing Neo4J output.
     * @param f_pg     File containing Postgres output.
     * @throws IOException Error comparing the files f_cypher and f_pg.
     */
    private static void printSummary(String line, String sql, File f_cypher, File f_pg) throws IOException {
        System.out.println("\n**********\nCypher Input : " + line);
        System.out.println("SQL Output: " + sql + "\nExact Result: " +
                FileUtils.contentEquals(f_cypher, f_pg) + "\nNumber of records from Neo4J: " +
                numResultsNeo + "\nNumber of results from PostG: " + numResultsPost +
                "\nTime on Neo4J: \t\t" + (CypherDriver.lastExecTime / 1000000.0) +
                " ms.\nTime on Postgres: \t" + ((DbUtil.lastExecTimeRead + DbUtil.lastExecTimeCreate +
                DbUtil.lastExecTimeInsert)
                / 1000000.0) +
                " ms.\n**********\n");
    }

    /**
     * Converting correctly Cypher queries with the keyword FOREACH.
     *
     * @param line          Cypher input.
     * @param typeTranslate
     * @return SQL equivalent of input.
     */
    private static String convertCypherForEach(String line, String typeTranslate) {
        String changeLine = line.toLowerCase().replace("with", "return");
        String[] feParts = changeLine.toLowerCase().split(" foreach ");
        DecodedQuery dQ = convertCypherToSQL(feParts[0].trim() + ";", typeTranslate);
        CypForEach cypForEach = new CypForEach(feParts[1].trim());
        return SQLForEach.genQuery(dQ.getSqlEquiv(), cypForEach);
    }

    /**
     * Reset measured performance times.
     */
    private static void resetExecTimes() {
        CypherDriver.lastExecTime = 0;
        DbUtil.lastExecTimeCreate = 0;
        DbUtil.lastExecTimeRead = 0;
        DbUtil.lastExecTimeInsert = 0;
    }

    /**
     * Converting correctly Cypher queries with the keyword WITH.
     *
     * @param line          Cypher input.
     * @param typeTranslate
     * @return SQL equivalent.
     */
    private static String convertCypherWith(String line, String typeTranslate) {
        String changeLine = line.toLowerCase().replace("with", "return");
        String[] withParts = changeLine.toLowerCase().split("where");
        DecodedQuery dQ = convertCypherToSQL(withParts[0] + ";", typeTranslate);

        String withTemp = null;
        if (dQ != null) {
            withTemp = SQLWith.genTemp(dQ.getSqlEquiv());
        }

        String sqlSelect = SQLWith.createSelect(withParts[1].trim(), dQ);
        return withTemp + " " + sqlSelect;
    }

    /**
     * Execute the SQL command on the database.
     * If query is a concatenation of multiple queries, then perform then
     * one by one in order that was passed to the method.
     * Type of method call depends on whether or not query begins with CREATE
     * or not.
     *
     * @param sql         SQL to execute.
     * @param pg_results  File to store the results.
     * @param printOutput Write the results to a file for viewing.
     */
    private static void executeSQL(String sql, String pg_results, boolean printOutput) {
        try {
            String indivSQL[] = sql.split(";");
            for (String q : indivSQL) {
                if (q.trim().startsWith("CREATE")) {
                    DbUtil.executeCreateView(q + ";", dbName);
                } else if (q.trim().startsWith("INSERT")) {
                    DbUtil.insertOrDelete(q + ";", dbName);
                } else if (q.trim().startsWith("DELETE")) {
                    DbUtil.insertOrDelete(q + ";", dbName);
                } else
                    DbUtil.select(q + ";", dbName, pg_results, printOutput);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Convert Cypher queries to SQL
     *
     * @param cypher        Original Cypher query to translate.
     * @param typeTranslate
     * @return SQL that maps to the Cypher input.
     */
    private static DecodedQuery convertCypherToSQL(String cypher, String typeTranslate) {
        try {
            if (cypher.toLowerCase().contains(" union all ")) {
                String[] queries = cypher.toLowerCase().split(" union all ");
                ArrayList<String> unionSQL = new ArrayList<>();
                DecodedQuery dQ = null;
                for (String s : queries) {
                    dQ = CypherTokenizer.decode(s, false);
                    unionSQL.add(SQLTranslate.translateRead(dQ, typeTranslate));
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
                    unionSQL.add(SQLTranslate.translateRead(dQ, typeTranslate));
                }
                dQ.setSqlEquiv(SQLUnion.genUnion(unionSQL, "UNION"));
                lastDQ = dQ;
                return dQ;
            } else {
                DecodedQuery dQ = CypherTokenizer.decode(cypher, false);

                if (dQ.getRc() != null) {
                    // the translation is for a read query.
                    dQ.setSqlEquiv(SQLTranslate.translateRead(dQ, typeTranslate));
                } else {
                    if (dQ.getCypherAdditionalInfo().hasDelete()) {
                        // the translation is a delete query.
                        dQ.setSqlEquiv(SQLTranslate.translateDelete(dQ));
                    } else {
                        // the translation is an insert query.
                        dQ.setSqlEquiv(SQLTranslate.translateInsert(dQ));
                    }
                }
                lastDQ = dQ;
                return dQ;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}