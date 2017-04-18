package production;

import clauseObjects.CypForEach;
import clauseObjects.CypIterate;
import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import database.InsertSchema;
import org.apache.commons.io.FileUtils;
import org.neo4j.driver.v1.exceptions.ClientException;
import query_translation.*;
import schemaConversion.SchemaTranslate;
import translator.CypherTokenizer;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

/**
 * This is version 4.0 R1 of the translation tool.
 * <p>
 * First demo release of tool. More info in README.md
 * This will be designed for evaluation of the tool for the purposes of the dissertation.
 * Future versions will be refactoring to the code and more general code suitable
 * for open-source release.
 * <p>
 * Created by Oliver Crawford (ojc37@cam.ac.uk).
 */
public class Reagan_Main_V4 {
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

    // variable set at the command line to indicate that query or queries being run are manipulating records in the db.
    private static boolean updateInsDel = false;

    // variable set at the command line to indicate whether results should be emailed back to the user
    private static boolean emailUser = false;

    // store queries that fail so that they are not run again during the evaluation.
    private static ArrayList<String> denyList = new ArrayList<>();


    /**
     * Main method when application is launched.
     *
     * @param args arguments to the application.
     *             <-schema|-translate|-s|-t|-tc> <schemaFile|queriesFile> <databaseName> <-e|-p|-c>
     *             View README for additional guidance.
     */
    public static void main(String args[]) {
        // obtain properties for the program from the properties file.
        C2SProperties props = new C2SProperties();
        String[] configProps = props.getLocalProperties();

        // set the properties
        String cypher_results = configProps[0];
        String pg_results = configProps[1];
        workspaceArea = configProps[2];
        postUN = configProps[3];
        postPW = configProps[4];
        neoUN = configProps[5];
        neoPW = configProps[6];

        if (args.length < 3 || args.length > 4) {
            // error with the command line arguments
            System.err.println("Incorrect usage of Reagan v4 : " +
                    "<-schema|-translate|-s|-t|-tc> <schemaFile|queriesFile> <databaseName> <-e|-p|-c>");
            System.exit(1);
        } else {
            // create file objects to store results of the file
            File f_cypher = new File(cypher_results);
            File f_pg = new File(pg_results);

            dbName = args[2];

            if (args.length == 4 && args[3].equals("-p")) {
                System.out.println("WARNING: print to be enabled, are you sure? (Y/N)");
                Scanner in = new Scanner(System.in);
                String resp = in.nextLine();
                printBool = resp.toUpperCase().equals("Y");
            } else if (args.length == 4 && args[3].equals("-c")) {
                System.out.println("WARNING: manipulating records in the database, are you sure? (Y/N)");
                Scanner in = new Scanner(System.in);
                String resp = in.nextLine();
                updateInsDel = resp.toUpperCase().equals("Y");
            } else if (args.length == 4 && args[3].equals("-e")) {
                emailUser = true;
            }

            System.out.println("PRINT TO FILE : " + ((printBool) ? "enabled" : "disabled"));
            System.out.println("EMAILING  : " + ((emailUser) ? "enabled" : "disabled"));
            System.out.println("DATABASE RUNNING : " + dbName);

            switch (args[0]) {
                case "-schema":
                case "-s":
                    // perform the schema translation
                    convertNeo4JToSQL(args[1]);
                    break;
                case "-translate":
                case "-t":
                case "-tc":
                    getLabelMapping();

                    // warm up the Cypher caches if the server has just been turned on.
                    // https://neo4j.com/developer/kb/warm-the-cache-to-improve-performance-from-cold-start/
                    // also, if there is an SSL issue when changing databases, attempt to fix it by running
                    // some fix up code.
                    try {
                        CypherDriver.warmUp();
                    } catch (ClientException ce) {
                        if (ce.getMessage().contains("SSLEngine problem")) {
                            CypherDriver.resetSSLNeo4J();
                            CypherDriver.warmUp();
                        }
                    }

                    // clear the database test results in preparation for new test data.
                    DbUtil.clearTestContents(dbName);

                    if (!printBool && !updateInsDel) {
                        // translate Cypher queries to SQL.
                        // first, reorder the queries file to randomise order in which queries are executed
                        randomiseQueriesFile(args[1]);

                        // perform 3 dry runs, then record the times of 5 executions
                        for (int i = -2; i <= 5; i++) {
                            if (i < 1) System.out.println("Warming up - iterations left : " + (i * -1));
                            translateCypherToSQL(args[1].replace(".txt", "_temp.txt"), f_cypher, f_pg,
                                    cypher_results, pg_results, i, args[0]);
                        }

                        // send the results via email
                        if (emailUser)
                            try {
                                SendResultsEmail.sendEmail(dbName, args[0]);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }

                        // delete temporary queries file
                        File f = new File(args[1].replace(".txt", "_temp.txt"));
                        f.delete();
                    } else {
                        translateCypherToSQL(args[1], f_cypher, f_pg, cypher_results, pg_results, 1, args[0]);
                    }
                    break;
                default:
                    // error with the command line arguments
                    System.err.println("Incorrect usage of Reagan v4 : " +
                            "<-schema|-translate|-s|-t|-tc> <schemaFile|queriesFile> <databaseName> <-e|-p|-c>");
                    System.exit(1);
            }
        }
    }

    private static void randomiseQueriesFile(String queriesFile) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(queriesFile));
            Map<Integer, String> queries = new TreeMap<>();
            String line;
            Random r = new Random();
            while ((line = reader.readLine()) != null) {
                queries.put(r.nextInt(1000), line);
            }
            reader.close();

            FileWriter writer = new FileWriter(queriesFile.replace(".txt", "_temp.txt"));

            for (String val : queries.values()) {
                writer.write(val);
                writer.write('\n');
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
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
            fis = new FileInputStream(Reagan_Main_V4.workspaceArea + "/meta_labels.txt");
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

            fis = new FileInputStream(Reagan_Main_V4.workspaceArea + "/meta_rels.txt");
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
     * @param typeTranslate  In version three, stick with this being -t2, as -t will attempt to use the
     *                       transitive closure relation which probably does not exist in the database.
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
                if (!line.startsWith("//") && !line.isEmpty() && !denyList.contains(line)) {
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
                        } else if (line.toLowerCase().contains("shortestpath")) {
                            sql = convertCypherShortPath(line);
                        } else if (line.toLowerCase().contains("iterate")) {
                            sql = convertIterateQuery(line, typeTranslate);
                        } else {
                            sql = convertCypherToSQL(line, typeTranslate).getSqlEquiv();
                        }

                        returnItemsForCypher = null;

                        if (sql != null && !sql.startsWith("INSERT") && !sql.startsWith("DELETE")
                                && !line.toLowerCase().contains("iterate")) {
                            returnItemsForCypher = lastDQ.getCypherAdditionalInfo().getReturnClause()
                                    .replace(" ", "").split(",");
                        }
                    }

                    boolean sqlExecSuccess;
                    if (sql != null) {
                        sqlExecSuccess = executeSQL(sql, pg_results,
                                (printBool || line.toLowerCase().contains("count")));
                    } else throw new Exception("Conversion of SQL failed");

                    if (!sqlExecSuccess) denyList.add(line);

                    if (!line.toLowerCase().contains("iterate"))
                        CypherDriver.run(line, cypher_results, returnItemsForCypher, printBool);


                    // validate the results
                    if (sqlExecSuccess) {
                        if ((numResultsNeo != numResultsPost) && !line.toLowerCase().contains("iterate")
                                && !line.toLowerCase().startsWith("create") && !line.toLowerCase().contains("detach delete")
                                && !line.toLowerCase().contains("foreach")) {
                            translationFail(line, sql, f_cypher, f_pg);
                        } else if (line.toLowerCase().contains("count") && !line.toLowerCase().contains("with")) {
                            boolean countSame = FileUtils.contentEquals(f_cypher, f_pg);
                            if (!countSame) {
                                translationFail(line, sql, f_cypher, f_pg);
                            }
                        }

                        if (repeatCount > 0) {
                            // record the performance of Cypher and SQL on Neo4J and Postgres respectively.
                            printSummary(line, sql, f_cypher, f_pg);
                            DbUtil.insertMapping(line, sql, returnItemsForCypher, dbName);
                        }
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

    private static void translationFail(String line, String sql, File f_cypher, File f_pg) throws IOException {
        System.err.println("\n**********Statements do not appear to " +
                "be logically correct - please check**********\n"
                + line + "\n" + sql + "\n***********");
        printSummary(line, sql, f_cypher, f_pg);
        System.exit(1);
    }

    /**
     * Class to deal with special new query type which is not currently in Cypher.
     * Details in documentation and dissertation (this is an extension to the main tool).
     * More information to come.
     *
     * @param line (Cypher like) input.
     *             [NOTE THESE QUERIES CANNOT RUN ON CYPHER, THEY DO NOT EXIST IN THE GRAMMAR!]
     * @return SQL translation
     */
    private static String convertIterateQuery(String line, String typeTranslate) {
        line = line.toLowerCase();
        CypIterate cypIter = new CypIterate(line);
        return SQLIterate.translate(cypIter, typeTranslate);
    }

    /**
     * Translation method for Cypher's shortestPath syntax.
     *
     * @param line (containing allShortestPaths) to be translated.
     * @return SQL equivalent of the 'line' input.
     * @throws Exception
     */
    private static String convertCypherShortPath(String line) throws Exception {
        line = line.toLowerCase();
        int returnIndex = line.indexOf("return");
        int whereIndex = line.indexOf("where");
        String whereClause = null;
        if (whereIndex != -1) {
            whereClause = line.substring(whereIndex, returnIndex - 1);
        }
        int indexToUse = (whereIndex == -1) ? returnIndex : whereIndex;
        String path = line.substring(line.indexOf("(") + 1, indexToUse - 2);
        String returnClause = line.substring(line.indexOf("return"));
        String cypherPathQuery = "MATCH " + path + ((whereIndex != -1) ? whereClause : "") + " " + returnClause;
        DecodedQuery dQMainPath = CypherTokenizer.decode(cypherPathQuery, false);
        lastDQ = dQMainPath;
        return SQLShortestPath.translate(dQMainPath).toString();
    }

    /**
     * Translation method for Cypher's allShortestPaths syntax.
     *
     * @param line Cypher (containing allShortestPaths) to be translated.
     * @return SQL equivalent of the 'line' input.
     * @throws Exception
     */
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
     * @param typeTranslate The type of translation being performed, specified at the command line.
     *                      -t --> normal translation.
     *                      -tc --> use the transitive closure relational representation.
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
     * @param typeTranslate The type of translation being performed, specified at the command line.
     *                      -t --> normal translation.
     *                      -tc --> use the transitive closure relational representation.
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
    private static boolean executeSQL(String sql, String pg_results, boolean printOutput) {
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
            System.out.println("FAILED IN executeSQL -- " + sql);
            e.printStackTrace();
            if (emailUser) SendResultsEmail.sendFailEmail(dbName, sql);
            return false;
        }
        return true;
    }

    /**
     * Convert Cypher queries to SQL
     *
     * @param cypher        Original Cypher query to translate.
     * @param typeTranslate The type of translation being performed, specified at the command line.
     *                      -t --> normal translation.
     *                      -tc --> use the transitive closure relational representation.
     * @return SQL that maps to the Cypher input.
     */
    public static DecodedQuery convertCypherToSQL(String cypher, String typeTranslate) {
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