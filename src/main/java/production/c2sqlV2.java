package production;

import clauseObjects.CypForEach;
import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import database.InsertSchema;
import org.apache.commons.io.FileUtils;
import query_translation.SQLForEach;
import query_translation.SQLTranslate;
import query_translation.SQLUnion;
import query_translation.SQLWith;
import schemaConversion.SchemaTranslate;
import translator.CypherTokenizer;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This is version 2 of the translation tool.
 * This version is to add more features to v1 and more testing.
 * Queries aim to be more optimised.
 * Output of the whole tool to be cleaner and faster if possible.
 * Milestone end of version 2 should be a product that is ready to take
 * on the large datasets and return some results.
 * Created by ojc37.
 * Deadline : 12th January 2016 (show to supervisor and upload to GitHub).
 */
public class c2sqlV2 {
    // for optimisations based on the return clause of Cypher
    public static final Map<String, String> mapLabels = new HashMap<>();
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
    private static String dbName;
    // stores the last decoded query so that the Cypher results module can use it
    // , without having to rerun computation.
    private static DecodedQuery lastDQ = null;

    private static boolean printBool = false;

    /**
     * Main method when application is launched.
     *
     * @param args arguments to the application.
     *             <-schema|-translate|-s|-t> <schemaFile|queriesFile> <databaseName> <-p>
     */
    public static void main(String args[]) {
        C2SProperties props = new C2SProperties();
        String[] fileLocations = props.getLocalProperties();

        String cypher_results = fileLocations[0];
        String pg_results = fileLocations[1];
        workspaceArea = fileLocations[2];
        postUN = fileLocations[3];
        postPW = fileLocations[4];
        neoUN = fileLocations[5];
        neoPW = fileLocations[6];

        if (args.length != 3 && args.length != 4) {
            System.err.println("Incorrect usage of Cyp2SQL v2 : <schema|translate> " +
                    "<schemaFile|queriesFile> <databaseName>");
            System.exit(1);
        } else {
            File f_cypher = new File(cypher_results);
            File f_pg = new File(pg_results);

            dbName = args[2];

            if (args.length == 4 && args[3].equals("-p")) {
                printBool = true;
            }

            System.out.println("PRINT TO FILE : " + ((printBool) ? "enabled" : "disabled"));

            switch (args[0]) {
                case "-schema":
                case "-s":
                    // perform the schema translation
                    convertNeo4JToSQL(args[1]);
                    break;
                case "-translate":
                case "-t":
                    getLabelMapping();
                    translateCypherToSQL(args[1], f_cypher, f_pg, cypher_results, pg_results);
                    break;
                default:
                    System.err.println("Incorrect usage of Cyp2SQL v2 : <schema|translate> " +
                            "<schemaFile|queriesFile> <databaseName>");
                    System.exit(1);
            }
        }
    }

    private static void translateCypherToSQL(String translateFile, File f_cypher, File f_pg, String cypher_results,
                                             String pg_results) {
        try {
            FileInputStream fis = new FileInputStream(translateFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            String sql;

            while ((line = br.readLine()) != null) {
                // if line is commented out in the read queries file, then do not attempt to convert it
                if (!line.startsWith("//")) {
                    //Object[] mapping = DbUtil.getMapping(line, dbName);
                    Object[] mapping = {null, null};
                    String[] returnItemsForCypher;

                    if (mapping[0] != null) {
                        sql = (String) mapping[0];
                        returnItemsForCypher = (String[]) mapping[1];
                    } else {
                        if (line.toLowerCase().contains(" foreach ")) {
                            sql = convertCypherForEach(line);
                        } else if (line.toLowerCase().contains(" with ")) {
                            sql = convertCypherWith(line);
                        } else {
                            if (line.contains("match") && line.contains("create")) line = line.replace(",", "-[]-");
                            sql = convertCypherToSQL(line).getSqlEquiv();
                        }

                        returnItemsForCypher = null;
                        if (sql != null && !sql.startsWith("INSERT") && !sql.startsWith("DELETE")) {
                            returnItemsForCypher = lastDQ.getCypherAdditionalInfo().
                                    getReturnClause().replace(" ", "").split(",");
                            DbUtil.insertMapping(line, sql, returnItemsForCypher, dbName);
                        }
                    }

                    if (sql != null) {
                        executeSQL(sql, pg_results, printBool);
                    } else throw new Exception("Conversion of SQL failed");

                    CypherDriver.run(line, cypher_results, returnItemsForCypher, printBool);

                    System.out.println("\n**********\nCypher Input : " + line);
                    System.out.println("SQL Output: " + sql + "\nExact Result: " +
                            FileUtils.contentEquals(f_cypher, f_pg) + "\nNumber of records from Neo4J: " +
                            numResultsNeo + "\nNumber of results from PostG: " + numResultsPost +
                            "\nTime on Neo4J: " + (CypherDriver.lastExecTime / 1000000.0) +
                            " ms.\nTime on Postgres: " + ((DbUtil.lastExecTimeRead + DbUtil.lastExecTimeCreate +
                            DbUtil.lastExecTimeInsert)
                            / 1000000.0) +
                            " ms.\n**********\n");

                    resetExecTimes();
                }
            }
            br.close();
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String convertCypherForEach(String line) {
        String changeLine = line.toLowerCase().replace("with", "return");
        String[] feParts = changeLine.toLowerCase().split(" foreach ");
        DecodedQuery dQ = convertCypherToSQL(feParts[0].trim() + ";");
        CypForEach cypForEach = new CypForEach(feParts[1].trim());

        return SQLForEach.genQuery(dQ.getSqlEquiv(), cypForEach);
    }

    private static void resetExecTimes() {
        CypherDriver.lastExecTime = 0;
        DbUtil.lastExecTimeCreate = 0;
        DbUtil.lastExecTimeRead = 0;
        DbUtil.lastExecTimeInsert = 0;
    }

    private static String convertCypherWith(String line) {
        String changeLine = line.toLowerCase().replace("with", "return");
        String[] withParts = changeLine.toLowerCase().split("where");
        DecodedQuery dQ = convertCypherToSQL(withParts[0] + ";");

        String withTemp = null;
        if (dQ != null) {
            withTemp = SQLWith.genTemp(dQ.getSqlEquiv());
        }

        System.out.println(withTemp);

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
                    unionSQL.add(SQLTranslate.translateRead(dQ));
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
                    unionSQL.add(SQLTranslate.translateRead(dQ));
                }
                dQ.setSqlEquiv(SQLUnion.genUnion(unionSQL, "UNION"));
                lastDQ = dQ;
                return dQ;
            } else {
                DecodedQuery dQ = CypherTokenizer.decode(cypher, false);
                if (dQ.getRc() != null) {
                    // the translation is for a read query.
                    dQ.setSqlEquiv(SQLTranslate.translateRead(dQ));
                } else {
                    if (dQ.getCypherAdditionalInfo().hasDelete()) {
                        // the translation is a delete query.
                        dQ.setSqlEquiv(SQLTranslate.translateDelete(dQ));
                    } else {
                        // the translation is for an insert query.
                        if (dQ.getCypherAdditionalInfo().getCreateClauseRel() != null) {
                            dQ.setSqlEquiv(SQLTranslate.translateInsertRels(dQ));
                        } else dQ.setSqlEquiv(SQLTranslate.translateInsertNodes(dQ));
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

    /**
     * Convert Neo4J schema to Postgres (relational schema)
     *
     * @param dumpFile Generated BY THE USER from Neo4J shell (see README)
     */
    private static void convertNeo4JToSQL(String dumpFile) {
        SchemaTranslate.translate(dumpFile);
        InsertSchema.executeSchemaChange(dbName);
    }

    private static void getLabelMapping() {
        // open file and read in removing duplicated as they are of no use.
        FileInputStream fis;
        try {
            fis = new FileInputStream(c2sqlV2.workspaceArea + "/meta_labels.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            String currentLabelType = null;
            ArrayList<String> dupProps = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                if (line.startsWith("*")) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
