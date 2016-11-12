package production;

import clauseObjects.DecodedQuery;
import database.CypherDriver;
import database.DbUtil;
import org.apache.commons.io.FileUtils;
import query_translation.InterToSQLNodesEdges;
import query_translation.SQLUnion;
import schemaConversion.SchemaTranslate;
import toolv1.CypherTokenizer;

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
    private static String dbName;
    private static String cypher_results = "C:/Users/ocraw/Desktop/cypher_results.txt";
    private static String pg_results = "C:/Users/ocraw/Desktop/pg_results.txt";

    public static void main(String args[]) {
        // STEP 1 : input to the command line the Neo4J dump (to take args[0])
        // in future versions, attempt to automate this, but not important yet
        // if supplied without file, then do not do this step - assumes data already
        // in PostGres

        if (args.length == 3) {
            dbName = args[2];
            long startMillis = System.currentTimeMillis();
            convertNeo4JToSQL(args[0]);
            System.out.println(System.currentTimeMillis() - startMillis + " ms.");
        } else if (args.length != 2) {
            System.err.println("Incorrect usage of Cyp2SQL v1 : <dumpFile> <queriesFile> <database>");
            System.exit(1);
        } else {
            dbName = args[1];
        }

        File f1 = new File(cypher_results);
        File f2 = new File(pg_results);


        // with the conversion having occurred, proceed to go through text file containing Cypher
        // queries, convert them to intermediate form, convert that to SQL, and run on Postgres.
        try {
            FileInputStream fis = new FileInputStream((args.length == 2) ? args[0] : args[1]);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            String sql;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("//")) {
                    CypherDriver.run(line);
                    sql = convertCypherToSQL(line);
                    System.out.println(sql);
                    if (sql != null) {
                        executeSQL(sql);

                    } else throw new Exception("Conversion of SQL failed");
                    System.out.println("QUERY: " + line + "\nRESULT: " +
                            FileUtils.contentEquals(f1, f2));
                }
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Success : Results written to file <pg_results.txt>.");
    }

    private static void executeSQL(String sql) {
        try {
            String indivSQL[] = sql.split(";");
            for (String q : indivSQL) {
                if (q.trim().startsWith("CREATE")) {
                    DbUtil.executeCreateView(q + ";", dbName);
                } else
                    DbUtil.select(q + ";", dbName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String convertCypherToSQL(String cypher) {
        try {
            if (cypher.toLowerCase().contains(" union all ")) {
                String[] queries = cypher.toLowerCase().split(" union all ");
                ArrayList<String> unionSQL = new ArrayList<>();
                DecodedQuery dQ;
                for (String s : queries) {
                    dQ = CypherTokenizer.decode(s);
                    unionSQL.add(InterToSQLNodesEdges.translate(dQ));
                }
                return SQLUnion.genUnion(unionSQL, "UNION ALL");
            } else if (cypher.toLowerCase().contains(" union ")) {
                String[] queries = cypher.toLowerCase().split(" union ");
                ArrayList<String> unionSQL = new ArrayList<>();
                DecodedQuery dQ;
                for (String s : queries) {
                    dQ = CypherTokenizer.decode(s);
                    unionSQL.add(InterToSQLNodesEdges.translate(dQ));
                }
                return SQLUnion.genUnion(unionSQL, "UNION");
            } else {
                DecodedQuery decodedQuery = CypherTokenizer.decode(cypher);
                return InterToSQLNodesEdges.translate(decodedQuery);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void convertNeo4JToSQL(String dumpFile) {
        // true is a debug print parameter
        SchemaTranslate.translate(dumpFile);
        // testa is the default database location
        DbUtil.insertSchema(dbName);
    }
}
