package production;

import clauseObjects.DecodedQuery;
import database.DbUtil;
import query_translation.InterToSQLNodesEdges;
import schemaConversion.SchemaTranslate;
import toolv1.CypherTokenizer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Map;

/**
 * This is version 1 of the translation tool.
 * Focuses mainly on the schema translation, and basic query translation.
 * Optimisations, and other features/extensions are not in this version.
 * Created by ojc37.
 * Deadline : 5th December 2016 (show to supervisor and upload to GitHub).
 */
public class c2sqlV1 {
    private static String dbName;

    public static void main(String args[]) {
        // STEP 1 : input to the command line the Neo4J dump (to take args[0])
        // in future versions, attempt to automate this, but not important yet
        // if supplied without file, then do not do this step - assumes data already
        // in PostGres

        if (args.length == 3) {
            dbName = args[2];
            convertNeo4JToSQL(args[0]);
        } else if (args.length != 2) {
            System.err.println("Incorrect usage of Cyp2SQL v1 : <dumpFile> <queriesFile> <database>");
            System.exit(1);
        } else {
            dbName = args[1];
        }

        // with the conversion having occurred, proceed to go through text file containing Cypher
        // queries, convert them to intermediate form, convert that to SQL, and run on Postgres.
        try {
            FileInputStream fis = new FileInputStream((args.length == 2) ? args[0] : args[1]);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            String sql;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("//")) {
                    sql = convertCypherToSQL(line);
                    if (sql != null) {
                        executeSQL(sql);
                    } else throw new Exception("Conversion of SQL failed");
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
            DbUtil.select(sql, dbName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String convertCypherToSQL(String cypher) {
        try {
            DecodedQuery decodedQuery = CypherTokenizer.decode(cypher);
            return InterToSQLNodesEdges.translate(decodedQuery);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void convertNeo4JToSQL(String dumpFile) {
        // true is a debug print parameter
        Map<String, String> schemaGenerated = SchemaTranslate.translate(dumpFile, true);
        // testa is the default database location
        DbUtil.insertSchema(schemaGenerated, dbName);
    }
}