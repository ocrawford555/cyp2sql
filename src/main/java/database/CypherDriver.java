package database;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import production.Cyp2SQL_v2_Apoc;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Driver connected to the Neo4J database.
 * Cypher queries can be run from within Java, and then the results parsed,
 * and outputted to a text file. The idea is this text file can then
 * be automatically compared against the results from Postgres.
 */
public class CypherDriver {
    // public variable keeping track of how long the Cypher queries take.
    public static long lastExecTime = 0;

    /**
     * Method that runs Cypher query.
     *
     * @param query          Cypher to execute.
     * @param cypher_results File to store the results.
     * @param returnItems    Array containing the items to return, used when outputting the results to disk.
     * @param printOutput    Set to true to store the outputs of the query on disk.
     */
    public static void run(String query, String cypher_results, String[] returnItems, boolean printOutput) {
        // database essentials
        Driver driver = GraphDatabase.driver("bolt://localhost",
                AuthTokens.basic(Cyp2SQL_v2_Apoc.neoUN, Cyp2SQL_v2_Apoc.neoPW));
        Session session = driver.session();

        // timing unit
        long startNano = System.nanoTime();
        session.run(query).consume();
        long endNano = System.nanoTime();
        lastExecTime = endNano - startNano;

        // only print results to the output file if the query is for reading.
        if (!query.toLowerCase().startsWith("create")) {
            StatementResult result = session.run(query);

            // keep a track of the number of records returned from Neo4J
            int countRecords = 0;

            PrintWriter writer;
            try {
                writer = new PrintWriter(cypher_results, "UTF-8");
                while (result.hasNext()) {
                    Record record = result.next();
                    if (printOutput) {
                        for (String t : returnItems) {
                            try {
                                if (t.contains(".")) {
                                    String bits[] = t.split("\\.");

                                    // fixes problem when an alias is used.
                                    if (bits[1].contains("AS")) {
                                        bits[1] = bits[1].split("AS")[1];
                                        t = bits[1];
                                    }

                                    try {
                                        String resultStr = record.get(t).asString().toLowerCase();
                                        if (!resultStr.equals("null"))
                                            writer.println(bits[1].toLowerCase() + " : " + resultStr);
                                    } catch (ClientException ce) {
                                        // failed to cast int to string, so write as int.
                                        int resultInt = record.get(t).asInt();
                                        writer.println(bits[1].toLowerCase() + " : " + resultInt);
                                    }
                                } else {
                                    // currently only deals with returning nodes
                                    List<String> fields = getAllFieldsNodes();
                                    if (fields != null) {
                                        for (String s : fields) {
                                            try {
                                                String resultStr = record.get(t).asNode().get(s)
                                                        .asString().toLowerCase();
                                                if (!resultStr.equals("null")) writer.println(s + " : " + resultStr);
                                            } catch (ClientException ce) {
                                                // failed to cast int to string, so write as int.
                                                int resultInt = record.get(t).asNode().get(s).asInt();
                                                writer.println(s + " : " + resultInt);
                                            }
                                        }
                                    }
                                }
                            } catch (ClientException ce) {
                                // silently throw away error message.
                                // System.err.println("Error thrown in CypherDriver." + ce.toString());
                            }
                        }
                    }
                    countRecords++;
                }
                writer.println();
                writer.println("NUM RECORDS : " + countRecords);
                writer.close();

                Cyp2SQL_v2_Apoc.numResultsNeo = countRecords;
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        // close the database connection.
        session.close();
        driver.close();
    }

    /**
     * If the Cypher query wishes to return all information about a node, then this can be done by
     * opening a "metadata" file which contains all of the possible labels for the graph db.
     *
     * @return List of labels expect 'id'.
     */
    private static List<String> getAllFieldsNodes() {
        List<String> toReturn = new ArrayList<>();
        try {
            FileInputStream fis = new FileInputStream(Cyp2SQL_v2_Apoc.workspaceArea + "/meta.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.equals("id") && !line.equals("label"))
                    toReturn.add(line);
            }
            br.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return toReturn;
    }

    /**
     * Erase contents of this file to reset the SSL settings for Neo4J.
     */
    public static void resetSSLNeo4J() {
        String file = "C:/Users/ocraw/.neo4j/known_hosts";
        ArrayList<String> contents = new ArrayList<>();
        try {
            FileInputStream fis = new FileInputStream(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                contents.add(line);
            }
            br.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileOutputStream fos;
        try {
            fos = new FileOutputStream(file);

            //Construct BufferedReader from InputStreamReader
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (String s : contents) {
                if (s.startsWith("#")) {
                    bw.write(s);
                    bw.newLine();
                }
            }
            bw.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
