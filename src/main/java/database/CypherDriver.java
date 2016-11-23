package database;

import clauseObjects.DecodedQuery;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import production.c2sqlV1;

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
    /**
     * Method that runs Cypher query.
     *
     * @param query          Cypher to execute.
     * @param cypher_results File to store the results.
     * @param lastDQ         Decoded query containing useful information for the Neo4J driver.
     * @throws Exception Cypher database driver failed to perform some action.
     */
    public static void run(String query, String cypher_results, DecodedQuery lastDQ) throws Exception {
        // database essentials
        Driver driver = GraphDatabase.driver("bolt://localhost",
                AuthTokens.basic(c2sqlV1.neoUN, c2sqlV1.neoPW));
        Session session = driver.session();
        StatementResult result = session.run(query);

        String returnItems[] = lastDQ.getCypherAdditionalInfo().getReturnClause().replace(" ", "").split(",");

        // keep a track of the number of records returned from Neo4J
        int countRecords = 0;

        PrintWriter writer;
        try {
            writer = new PrintWriter(cypher_results, "UTF-8");
            while (result.hasNext()) {
                Record record = result.next();
                for (String t : returnItems) {
                    try {
                        if (t.contains(".")) {
                            String bits[] = t.split("\\.");

                            // fixes problem when an alias is used.
                            if (bits[1].contains("AS")) {
                                bits[1] = bits[1].split("AS")[1];
                                t = bits[1];
                            }

                            writer.println(bits[1].toLowerCase() + " : " + record.get(t).asString().toLowerCase());
                        } else {
                            // currently only deals with returning nodes
                            List<String> fields = getAllFieldsNodes();
                            if (fields != null) {
                                for (String s : fields) {
                                    try {
                                        writer.println(s + " : " +
                                                record.get(t).asNode().get(s).asString().toLowerCase());
                                    } catch (ClientException ce) {
                                        // failed to cast int to string, so write as int.
                                        writer.println(s + " : " + record.get(t).asNode().get(s).asInt());
                                    }
                                }
                            }
                        }
                    } catch (ClientException ce) {
                        System.out.println("Error thrown in CypherDriver." + ce.toString());
                    }
                }
                countRecords++;
            }
            writer.println();
            writer.println("NUM RECORDS : " + countRecords);
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
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
            FileInputStream fis = new FileInputStream(c2sqlV1.workspaceArea + "/meta.txt");
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
}
