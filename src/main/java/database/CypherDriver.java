package database;

import clauseObjects.DecodedQuery;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import toolv1.CypherTokenizer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class CypherDriver {
    private static final String databaseName = "neo4j";
    private static final String dbPassword = "ojc37";
    private static final String file_location_results = "C:/Users/ocraw/Desktop/cypher_results.txt";

    public static void run(String query) throws Exception {
        // database essentials
        Driver driver = GraphDatabase.driver("bolt://localhost",
                AuthTokens.basic(databaseName, dbPassword));
        Session session = driver.session();

        StatementResult result = session.run(query);

        // obtain information about the query from the decoder module
        DecodedQuery dQ = CypherTokenizer.decode(query);
        String returnItems[] = dQ.getCypherAdditionalInfo().getReturnClause().replace(" ", "").split(",");

        // keep a track of the number of records returned from Neo4J
        int countRecords = 0;

        PrintWriter writer;
        try {
            writer = new PrintWriter(file_location_results, "UTF-8");

            while (result.hasNext()) {
                Record record = result.next();
                for (String t : returnItems) {
                    try {
                        if (t.contains(".")) {
                            String bits[] = t.split("\\.");
                            writer.println(bits[1] + " : " + record.get(t).asString());
                        } else {
                            writer.println("name : " + record.get(t).asNode().get("name").asString());
                        }
                    } catch (ClientException ce) {
                        System.out.println("Error handled correctly in CypherDriver.");
                        ce.printStackTrace();
                    }
                }
                countRecords++;
            }
            System.out.println("\nNUM RECORDS : " + countRecords);
            writer.println();
            writer.println("NUM RECORDS : " + countRecords);
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        session.close();
        driver.close();
    }
}
