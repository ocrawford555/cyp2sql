package database;

import clauseObjects.DecodedQuery;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import toolv1.CypherTokenizer;

import java.util.Arrays;

public class CypherDriver {
    private static final String databaseName = "neo4j";
    private static final String dbPassword = "ojc37";

    public static void run(String query) throws Exception {
        // database essentials
        Driver driver = GraphDatabase.driver("bolt://localhost",
                AuthTokens.basic(databaseName, dbPassword));
        Session session = driver.session();

        StatementResult result = session.run(query);

        // obtain information about the query from the decoder module
        DecodedQuery dQ = CypherTokenizer.decode(query);
        String returnItems[] = dQ.getCypherAdditionalInfo().getReturnClause().replace(" ", "").split(",");
        System.out.println(Arrays.toString(returnItems));

        // keep a track of the number of records returned from Neo4J
        int countRecords = 0;

        while (result.hasNext()) {
            Record record = result.next();
            for (String t : returnItems) {
                try {
                    if (t.contains(".")) {
                        String bits[] = t.split("\\.");
                        System.out.println(bits[1] + " : " + record.get(t).asString());
                    } else {
                        System.out.println("name : " + record.get("n").asNode().get("name").asString());
                    }
                } catch (ClientException ce) {
                    System.out.println("I CAUGHT THE EXPLOSION!!!!!!!!");
                    ce.printStackTrace();
                }
            }
            //System.out.println("NAME : " + record.get("n").asNode().get("name").asString());
            countRecords++;
        }

        System.out.println("\nNUM RECORDS : " + countRecords);
        session.close();
        driver.close();
    }
}
