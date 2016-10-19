package database;

import org.neo4j.driver.v1.*;

public class CypherDriver {
    public static void run(String query) {
        Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "ojc37"));
        Session session = driver.session();

        StatementResult result = session.run(query);

        // to fix is to output the results in a more comparable way
        // do research of this early Michaelmas
        // Question: is using the driver even the best way of thinking about this,
        // can they be executed more directly to get a better idea of timing.
        int countRecords = 0;
        while (result.hasNext()) {
            Record record = result.next();
            System.out.println("NAME : " + record.get("n").asNode().get("name").asString());
            countRecords++;
        }

        System.out.println("\nNUM RECORDS : " + countRecords);
        session.close();
        driver.close();
    }
}
