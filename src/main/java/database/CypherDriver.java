package database;

import org.neo4j.driver.v1.*;

public class CypherDriver {
    public static void run(String query) {
        Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "ojc37"));
        Session session = driver.session();

        StatementResult result = session.run(query);

        while (result.hasNext()) {
            Record record = result.next();
            System.out.println("NAME : " + record.get("c").asNode().get("name").asString());
        }

        session.close();
        driver.close();
    }
}
