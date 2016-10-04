package toolv1;

import org.neo4j.driver.v1.*;

class CypherDriver {
    static void run(String query) {
        Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "ojc37"));
        Session session = driver.session();

        long startMillis = System.currentTimeMillis();
        StatementResult result = session.run(query);
        long finMillis = System.currentTimeMillis();

        while (result.hasNext()) {
            Record record = result.next();
            System.out.println("NAME : " + record.get("n").asNode().get("Name").asString());
        }

        System.out.println(finMillis - startMillis + " ms.");

        session.close();
        driver.close();
    }
}
