package database;

import clauseObjects.CypNode;
import clauseObjects.DecodedQuery;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import toolv1.CypherTokenizer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
        DecodedQuery dQ = CypherTokenizer.decode(query, false);

        String returnItems[] = dQ.getCypherAdditionalInfo().getReturnClause().replace(" ", "").split(",");

        // keep a track of the number of records returned from Neo4J
        int countRecords = 0;

        PrintWriter writer;
        try {
            writer = new PrintWriter(file_location_results, "UTF-8");

            while (result.hasNext()) {
                Record record = result.next();
                for (String t : returnItems) {
//                    if (t.equals("*")) {
//                        doAllReturn(dQ, record, writer);
//                        break;
//                    }
                    try {
                        if (t.contains(".")) {
                            String bits[] = t.split("\\.");
                            writer.println(bits[1].toLowerCase() + " : " + record.get(t).asString().toLowerCase());
                        } else {
                            // currently only deals with returning nodes
                            List<String> fields = getAllFields();
                            if (fields != null) {
                                for (String s : fields) {
                                    try {
                                        writer.println(s + " : " + record.get(t).asNode().get(s).asString().toLowerCase());
                                    } catch (ClientException ce) {
                                        writer.println(s + " : " + record.get(t).asNode().get(s).asInt());
                                    }
                                }
                            }
                        }
                    } catch (ClientException ce) {
                        System.out.println("Error handled correctly in CypherDriver." + ce.toString());
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

    private static void doAllReturn(DecodedQuery dQ, Record record, PrintWriter writer) {
        for (CypNode cN : dQ.getMc().getNodes()) {
            writer.println("name : " + record.get(cN.getId()).asNode().get("name").asString().toLowerCase());
        }
    }

    private static List<String> getAllFields() {
        List<String> toReturn = new ArrayList<>();
        try {
            FileInputStream fis = new FileInputStream("C:/Users/ocraw/Desktop/meta.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.equals("id"))
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
