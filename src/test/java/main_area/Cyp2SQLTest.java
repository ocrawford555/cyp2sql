package main_area;

import clauseObjects.CypNode;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;

public class Cyp2SQLTest extends TestCase {
    public void testOther() throws Exception {
        System.out.println(Cyp2SQL.convertQuery("match (n:NationalTeam {name:\"Spain\"})-[*1..2]-" +
                "(m:Club)-[*1..2]-(f:NationalTeam {name:\"France\"}) return distinct m;"));
    }

    public void testOne() throws Exception {
        testDatabaseOutput("match (n:Club {name:\"Liverpool FC\"})--(r) return distinct *");
    }

    public void testCypher() throws Exception {
        Cyp2SQL.cypherOutputToTextFile("match (n:Club {name:\"Sunderland AFC\"})-[*1..2]-(x) return x order by x.name asc");
    }

    public void testUnitAll() throws Exception {
        // have text file indexed with Cypher queries on the test graph
        // have another text file with a list of known correct translations
        // see if the output is the same.

        //assertEquals("SELECT n.* FROM nodes n WHERE n.id = a.a1;",
        // Cyp2SQL.convertQuery(query));

        ArrayList<String> toTest = new ArrayList<>();

        try {
            FileInputStream fis = new FileInputStream("C:/Users/ocraw/Desktop/list_of_queries.txt");
            //FileInputStream fis = new FileInputStream("C:/Users/ocraw/Desktop/list_of_queries_bond.txt");

            //Construct BufferedReader from InputStreamReader
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                toTest.add(line);
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String s : toTest)
            testDatabaseOutput(s);


    }

    public void testDatabaseOutput(String query) throws Exception {
        File file1 = new File("C:/Users/ocraw/Desktop/cypher_results.txt");
        File file2 = new File("C:/Users/ocraw/Desktop/pg_results.txt");
        Cyp2SQL.cypherOutputToTextFile(query);
        String sql = Cyp2SQL.convertQuery(query);
        System.out.println(sql);
        Cyp2SQL.printPostgresToTextFile(sql);

        // compare the outputs here
        assertEquals(true, FileUtils.contentEquals(file1, file2));
    }
}