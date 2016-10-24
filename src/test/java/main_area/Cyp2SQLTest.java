package main_area;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Cyp2SQLTest extends TestCase {
    public void testUnitAll() throws Exception {
        // have text file indexed with Cypher queries on the test graph
        // have another text file with a list of known correct translations
        // see if the output is the same.

        //assertEquals("SELECT n.* FROM nodes n WHERE n.id = a.a1;",
        // Cyp2SQL.convertQuery(query));

        Path path = Paths.get("C:/Users/ocraw/Desktop/list_of_queries.txt");

        try (Stream<String> lines = Files.lines(path)) {
            lines.forEachOrdered(line -> {
                try {
                    testDatabaseOutput(line);
                } catch (Exception e) {
                    //fail();
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testDatabaseOutput(String query) throws Exception {
        Cyp2SQL.cypherOutputToTextFile(query);
        String sql = Cyp2SQL.convertQuery(query);
        System.out.println(sql);
        Cyp2SQL.printPostgresToTextFile(sql);

        // compare the outputs here
        File file1 = new File("C:/Users/ocraw/Desktop/cypher_results.txt");
        File file2 = new File("C:/Users/ocraw/Desktop/pg_results.txt");
        assertEquals(true, FileUtils.contentEquals(file1, file2));
        // add to a text file the results of the tests.
    }
}