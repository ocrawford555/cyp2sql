package main_area;

import database.DbUtil;
import junit.framework.TestCase;

public class Cyp2SQLTest extends TestCase {
    public void testUnitAll() throws Exception {
        // have text file indexed with Cypher queries on the test graph
        // have another text file with a list of known correct translations
        // see if the output is the same.
        String query = "Match (n) return n;";
        assertEquals("SELECT n.* FROM nodes n WHERE n.id = a.a1;", Cyp2SQL.convertQuery(query));
    }

    public void testDatabaseOutput() throws Exception {
        String query = "match (n:Player)--(y:Club) return n limit 20;";
        Cyp2SQL.cypherOutputToTextFile(query);
        String sql = Cyp2SQL.convertQuery(query);
        System.out.println(sql);
        DbUtil.createConnection("testa");
        DbUtil.select(sql);
        DbUtil.closeConnection();
        // compare the outputs here
    }
}