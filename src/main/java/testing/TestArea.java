package testing;

import database.DbUtil;
import org.apache.commons.io.FileUtils;
import toolv1.SchemaTranslate;

import java.io.File;
import java.util.Map;

public class TestArea {
    public static void main(String[] args) throws Exception {
//        File file1 = new File("C:/Users/ocraw/Desktop/cypher_results.txt");
//        File file2 = new File("C:/Users/ocraw/Desktop/pg_results.txt");
//        System.out.println(FileUtils.contentEquals(file1, file2));

        Map<String, String> schemaToBuild =
                SchemaTranslate.readFile(
                        "C:/Users/ocraw/Documents/Year 3 Cambridge/Project/CypherStuff/database/dump4.txt",
                        true,
                        2);

        DbUtil.createConnection("testa");
        DbUtil.createAndInsert(schemaToBuild);
        DbUtil.closeConnection();
    }
}
