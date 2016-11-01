package testing;

import database.DbUtil;
import toolv1.Metadata_Schema;
import toolv1.SchemaTranslate;

import java.util.ArrayList;
import java.util.Map;

public class TestArea {
    public static void main(String[] args) throws Exception {
//        ArrayList<String> t = Metadata_Schema.getAllFields();
//
//        for (String s : t) {
//            System.out.println(s);
//        }

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
