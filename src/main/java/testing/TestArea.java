package testing;

import database.DbUtil;
import schemaConversion.SchemaTranslate;

import java.util.Map;

public class TestArea {
    public static void main(String[] args) throws Exception {
//        ArrayList<String> t = Metadata_Schema.getAllFields();
//
//        for (String s : t) {
//            System.out.println(s);
//        }

        Map<String, String> schemaToBuild =
                SchemaTranslate.translate(
                        "C:/Users/ocraw/Documents/Year 3 Cambridge/Project/CypherStuff/database/dump6.txt",
                        true
                );

        DbUtil.createConnection("testa");
        DbUtil.insertSchema(schemaToBuild, "testa");
        DbUtil.closeConnection();
    }
}
