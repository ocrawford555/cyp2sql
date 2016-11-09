package query_translation;

import java.util.ArrayList;

public class SQLUnion {
    public static String genUnion(ArrayList<String> queries, String unionType) {
        int i = 1;
        StringBuilder unionSQL = new StringBuilder();
        unionSQL.append("WITH ");
        for (String s : queries) {
            unionSQL.append("u").append(i++).append(" AS (");
            unionSQL.append(s.substring(0, s.length() - 1)).append("), ");
        }
        unionSQL.setLength(unionSQL.length() - 2);
        for (int j = 1; j < i; j++) {
            unionSQL.append(" SELECT * FROM u").append(j).append(" ").append(unionType);
        }
        unionSQL.setLength(unionSQL.length() - 6);
        unionSQL.append(";");
        return unionSQL.toString();
    }
}
