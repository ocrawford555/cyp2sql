package query_translation;

import java.util.ArrayList;

/**
 * Class for creating correct SQL query when UNION/UNION ALL keyword is present
 * in the original Cypher query.
 */
public class SQLUnion {
    /**
     * Generate the union of two or more queries.
     *
     * @param queries   Individual SQL queries.
     * @param unionType Discrete: either UNION or UNION ALL.
     * @return Complete SQL query with UNION/UNION ALL added.
     */
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
        if (unionType.equals("UNION")) unionSQL.setLength(unionSQL.length() - 6);
        else if (unionType.equals("UNION ALL")) unionSQL.setLength(unionSQL.length() - 10);
        unionSQL.append(";");
        return unionSQL.toString();
    }
}
