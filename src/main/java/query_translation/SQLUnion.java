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
        StringBuilder unionSQL = new StringBuilder();
        for (String s : queries) {
            unionSQL.append(s.substring(0, s.length() - 1));
            unionSQL.append(unionType).append(" ");
        }
        unionSQL.setLength(unionSQL.length() - (1 + unionType.length()));
        unionSQL.append(";");
        return unionSQL.toString();
    }
}
