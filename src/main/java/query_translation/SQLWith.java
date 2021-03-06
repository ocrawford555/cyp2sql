package query_translation;

import clauseObjects.CypReturn;
import clauseObjects.DecodedQuery;
import translator.CypherTokenizer;

import java.util.ArrayList;

public class SQLWith {
    private static final String o = "order";
    private static final String r = "return";
    private static final String s = "skip";
    private static final String l = "limit";


    public static String genTemp(String query) {
        String initial = "CREATE TEMP VIEW wA AS (";
        return initial + query.substring(0, query.length() - 1) + ");";
    }

    // current WITH statement setup presumes no aliasing of return in second part of the WITH clause.
    public static String createSelect(String query, DecodedQuery dQ) {
        StringBuilder sWith = new StringBuilder();
        ArrayList<String> tokens = CypherTokenizer.getTokenList(query, false);

        // get SELECT
        sWith = getSelectForWith(sWith, tokens, dQ);

        // get WHERE
        if (!tokens.get(0).equals(o) && !tokens.get(0).equals(s) && !tokens.get(0).equals(l)
                && !tokens.get(0).equals(r)) {
            sWith = getWhereForWith(sWith, tokens);
        }

        // get ORDER
        if (tokens.contains("order")) {
            sWith = getOrderByForWith(sWith, tokens);
        }

        int posOfSkip = tokens.indexOf("skip");
        if (posOfSkip != -1) sWith.append(" OFFSET ").append(tokens.get(++posOfSkip));
        int posOfLimit = tokens.indexOf("limit");
        if (posOfLimit != -1) sWith.append(" LIMIT ").append(tokens.get(++posOfLimit));

        return sWith.toString() + ";";
    }

    private static StringBuilder getOrderByForWith(StringBuilder sWith, ArrayList<String> tokens) {
        int posOfOrder = tokens.indexOf("order");
        sWith.append(" ORDER BY ");
        while (true) {
            String field = tokens.get(posOfOrder + 2);
            String dir = (posOfOrder + 3 >= tokens.size()) ? null : tokens.get(posOfOrder + 3);
            if (dir != null) {
                dir = (dir.equals("asc") || dir.equals("desc")) ? dir : null;
            }
            sWith.append(field).append(" ").append((dir == null) ? "" : dir);
            posOfOrder += 2;
            if (posOfOrder + 1 >= tokens.size() || !tokens.get(posOfOrder + 1).equals(",")) {
                break;
            } else {
                posOfOrder++;
            }
        }
        return sWith;
    }

    private static StringBuilder getWhereForWith(StringBuilder sWith, ArrayList<String> tokens) {
        int i = 0;
        sWith.append(" WHERE ");
        String whereStmt = "";
        while (true) {
            String currentTok = tokens.get(i);

            // loop termination condition
            if (currentTok.equals(o) || currentTok.equals(r) || currentTok.equals(s) || currentTok.equals(l)) break;

            whereStmt = whereStmt + " " + currentTok;
            i++;
        }
        return sWith.append(whereStmt);
    }

    private static StringBuilder getSelectForWith(StringBuilder sWith, ArrayList<String> tokens, DecodedQuery dQ) {
        sWith.append("SELECT ");
        int posOfReturn = tokens.indexOf("return");

        for (int i = posOfReturn + 1; i < tokens.size(); i++) {
            String currentTok = tokens.get(i);
            if (currentTok.equals("order") || currentTok.equals("skip") || currentTok.equals("limit")) {
                break;
            } else {
                String returnStmt = currentTok;

                while (i + 1 < tokens.size()) {
                    if (!tokens.get(i + 1).equals(",") && !tokens.get(i + 1).equals("order") &&
                            !tokens.get(i + 1).equals("skip") && !tokens.get(i + 1).equals("limit")
                            && !tokens.get(i + 1).equals(".")) {
                        returnStmt = returnStmt + " " + tokens.get(i++);
                    } else if (tokens.get(i + 1).equals(".")) {
                        returnStmt = returnStmt + "." + tokens.get(i + 2);
                        i += 2;
                    } else {
                        i++;
                        break;
                    }
                }

                String[] idAndProp = returnStmt.split("\\.");

                for (CypReturn cR : dQ.getRc().getItems()) {
                    if (cR.getField() != null && cR.getField().startsWith("count")) {
                        sWith.append(idAndProp[0]).append(", ");
                        break;
                    } else if (cR.getNodeID().equals(idAndProp[0])) {
                        if (cR.getField() == null && idAndProp[1] == null) {
                            sWith.append("*, ");
                        } else {
                            String field = (idAndProp[1] == null) ? cR.getField() : idAndProp[1];
                            sWith.append(field).append(", ");
                        }
                        break;
                    }
                }

                if (tokens.get(i).equals("order") || tokens.get(i).equals("skip") ||
                        tokens.get(i).equals("limit")) break;
            }
        }

        sWith.setLength(sWith.length() - 2);
        sWith.append(" FROM wA");

        return sWith;
    }
}
