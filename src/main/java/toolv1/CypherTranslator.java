package toolv1;

import clauseObjects.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.*;

class CypherTranslator {
    private static final char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    static String MatchAndReturn(StringBuilder sql, ArrayList<String> tokenList)
            throws Exception {
        // query has structure MATCH ... RETURN ...
        // check to perform is returning something mentioned in match clause
        int posOfMatch = tokenList.indexOf("MATCH");
        int posOfReturn = tokenList.indexOf("RETURN");

        List<String> matchClause = tokenList.subList(posOfMatch + 1, posOfReturn);
        List<String> returnClause = tokenList.subList(posOfReturn + 1, tokenList.size());

        MatchClause matchC = matchDecode(matchClause);
        ReturnClause returnC = returnDecode(returnClause);

        if (returnC.getItems() == null)
            throw new Exception("NOTHING SPECIFIED TO RETURN");

        sql = obtainMatchAndReturn(matchC, returnC, sql);
        sql.append(";");
        return sql.toString();
    }

    static String MatchAndReturnAndOrder(StringBuilder sql, ArrayList<String> tokenList) throws Exception {
        // query has structure MATCH ... RETURN ... ORDER BY [ASC|DESC]
        // check to perform is returning something mentioned in match clause
        int posOfMatch = tokenList.indexOf("MATCH");
        int posOfReturn = tokenList.indexOf("RETURN");
        int posOfOrder = tokenList.indexOf("ORDER");

        List<String> matchClause = tokenList.subList(posOfMatch + 1, posOfReturn);
        List<String> returnClause = tokenList.subList(posOfReturn + 1, posOfOrder);
        List<String> orderClause = tokenList.subList(posOfOrder + 2, tokenList.size());

        MatchClause matchC = matchDecode(matchClause);
        ReturnClause returnC = returnDecode(returnClause);
        OrderClause orderC = orderDecode(orderClause);

        sql = obtainMatchAndReturn(matchC, returnC, sql);

        sql = obtainOrderByClause(matchC, orderC, sql);

        sql.append(";");
        return sql.toString();
    }

    private static StringBuilder obtainOrderByClause(MatchClause matchC, OrderClause orderC,
                                                     StringBuilder sql) throws Exception {
        sql.append(" ");
        sql.append("ORDER BY ");
        for (CypOrder cO : orderC.getItems()) {
            String entity = validateNodeID(cO.getNodeID(), matchC);

            if (entity != null) {
                sql.append(entity).append(".").append(cO.getField()).append(" ").append(cO.getAscOrDesc());
                sql.append(", ");
            } else
                throw new Exception("ORDER BY CLAUSE INCORRECT");
        }
        sql.setLength(sql.length() - 2);
        return sql;
    }

    static String MatchAndReturnAndOrderAndSkip(StringBuilder sql, ArrayList<String> tokenList) throws Exception {
        // query has structure MATCH ... RETURN ... ORDER BY [ASC|DESC]
        // check to perform is returning something mentioned in match clause
        int posOfMatch = tokenList.indexOf("MATCH");
        int posOfReturn = tokenList.indexOf("RETURN");
        int posOfOrder = tokenList.indexOf("ORDER");
        int posOfSkip = tokenList.indexOf("SKIP");
        int posOfLimit = tokenList.indexOf("LIMIT");

        //TODO: fix for skip and limit tokens
        List<String> matchClause = tokenList.subList(posOfMatch + 1, posOfReturn);
        List<String> returnClause = tokenList.subList(posOfReturn + 1, posOfOrder);
        List<String> orderClause;
        List<String> skipClause = null;
        List<String> limitClause = null;

        if (posOfSkip == -1 && posOfLimit == -1) {
            orderClause = tokenList.subList(posOfOrder + 2, tokenList.size());
        } else if (posOfLimit == -1) {
            orderClause = tokenList.subList(posOfOrder + 2, posOfSkip);
            skipClause = tokenList.subList(posOfSkip + 1, tokenList.size());
        } else if (posOfSkip == -1) {
            orderClause = tokenList.subList(posOfOrder + 2, posOfLimit);
            limitClause = tokenList.subList(posOfLimit + 1, tokenList.size());
        } else {
            orderClause = tokenList.subList(posOfOrder + 2, posOfSkip);
            skipClause = tokenList.subList(posOfSkip + 1, posOfLimit);
            limitClause = tokenList.subList(posOfLimit + 1, tokenList.size());
        }

        MatchClause matchC = matchDecode(matchClause);
        ReturnClause returnC = returnDecode(returnClause);
        OrderClause orderC = orderDecode(orderClause);
        int skipAmount = (posOfSkip != -1) ? skipDecode(skipClause) : -1;
        int limitAmount = (posOfLimit != -1) ? limitDecode(limitClause) : -1;

        sql = obtainMatchAndReturn(matchC, returnC, sql);

        sql = obtainOrderByClause(matchC, orderC, sql);

        if (skipAmount != -1) sql.append(" OFFSET ").append(skipAmount);

        if (limitAmount != -1) sql.append(" LIMIT ").append(limitAmount);

        sql.append(";");
        return sql.toString();
    }

    private static int limitDecode(List<String> clause) throws Exception {
        if (clause.size() == 1) {
            return Integer.parseInt(clause.get(0));
        } else throw new Exception("RETURN CLAUSE MALFORMED");
    }

    private static int skipDecode(List<String> clause) throws Exception {
        if (clause.size() == 1) {
            return Integer.parseInt(clause.get(0));
        } else throw new Exception("RETURN CLAUSE MALFORMED");
    }

    private static String validateNodeID(String nodeID, MatchClause matchC) {
        for (CypNode cN : matchC.getNodes()) {
            if (nodeID.equals(cN.getId()))
                return cN.getAlias()[1];
        }
        return null;
    }

    private static StringBuilder obtainMatchAndReturn(MatchClause matchC, ReturnClause returnC,
                                                      StringBuilder sql) throws Exception {
        if (returnC.getItems() == null)
            throw new Exception("NOTHING SPECIFIED TO RETURN");

        if (matchC.getRels().isEmpty()) {
            // no relationships, just return some nodes
            sql = obtainSelectClause(returnC, matchC, sql);

            sql = obtainFromClause(matchC, returnC, sql);

            sql = obtainWhereClause(matchC, sql, false, returnC);

            return sql;
        } else {
            // there are relationships to deal with, so use the WITH structure
            sql = obtainWithClause(sql, matchC);

            sql = obtainSelectClause(returnC, matchC, sql);

            sql = obtainFromClause(matchC, returnC, sql);

            sql = obtainWhereClause(matchC, sql, true, returnC);

            return sql;
        }
    }

    private static OrderClause orderDecode(List<String> orderClause) throws Exception {
        System.out.println(orderClause);
        OrderClause o = new OrderClause();

        List<CypOrder> items = new ArrayList<CypOrder>();

        List<String> currentWorking;

        // find all the separate parts of the return clause
        while (orderClause.contains(",")) {
            int posComma = orderClause.indexOf(",");
            currentWorking = orderClause.subList(0, posComma);
            orderClause = orderClause.subList(posComma + 1,
                    orderClause.size());

            items.add(extractOrder(currentWorking));
        }

        if (!orderClause.isEmpty()) {
            items.add(extractOrder(orderClause));
        }

        o.setItems(items);

        for (CypOrder c : o.getItems())
            System.out.println(c.toString());

        return o;
    }

    private static CypOrder extractOrder(List<String> clause) throws Exception {
        System.out.println(clause.toString());
        if (clause.size() == 4 && clause.contains(".")) {
            return new CypOrder(clause.get(0), clause.get(2), clause.get(3));
        } else if (clause.size() == 3 && clause.contains(".")) {
            return new CypOrder(clause.get(0), clause.get(2), "ASC");
        } else throw new Exception("RETURN CLAUSE MALFORMED");
    }

    private static StringBuilder obtainWithClause(StringBuilder sql, MatchClause matchC) {
        sql.append("WITH ");
        int indexRel = 0;

        for (CypRel cR : matchC.getRels()) {
            String withAlias = String.valueOf(alphabet[indexRel]);
            sql.append(withAlias).append(" AS (SELECT ");

            boolean includesWhere = false;
            int posOfRel = cR.getPosInClause();
            String relGenID = GenerateAlias.gen();

            CypNode leftNode = obtainNode(matchC, posOfRel);
            JsonObject leftProps = leftNode.getProps();
            CypNode rightNode = obtainNode(matchC, posOfRel + 1);
            JsonObject rightProps = rightNode.getProps();

            String[] tableA = leftNode.getAlias();
            String[] tableB = rightNode.getAlias();

            sql.append(tableA[1]).append(".id AS ").
                    append(withAlias).append(1).append(", ");

            sql.append(tableB[1]).append(".id AS ").
                    append(withAlias).append(2).append(" FROM ");

            if (cR.getDirection().equals("left")) {
                // flip the tables around to suit the direction
                String tempTable[] = tableB;
                tableB = tableA;
                tableA = tempTable;
            }

            sql.append(tableA[0]).append(" ").append(tableA[1]);

            sql.append(" INNER JOIN ");
            sql.append(cR.getType()).append(" ").append(relGenID).append(" ON ");
            sql.append(tableA[1]).append(".id = ").append(relGenID).append(".idl INNER JOIN ");
            sql.append(tableB[0]).append(" ").append(tableB[1]);
            sql.append(" ON ").append(relGenID).append(".idr = ").append(tableB[1]).append(".id");

            if (leftProps != null) {
                sql.append(" WHERE ");
                includesWhere = true;

                String table;

                if (cR.getDirection().equals("left"))
                    table = tableB[1];
                else
                    table = tableA[1];

                Set<Map.Entry<String, JsonElement>> entrySet = leftProps.entrySet();
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    sql.append(table).append(".").append(entry.getKey());
                    sql.append("='").append(entry.getValue().getAsString());
                    sql.append("' AND ");
                }
            }

            if (rightProps != null) {
                if (!includesWhere) {
                    sql.append(" WHERE ");
                    includesWhere = true;
                }

                String table;

                if (cR.getDirection().equals("left"))
                    table = tableA[1];
                else
                    table = tableB[1];

                Set<Map.Entry<String, JsonElement>> entrySet = rightProps.entrySet();
                for (Map.Entry<String, JsonElement> entry : entrySet) {
                    sql.append(table).append(".").append(entry.getKey());
                    sql.append("='").append(entry.getValue().getAsString());
                    sql.append("' AND ");
                }
            }

            if (includesWhere) sql.setLength(sql.length() - 5);
            sql.append("), ");
            indexRel++;
        }

        sql.setLength(sql.length() - 2);
        sql.append(" ");
        return sql;
    }

    private static StringBuilder obtainSelectClause(ReturnClause returnC, MatchClause matchC,
                                                    StringBuilder sql)
            throws Exception {
        sql.append("SELECT ");
        for (CypReturn cR : returnC.getItems()) {
            String prop = cR.getField();
            String table[] = obtainTable(cR.getNodeID(), matchC);
            sql.append(table[1]).append(".");
            if (prop != null)
                sql.append(prop);
            else
                sql.append("*");
            sql.append(", ");
        }

        sql.setLength(sql.length() - 2);
        return sql;
    }

    private static StringBuilder obtainFromClause(MatchClause matchC, ReturnClause returnC,
                                                  StringBuilder sql)
            throws Exception {
        sql.append(" FROM ");

        for (CypReturn ret : returnC.getItems()) {
            String table[] = obtainTable(ret.getNodeID(), matchC);
            sql.append(table[0]).append(" ").append(table[1]).append(", ");
        }

        if (!matchC.getRels().isEmpty()) {
            // deal with any relationships
            int numRels = matchC.getRels().size();
            for (int i = 0; i < numRels; i++)
                sql.append(alphabet[i]).append(", ");
        }

        sql.setLength(sql.length() - 2);

        return sql;
    }

    private static StringBuilder obtainWhereClause(MatchClause matchC, StringBuilder sql, boolean hasRel,
                                                   ReturnClause returnC) throws Exception {
        if (hasRel) {
            sql.append(" WHERE ");
            int numRels = matchC.getRels().size();
            for (int i = 0; i < numRels - 1; i++) {
                sql.append(alphabet[i]).append(".").append(alphabet[i]).append(2);
                sql.append(" = ");
                sql.append(alphabet[i + 1]).append(".").append(alphabet[i + 1]).append(1);
                sql.append(" AND ");
            }

            for (CypReturn cR : returnC.getItems()) {
                String table[] = obtainTable(cR.getNodeID(), matchC);
                int posInRel = obtainPos(matchC, cR.getNodeID());
                if (posInRel != -1) {
                    sql.append(table[1]).append(".id = ");
                    if (posInRel == 1) {
                        sql.append("a.a1 AND ");
                    } else {
                        posInRel -= 2;
                        sql.append(alphabet[posInRel]).append(".").append(alphabet[posInRel]).append(2);
                        sql.append(" AND ");
                    }
                }
            }

            sql.setLength(sql.length() - 5);

        } else {
            boolean whereClause = false;

            for (CypNode cN : matchC.getNodes()) {
                if (cN.getProps() != null) {
                    if (!whereClause) {
                        sql.append(" WHERE ");
                        whereClause = true;
                    }

                    JsonObject p = cN.getProps();
                    String table = cN.getAlias()[1];

                    Set<Map.Entry<String, JsonElement>> entrySet = p.entrySet();
                    for (Map.Entry<String, JsonElement> entry : entrySet) {
                        sql.append(table).append(".").append(entry.getKey());
                        sql.append("='").append(entry.getValue().getAsString());
                        sql.append("' AND ");
                    }
                }
            }


            if (whereClause) sql.setLength(sql.length() - 5);
        }

        return sql;
    }

    private static int obtainPos(MatchClause matchC, String cR) {
        for (CypNode c : matchC.getNodes()) {
            if (c.getId().equals(cR)) {
                return c.getPosInClause();
            }
        }
        return -1;
    }

    private static CypNode obtainNode(MatchClause matchC, int i) {
        for (CypNode c : matchC.getNodes()) {
            if (c.getPosInClause() == i) {
                return c;
            }
        }
        return null;
    }

    private static String[] obtainTable(String nodeID, MatchClause matchC)
            throws Exception {
        for (CypNode cN : matchC.getNodes()) {
            if (nodeID.equals(cN.getId())) {
                return cN.getAlias();
            }
        }
        throw new Exception("MATCH AND RETURN CLAUSES INCOMPATIBLE");
    }

    private static ReturnClause returnDecode(List<String> returnClause) throws Exception {
        ReturnClause r = new ReturnClause();

        List<CypReturn> items = new ArrayList<CypReturn>();

        List<String> currentWorking;

        // find all the separate parts of the return clause
        while (returnClause.contains(",")) {
            int posComma = returnClause.indexOf(",");
            currentWorking = returnClause.subList(0, posComma);
            returnClause = returnClause.subList(posComma + 1,
                    returnClause.size());

            items.add(extractReturn(currentWorking));
        }

        if (!returnClause.isEmpty()) {
            items.add(extractReturn(returnClause));
        }

        r.setItems(items);

        for (CypReturn c : r.getItems())
            System.out.println(c.toString());

        return r;
    }

    private static CypReturn extractReturn(List<String> clause) throws Exception {
        if (clause.size() == 3 && clause.contains(".")) {
            return new CypReturn(clause.get(0), clause.get(2));
        } else if (clause.size() == 1) {
            return new CypReturn(clause.get(0), null);
        } else throw new Exception("RETURN CLAUSE MALFORMED");
    }

    // current status: deals with no relation or one relation
    private static MatchClause matchDecode(List<String> matchClause) {
        MatchClause m = new MatchClause();

        // extract the nodes from the match clause
        m.setNodes(extractNodes(matchClause, m));

        // reset ID between method calls
        m.resetInternalID();

        // extract any relationships from the match clause
        m.setRels(extractRels(matchClause, m));

        for (CypNode c : m.getNodes()) {
            System.out.println(c.toString());
        }

        for (CypRel c : m.getRels()) {
            System.out.println(c.toString());
        }

        return m;
    }

    private static ArrayList<CypRel> extractRels(List<String> clause,
                                                 MatchClause m) {
        ArrayList<CypRel> rels = new ArrayList<CypRel>();

        JsonObject o;
        String direction;
        List<String> relString;
        List<String> propsString;
        String id;
        String type;

        while (!clause.isEmpty()) {
            id = null;
            type = null;
            propsString = null;
            o = null;
            direction = null;

            int lSq = clause.indexOf("[");
            int rSq = clause.indexOf("]");

            if (lSq != -1 && rSq != -1) {
                String tokBeforeLSQ1 = clause.get(lSq - 1);
                String tokAfterRSQ1 = clause.get(rSq + 1);
                String tokBeforeLSQ2 = clause.get(lSq - 2);
                String tokAfterRSQ2 = clause.get(rSq + 2);

                if (tokBeforeLSQ1.equals("-") &&
                        tokAfterRSQ1.equals("-")) {
                    // is a valid relationship structure
                    relString = clause.subList(lSq + 1, rSq);
                    if (tokAfterRSQ2.equals(">")) {
                        direction = "right";
                        clause = clause.subList(rSq + 3, clause.size());
                    } else {
                        clause = clause.subList(rSq + 2, clause.size());
                        if (tokBeforeLSQ2.equals("<")) {
                            direction = "left";
                        } else {
                            direction = "none";
                        }
                    }

                    if (relString.contains("{")) {
                        int lCurly = relString.indexOf("{");
                        int rCurly = relString.indexOf("}");

                        if (lCurly != -1 && rCurly != -1) {
                            propsString = relString.subList(lCurly + 1, rCurly);
                            relString = relString.subList(0, lCurly);
                        }
                    }

                    String[] temp = extractIdAndType(relString);
                    id = temp[0];
                    type = temp[1];

                    if (propsString != null) {
                        o = getJSONProps(propsString);
                    }
                }
            } else {
                // may be a relationship still there
                if (clause.contains("<")) {
                    int posArrow = clause.indexOf("<");
                    direction = "left";
                    clause = clause.subList(posArrow + 3, clause.size());
                } else if (clause.contains(">")) {
                    int posArrow = clause.indexOf(">");
                    direction = "right";
                    clause = clause.subList(posArrow + 1, clause.size());
                } else if (clause.contains("-")) {
                    int posDash = clause.indexOf("-");
                    if (clause.get(posDash + 1).equals("-")) {
                        direction = "none";
                        clause = clause.subList(posDash + 2, clause.size());
                    }
                } else
                    break;
            }
            rels.add(new CypRel(m.getInternalID(), id, type, o, direction));
        }
        return rels;
    }

    private static String[] extractIdAndType(List<String> tokens) {
        String[] toReturn = {null, null};
        if (tokens.size() == 3) {
            toReturn[0] = tokens.get(0);
            toReturn[1] = tokens.get(2);
        } else if (tokens.size() == 2) {
            toReturn[1] = tokens.get(1);
        } else if (tokens.size() == 1) {
            toReturn[0] = tokens.get(0);
        }
        return toReturn;
    }

    private static JsonObject getJSONProps(List<String> propsString) {
        JsonParser parser = new JsonParser();
        StringBuilder temp = new StringBuilder();
        int i = 0;

        for (String a : propsString) {
            if (i % 3 == 0) {
                temp.append("{\"").append(a).append("\"");
            }
            if (i % 3 == 1) {
                temp.append(":");
            }
            if (i % 3 == 2) {
                temp.append(a).append("}, ");
            }
            i++;
        }

        temp.setLength(temp.length() - 2);
        return parser.parse(temp.toString()).getAsJsonObject();
    }

    private static ArrayList<CypNode> extractNodes(List<String> clause,
                                                   MatchClause m) {
        ArrayList<CypNode> nodes = new ArrayList<CypNode>();
        Map<String, Integer> nodeIDS = new HashMap<String, Integer>();

        JsonObject o;
        List<String> nodeString;
        List<String> propsString;

        String id;
        String type;

        while (!clause.isEmpty()) {
            nodeString = null;
            propsString = null;
            o = null;

            // find nodes in the clause
            int lBrack = clause.indexOf("(");
            int rBrack = clause.indexOf(")");

            // check if a node has been found or not
            if (lBrack != -1 && rBrack != -1) {
                // extract the inner node tokens
                nodeString = clause.subList(lBrack + 1, rBrack);

                // keep rest of clause safe
                clause = clause.subList(rBrack + 1, clause.size());

                while (!nodeString.isEmpty()) {
                    // extract any properties from the nodes
                    int lCurly = nodeString.indexOf("{");
                    int rCurly = nodeString.indexOf("}");

                    if (lCurly != -1 && rCurly != -1) {
                        propsString = nodeString.subList(lCurly + 1, rCurly);
                        nodeString = nodeString.subList(0, lCurly);
                    } else {
                        break;
                    }
                }
            }

            if (nodeString != null) {
                if (propsString != null) {
                    o = getJSONProps(propsString);
                }

                String[] temp = extractIdAndType(nodeString);
                id = temp[0];
                type = temp[1];

                // add the formatted node object to list of nodes
                // associated with the match clause
                int internalID = m.getInternalID();

                if (id != null && !nodeIDS.containsKey(id)) {
                    nodeIDS.put(id, internalID);
                } else if (nodeIDS.containsKey(id)) {
                    type = nodes.get(nodeIDS.get(id) - 1).getType();
                    o = nodes.get(nodeIDS.get(id) - 1).getProps();
                }
                nodes.add(new CypNode(internalID, id, type, o));
            }
        }
        return nodes;
    }
}
