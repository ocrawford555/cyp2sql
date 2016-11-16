package toolv1;

import clauseObjects.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CypherTranslator {
    private static JsonParser parser = new JsonParser();

    static DecodedQuery generateDecodedQuery(ArrayList<String> tokenList, CypherWalker cypherQ) throws Exception {
        // find positions of the tokens in the query (-1 means not found)
        int posOfMatch = tokenList.indexOf("match");
        int posOfWhere = tokenList.indexOf("where");
        int posOfReturn = tokenList.indexOf("return");
        int posOfOrder = tokenList.indexOf("order");
        int posOfSkip = tokenList.indexOf("skip");
        int posOfLimit = tokenList.indexOf("limit");

        // for time being (v1), MATCH and RETURN always present
        List<String> matchClause;
        List<String> returnClause;
        List<String> orderClause = null;

        if (cypherQ.doesCluaseHaveWhere())
            matchClause = tokenList.subList(posOfMatch + 1, posOfWhere);
        else
            matchClause = tokenList.subList(posOfMatch + 1, posOfReturn);

        if (posOfOrder == -1) {
            if (posOfSkip == -1 && posOfLimit == -1) {
                returnClause = tokenList.subList(posOfReturn + 1 +
                        ((cypherQ.hasDistinct()) ? 1 : 0), tokenList.size());
            } else if (posOfLimit == -1) {
                returnClause = tokenList.subList(posOfReturn + 1 + ((cypherQ.hasDistinct()) ? 1 : 0), posOfSkip);
            } else if (posOfSkip == -1) {
                returnClause = tokenList.subList(posOfReturn + 1 + ((cypherQ.hasDistinct()) ? 1 : 0), posOfLimit);
            } else {
                returnClause = tokenList.subList(posOfReturn + 1 + ((cypherQ.hasDistinct()) ? 1 : 0), posOfSkip);
            }
        } else {
            returnClause = tokenList.subList(posOfReturn + 1 + ((cypherQ.hasDistinct()) ? 1 : 0), posOfOrder);

            if (posOfSkip == -1 && posOfLimit == -1) {
                orderClause = tokenList.subList(posOfOrder + 2, tokenList.size());
            } else if (posOfLimit == -1) {
                orderClause = tokenList.subList(posOfOrder + 2, posOfSkip);
            } else if (posOfSkip == -1) {
                orderClause = tokenList.subList(posOfOrder + 2, posOfLimit);
            } else {
                orderClause = tokenList.subList(posOfOrder + 2, posOfSkip);
            }
        }

        MatchClause matchC = matchDecode(matchClause);
        ReturnClause returnC = returnDecode(returnClause, matchC, cypherQ);
        OrderClause orderC = null;

        // if ORDER BY is present in the query
        if (orderClause != null)
            orderC = orderDecode(orderClause);

        int skipAmount = (posOfSkip != -1) ? cypherQ.getSkipAmount() : -1;
        int limitAmount = (posOfLimit != -1) ? cypherQ.getLimitAmount() : -1;

        if (cypherQ.doesCluaseHaveWhere()) {
            whereDecode(matchC, cypherQ);
        }

        return new DecodedQuery(matchC, returnC, orderC, skipAmount, limitAmount, cypherQ);
    }

    // current status: unsure, test
    private static MatchClause matchDecode(List<String> matchClause) throws Exception {
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

    // tested and working
    private static ArrayList<CypNode> extractNodes(List<String> clause, MatchClause m) {
        // nodes to return at the end of the function.
        ArrayList<CypNode> nodes = new ArrayList<>();
        // keep track of current nodes if node of the same ID reappears later on in the query.
        Map<String, Integer> nodeIDS = new HashMap<>();

        JsonObject o;
        List<String> nodeString;
        List<String> propsString;

        String id;
        String labels;

        while (!clause.isEmpty()) {
            nodeString = null;
            propsString = null;
            o = null;

            // find nodes in the clause
            int lBrack = clause.indexOf("(");
            int rBrack = clause.indexOf(")");

            // check if node structure valid
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

                String[] idAndLabels = extractIdAndLabels(nodeString);
                id = idAndLabels[0];
                labels = idAndLabels[1];

                // add the formatted node object to list of nodes
                // associated with the match clause
                int internalID = m.getInternalID();

                if (id != null && !nodeIDS.containsKey(id)) {
                    nodeIDS.put(id, internalID);
                } else if (nodeIDS.containsKey(id)) {
                    labels = nodes.get(nodeIDS.get(id) - 1).getType();
                    o = nodes.get(nodeIDS.get(id) - 1).getProps();
                }
                nodes.add(new CypNode(internalID, id, labels, o));
            }
        }
        return nodes;
    }

    private static ArrayList<CypRel> extractRels(List<String> clause, MatchClause m) throws Exception {
        ArrayList<CypRel> rels = new ArrayList<>();

        /*
          types of relationships
          -- & -[]-        (id: null     type: null      props: null     direction : none)
          <-- & <-[]-      (id: null     type: null      props: null     direction : left)
          --> & -[]->      (id: null     type: null      props: null     direction : right)
          -[:a]-           (id: null     type: a         props: null     direction : none)
          -[b:a]-          (id: b        type: a         props: null     direction : none)
          <-[:a]-          (id: null     type: a         props: null     direction : left)
          <-[b:a]-         (id: b        type: a         props: null     direction : left)
          -[:a]->          (id: null     type: a         props: null     direction : right)
          -[b:a]->         (id: b        type: a         props: null     direction : right)
          -[:a {c}]-       (id: null     type: a         props: c        direction : none)
          -[b:a {c}]-      (id: b        type: a         props: c        direction : none)
          <-[:a {c}]-      (id: null     type: a         props: c        direction : left)
          <-[b:a {c}]-     (id: b        type: a         props: c        direction : left)
          -[:a {c}]->      (id: null     type: a         props: c        direction : right)
          -[b:a {c}]->     (id: b        type: a         props: c        direction : right)

          -[*1..2]->        (id: null     type: null      props: null     direction : var1-2)
          <-[*]-            (id: null     type: null      props: null     direction : var)
          -[*1..4]->        (id: null     type: null      props: null     direction : var1-4)
          <-[*3..4]-        (id: null     type: null      props: null     direction : var3-4)
         */

        String id;
        String type;
        JsonObject o;
        String direction;
        List<String> relString;
        List<String> propsString;

        while (!clause.isEmpty()) {
            id = null;
            type = null;
            propsString = null;
            o = null;
            direction = null;

            if (!clause.contains("-")) {
                // no relationships left to consider.
                break;
            } else if (clause.contains("*")) {
                String varD = "none";
                m.setVarRel(true);

                int posOfLHypher = clause.indexOf("-");
                if (clause.get(posOfLHypher - 1).equals("<")) varD = "left";
                int posOfRSq = clause.indexOf("]");
                if (clause.get(posOfRSq + 2).equals(">")) varD = (varD.equals("left")) ? "none" : "right";

                List<String> varRel = clause.subList(posOfLHypher + 2, posOfRSq);
                clause = clause.subList(posOfRSq + 2, clause.size());
                rels.add(extractVarRel(varRel, m, varD));
            } else {
                int posOfHyphen = clause.indexOf("-");

                if (clause.get(posOfHyphen - 1).equals("<")) {
                    direction = "left";

                    int lSq = clause.indexOf("[");
                    int rSq = clause.indexOf("]");

                    if (lSq != -1 && rSq != -1) {
                        relString = clause.subList(lSq + 1, rSq);

                        if (relString.contains("{")) {
                            int lCurly = relString.indexOf("{");
                            int rCurly = relString.indexOf("}");

                            if (lCurly != -1 && rCurly != -1) {
                                propsString = relString.subList(lCurly + 1, rCurly);
                                relString = relString.subList(0, lCurly);
                            }
                        }

                        String[] temp = extractIdAndLabels(relString);
                        id = temp[0];
                        type = temp[1];

                        if (propsString != null) {
                            o = getJSONProps(propsString);
                        }
                        clause = clause.subList(rSq + 2, clause.size());
                    } else {
                        clause = clause.subList(posOfHyphen + 2, clause.size());
                    }
                } else if (clause.get(posOfHyphen + 1).equals("-") &&
                        clause.get(posOfHyphen + 2).equals("(")) {
                    direction = "none";
                    clause = clause.subList(posOfHyphen + 2, clause.size());
                } else if (clause.get(posOfHyphen + 1).equals("-") &&
                        clause.get(posOfHyphen + 2).equals(">")) {
                    direction = "right";
                    clause = clause.subList(posOfHyphen + 3, clause.size());
                } else if (clause.get(posOfHyphen + 1).equals("[")) {
                    int lSq = clause.indexOf("[");
                    int rSq = clause.indexOf("]");

                    if (lSq != -1 && rSq != -1) {
                        relString = clause.subList(lSq + 1, rSq);

                        if (relString.contains("{")) {
                            int lCurly = relString.indexOf("{");
                            int rCurly = relString.indexOf("}");

                            if (lCurly != -1 && rCurly != -1) {
                                propsString = relString.subList(lCurly + 1, rCurly);
                                relString = relString.subList(0, lCurly);
                            }
                        }

                        String[] temp = extractIdAndLabels(relString);
                        id = temp[0];
                        type = temp[1];

                        if (propsString != null) {
                            o = getJSONProps(propsString);
                        }

                        if (clause.get(rSq + 1).equals("-") && clause.get(rSq + 2).equals(">")) {
                            direction = "right";
                            clause = clause.subList(rSq + 3, clause.size());
                        } else if (clause.get(rSq + 1).equals("-")) {
                            direction = "none";
                            clause = clause.subList(rSq + 2, clause.size());
                        }
                    }
                } else {
                    throw new Exception("RELATIONSHIP STRUCTURE IS INVALID");
                }
                rels.add(new CypRel(m.getInternalID(), id, type, o, direction));
            }
        }
        return rels;
    }

    private static JsonObject getJSONProps(List<String> propsString) {
        StringBuilder json = new StringBuilder();

        json.append("{");

        int i = 0;
        for (String a : propsString) {
            if (a.equals(",")) {
                continue;
            }
            if (i % 3 == 0) {
                json.append("\"").append(a).append("\"");
            }
            if (i % 3 == 1) {
                json.append(":");
            }
            if (i % 3 == 2) {
                json.append(a).append(", ");
            }
            i++;
        }

        // remove trailing comma
        json.setLength(json.length() - 2);
        json.append("}");
        return parser.parse(json.toString()).getAsJsonObject();
    }

    private static String[] extractIdAndLabels(List<String> tokens) {
        String[] idAndLabels = {null, null};

        if (tokens.isEmpty()) return idAndLabels;

        if (tokens.get(0).equals(":")) {
            // no id, just label(s)
            for (String label : tokens) {
                if (!label.equals(":")) {
                    if (idAndLabels[1] == null) {
                        idAndLabels[1] = label;
                    } else {
                        idAndLabels[1] = idAndLabels[1] + ", " + label;
                    }
                }
            }
            return idAndLabels;
        } else {
            idAndLabels[0] = tokens.get(0);
            if (tokens.size() == 1) {
                return idAndLabels;
            } else {
                tokens = tokens.subList(1, tokens.size());
                for (String label : tokens) {
                    if (!label.equals(":")) {
                        if (idAndLabels[1] == null) {
                            idAndLabels[1] = label;
                        } else {
                            idAndLabels[1] = idAndLabels[1] + ", " + label;
                        }
                    }
                }
                return idAndLabels;
            }
        }
    }

    private static void whereDecode(MatchClause matchC, CypherWalker cypherQ) throws Exception {
        WhereClause wc = new WhereClause(cypherQ.getWhereClause());
        while (!wc.getClause().isEmpty()) {
            extractWhere(wc.getClause(), matchC, wc);
        }
    }

    private static WhereClause extractWhere(String clause, MatchClause matchC, WhereClause wc) throws Exception {
        if (clause.contains(" or ")) {
            System.out.println("hello...");
            String[] items = clause.split(" or ");
            wc.setHasOr(true);
            wc.addToOr(items);
            for (String item : items) {
                extractWhere(item, matchC, wc);
            }
        } else {
            wc.setClause(wc.getClause().substring(clause.length()));
            if (clause.contains(" = ")) {
                String[] idAndValue = clause.split(" = ");
                addCondition(idAndValue, matchC, "equals");
            } else if (clause.contains(" <> ")) {
                String[] idAndValue = clause.split(" <> ");
                addCondition(idAndValue, matchC, "nequals");
            }
        }
        return wc;
    }

    private static void addCondition(String[] idAndValue, MatchClause matchC, String op) throws Exception {
        String[] idAndProp = idAndValue[0].split("\\.");

        for (CypNode cN : matchC.getNodes()) {
            if (cN.getId().equals(idAndProp[0])) {
                JsonObject obj = cN.getProps();
                if (obj == null) obj = new JsonObject();
                if (op.equals("equals")) obj.addProperty(idAndProp[1], idAndValue[1].replace("\"", "").toLowerCase());
                else obj.addProperty(idAndProp[1], "<#" + idAndValue[1].replace("\"", "").toLowerCase() + "#>");
                cN.setProps(obj);
                return;
            }
        }

        for (CypRel cR : matchC.getRels()) {
            if (cR.getId().equals(idAndProp[0])) {
                JsonObject obj = cR.getProps();
                if (obj == null) obj = new JsonObject();
                if (op.equals("equals")) obj.addProperty(idAndProp[1], idAndValue[1].replace("\"", "").toLowerCase());
                else obj.addProperty(idAndProp[1], "<#" + idAndValue[1].replace("\"", "").toLowerCase() + "#>");
                cR.setProps(obj);
                return;
            }
        }
        throw new Exception("WHERE CLAUSE MALFORMED");
    }

    private static CypRel extractVarRel(List<String> varRel, MatchClause m, String varD) throws Exception {
        varRel = varRel.subList(1, varRel.size());
        String direction;
        if (varRel.size() == 1) {
            direction = "var" + varRel.get(0) + "#" + varD;
        } else {
            direction = "var" + varRel.get(0) + "-" + varRel.get(2) + "#" + varD;
        }
        return new CypRel(m.getInternalID(), null, null, null, direction);
    }

    private static ReturnClause returnDecode(List<String> returnClause,
                                             MatchClause matchC, CypherWalker cypherQ) throws Exception {
        ReturnClause r = new ReturnClause();
        List<CypReturn> items = new ArrayList<>();

        List<String> currentWorking;

        // find all the separate parts of the return clause
        while (returnClause.contains(",")) {
            int posComma = returnClause.indexOf(",");
            currentWorking = returnClause.subList(0, posComma);
            returnClause = returnClause.subList(posComma + 1, returnClause.size());

            items.add(extractReturn(currentWorking, matchC, cypherQ));
        }

        if (!returnClause.isEmpty()) {
            items.add(extractReturn(returnClause, matchC, cypherQ));
        }

        r.setItems(items);

        for (CypReturn c : r.getItems())
            System.out.println(c.toString());

        return r;
    }

    private static CypReturn extractReturn(List<String> clause, MatchClause matchC, CypherWalker cypherQ)
            throws Exception {
        // if clause of type (id).(property)
        if (clause.size() == 3 && clause.contains(".")) {
            return new CypReturn(clause.get(0), clause.get(2), matchC);
        } else if (clause.size() == 1)
            if (clause.get(0).equals("*"))
                return new CypReturn(null, "*", null);
            else
                return new CypReturn(clause.get(0), null, matchC);
        else if (cypherQ.hasCount())
            return new CypReturn(null, "count(" + clause.get(2) + ")", null);
        else throw new Exception("RETURN CLAUSE MALFORMED");
    }

    private static OrderClause orderDecode(List<String> orderClause) throws Exception {
        OrderClause o = new OrderClause();

        List<CypOrder> items = new ArrayList<>();

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
            return new CypOrder(clause.get(0), clause.get(2), "asc");
        } else if (clause.size() == 5 && clause.contains("count")) {
            return new CypOrder(clause.get(2), "count(n)", clause.get(4));
        } else throw new Exception("ORDER CLAUSE MALFORMED");
    }

}
