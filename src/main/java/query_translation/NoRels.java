package query_translation;

import clauseObjects.*;
import production.c2sqlV2;

import java.util.Map;

class NoRels {
    private static boolean useOptimalTable = false;

    static StringBuilder translate(StringBuilder sql, DecodedQuery decodedQuery) {
        useOptimalTable = false;
        sql = getSelect(decodedQuery.getRc(), decodedQuery.getMc(), sql,
                decodedQuery.getCypherAdditionalInfo().hasDistinct(),
                decodedQuery.getCypherAdditionalInfo().getAliasMap());
        sql = getFrom(sql, decodedQuery.getMc(), decodedQuery.getRc());
        sql = getWhere(sql, decodedQuery.getRc(), decodedQuery.getMc(), decodedQuery.getWc());
        return sql;
    }

    private static StringBuilder getSelect(ReturnClause rc, MatchClause mc,
                                           StringBuilder sql, boolean hasDistinct, Map<String, String> alias) {
        sql.append("SELECT ");
        if (hasDistinct) sql.append("DISTINCT ");

        for (CypReturn r : rc.getItems()) {
            if (r.getNodeID() == null && r.getField().equals("*")) {
                sql.append("*");
            } else if (r.getCount()) {
                sql.append("count(n)");
            } else {
                for (CypNode cN : mc.getNodes()) {
                    if (r.getNodeID().equals(cN.getId())) {
                        String prop = r.getField();
                        if (prop != null) {
                            sql.append("n").append(".").append(prop)
                                    .append(useAlias(r.getNodeID(), r.getField(), alias)).append(", ");
                        } else {
                            sql.append("n.*").append(useAlias(r.getNodeID(), r.getField(), alias)).append(", ");
                        }
                        break;
                    }
                }
            }
        }

        if (sql.toString().endsWith(", ")) sql.setLength(sql.length() - 2);
        sql.append(" ");
        return sql;
    }

    /**
     * @param nodeID
     * @param field
     * @param alias  @return
     */
    private static String useAlias(String nodeID, String field, Map<String, String> alias) {
        if (alias.isEmpty()) {
            return "";
        } else {
            for (String s : alias.keySet()) {
                String key = s.split(" AS ")[0];
                if (field != null) {
                    if (key.equals((nodeID) + "." + (field))) {
                        return (" AS " + alias.get(s));
                    }
                } else {
                    if (key.equals(nodeID)) {
                        return (" AS " + alias.get(s));
                    }
                }
            }
        }
        return "";
    }

    private static StringBuilder getFrom(StringBuilder sql, MatchClause mc, ReturnClause rc) {
        sql.append("FROM ");
        String table = TranslateUtils.getLabelType(mc.getNodes().get(0).getType());
        if (!table.equals("nodes")) {
            useOptimalTable = true;
        } else {
            boolean possibleOpti = true;
            String possTable = "nodes";

            for (CypReturn cR : rc.getItems()) {
                if (!c2sqlV2.mapLabels.containsKey(cR.getField())) {
                    possibleOpti = false;
                    break;
                } else {
                    String newTable = c2sqlV2.mapLabels.get(cR.getField());
                    if (!possTable.equals(newTable) && !possTable.equals("nodes")) {
                        possibleOpti = false;
                        break;
                    }
                    possTable = newTable;
                }
            }

            if (possibleOpti) table = possTable;
            else table = "nodes";
        }
        sql.append(table);
        sql.append(" n");
        return sql;
    }

    /**
     * Obtain WHERE clause of SQL when there are no relationships to deal with.
     *
     * @param sql     Existing SQL.
     * @param returnC Return Clause of original Cypher query.
     * @param matchC  Match Clause of original Cypher query.
     * @param wc
     * @return New SQL.
     */
    private static StringBuilder getWhere(StringBuilder sql, ReturnClause returnC,
                                          MatchClause matchC, WhereClause wc) {
        boolean hasWhere = false;

        for (CypReturn cR : returnC.getItems()) {
            if (cR.getNodeID() == null && cR.getField().equals("*")) {
                CypNode cN = matchC.getNodes().get(0);
                sql.append(" WHERE n.label LIKE ").append(TranslateUtils.genLabelLike(cN));
                if (cN.getProps() != null) {
                    sql.append(" AND ");
                    sql = TranslateUtils.getWholeWhereClause(sql, cN, wc);
                }
            } else {
                CypNode cN = null;

                for (CypNode c : matchC.getNodes()) {
                    if (c.getId().equals(cR.getNodeID()))
                        cN = c;
                }

                if (cN != null) {
                    if (cN.getProps() != null) {
                        if (!hasWhere) {
                            sql.append(" WHERE ");
                            hasWhere = true;
                        }
                        sql = TranslateUtils.getWholeWhereClause(sql, cN, wc);
                    }

                    if (cN.getType() != null && !useOptimalTable) {
                        if (!hasWhere) {
                            sql.append(" WHERE n.label LIKE");
                            hasWhere = true;
                        } else sql.append(" n.label LIKE");
                        sql.append(" ").append(TranslateUtils.genLabelLike(cN));
                    }
                }
            }
            sql.append(" AND ");
        }

        sql.setLength(sql.length() - 5);
        return sql;
    }
}
