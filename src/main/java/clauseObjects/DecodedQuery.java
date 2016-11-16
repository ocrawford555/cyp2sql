package clauseObjects;

import toolv1.CypherWalker;

public class DecodedQuery {
    // this contains all of the intermediate representation of the Cypher query

    private MatchClause mc;
    private ReturnClause rc;
    private OrderClause oc;
    private int skipAmount;
    private int limitAmount;
    private CypherWalker cypherAdditionalInfo;
    private String sqlEquiv;

    public DecodedQuery(MatchClause m, ReturnClause r, OrderClause o,
                        int skip, int limit, CypherWalker c) {
        this.mc = m;
        this.rc = r;
        this.oc = o;
        this.skipAmount = skip;
        this.limitAmount = limit;
        this.cypherAdditionalInfo = c;
    }

    public MatchClause getMc() {
        return mc;
    }

    public ReturnClause getRc() {
        return rc;
    }

    public OrderClause getOc() {
        return oc;
    }

    public int getSkipAmount() {
        return skipAmount;
    }

    public int getLimitAmount() {
        return limitAmount;
    }

    public CypherWalker getCypherAdditionalInfo() {
        return cypherAdditionalInfo;
    }

    public String getSqlEquiv() {
        return sqlEquiv;
    }

    public void setSqlEquiv(String sqlEquiv) {
        this.sqlEquiv = sqlEquiv;
    }
}
