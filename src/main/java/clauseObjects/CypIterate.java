package clauseObjects;

public class CypIterate {
    private String sql;
    private String originalCypherInput;
    private String loopQuery;
    private String loopIndexTo;
    private String loopIndexFrom;
    private String collectIndex;
    private String returnStatement;


    public CypIterate(String cypher) {
        this.originalCypherInput = cypher;
    }

    public String getOriginalCypherInput(){
        return this.originalCypherInput;
    }

    public void setLoopIndexTo(String loopIndexTo) {
        this.loopIndexTo = loopIndexTo;
    }

    public void setLoopIndexFrom(String loopIndexFrom) {
        this.loopIndexFrom = loopIndexFrom;
    }

    public void setCollectIndex(String collectIndex) {
        this.collectIndex = collectIndex;
    }

    public void setReturnStatement(String returnStatement) {
        this.returnStatement = returnStatement;
    }

    @Override
    public String toString() {
        String ret = "";
        ret += "ORIGINAL INPUT : " + originalCypherInput + "\n";
        ret += "LOOP QUERY : " + loopQuery + "\n";
        ret += "INDEX FROM : " + loopIndexFrom + " -- INDEX TO : " + loopIndexTo + "\n";
        ret += "COLLECTING ON : " + collectIndex + "\n";
        ret += "RETURN STATEMENT : " + returnStatement;
        return ret;
    }

    public String getLoopQuery() {
        return loopQuery;
    }

    public void setLoopQuery(String loopQuery) {
        this.loopQuery = loopQuery;
    }

    public String getSQL() {
        return this.sql;
    }

    public void setSQL(String SQL) {
        this.sql = SQL;
    }

    public String getLoopIndexTo() {
        return loopIndexTo;
    }
}
