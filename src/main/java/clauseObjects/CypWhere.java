package clauseObjects;

public class CypWhere {
    private String cluase1;
    private String op;

    public CypWhere(String c, String opType) {
        this.cluase1 = c;
        this.op = opType;
    }

    public String getCluase1() {
        return cluase1;
    }

    public String getOp() {
        return op;
    }

    @Override
    public String toString() {
        return cluase1 + " : " + op;
    }
}
