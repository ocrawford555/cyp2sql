package clauseObjects;

public class CypReturn {
    private String nodeID;
    private String field;
    private String type;
    private int posInClause;

    public CypReturn(String id, String f, MatchClause matchC) {
        this.nodeID = id;
        this.field = f;
        this.type = discoverType(this.nodeID, matchC);
    }

    private String discoverType(String nodeID, MatchClause matchC) {
        //check the nodes first
        for (CypNode cN : matchC.getNodes()) {
            if (cN.getId().equals(nodeID)) {
                posInClause = cN.getPosInClause();
                return "node";
            }
        }
        //check relationships
        for (CypRel cR : matchC.getRels()) {
            if (cR.getId().equals(nodeID)) {
                posInClause = cR.getPosInClause();
                return "rel";
            }
        }
        posInClause = -1;
        return null;
    }

    public String getNodeID() {
        return nodeID;
    }

    public String getField() {
        return field;
    }

    @Override
    public String toString(){
        return "(ID:" + this.nodeID + ",FIELD:" + this.field + ")";
    }

    public String getType() {
        return type;
    }

    public int getPosInClause() {
        return posInClause;
    }
}
