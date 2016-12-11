package clauseObjects;

/**
 * Class for storing the return fields of a Cypher query.
 */
public class CypReturn {
    private String nodeID;
    private String field;
    private String type;
    private boolean count;
    private boolean collect;
    private int posInClause;

    public CypReturn(String id, String f, boolean count_x, boolean collect_x, MatchClause matchC) {
        this.nodeID = id;
        this.field = f;
        this.count = count_x;
        this.collect = collect_x;
        if (this.nodeID != null) {
            // discoverType finds out whether we are returning a node or a
            // relationship.
            this.type = discoverType(this.nodeID, matchC);
        } else {
            // TODO: is this needed still?
            this.type = "node";
            this.posInClause = 1;
        }
    }

    private String discoverType(String nodeID, MatchClause matchC) {
        //check the nodes first
        for (CypNode cN : matchC.getNodes()) {
            if (cN.getId() != null && cN.getId().equals(nodeID)) {
                posInClause = cN.getPosInClause();
                return "node";
            }
        }

        //check relationships
        for (CypRel cR : matchC.getRels()) {
            if (cR.getId() != null && cR.getId().equals(nodeID)) {
                posInClause = cR.getPosInClause();
                return "rel";
            }
        }

        //TODO: is this needed?
        posInClause = -1;
        return null;
    }

    public String getNodeID() {
        return nodeID;
    }

    public String getField() {
        return field;
    }

    public boolean getCount() {
        return count;
    }

    public boolean getCollect() {
        return collect;
    }

    @Override
    public String toString() {
        return "(ID:" + this.nodeID +
                ",FIELD:" + this.field + ",TYPE:" + this.type + ",COUNT:" + this.count +
                ",COLLECT:" + this.collect + ")";
    }

    public String getType() {
        return type;
    }

    public int getPosInClause() {
        return posInClause;
    }
}
