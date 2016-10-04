package clauseObjects;

public class CypReturn {
    private String nodeID;
    private String field;

    public CypReturn(String id, String f){
        this.nodeID = id;
        this.field = f;
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
}
