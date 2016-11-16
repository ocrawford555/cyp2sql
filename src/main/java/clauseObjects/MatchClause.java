package clauseObjects;

import java.util.ArrayList;

/**
 * Stores all the CypNodes, CypRels, and has an internal ID to keep track of order.
 */
public class MatchClause {
    private ArrayList<CypNode> nodes = new ArrayList<>();
    private ArrayList<CypRel> rels = new ArrayList<>();

    // varRel indicates that a relationshiop of the type -[*a..b]- is being used.
    private boolean varRel = false;

    private int internalID;

    public MatchClause() {
        internalID = 0;
    }

    public ArrayList<CypNode> getNodes() {
        return nodes;
    }

    public void setNodes(ArrayList<CypNode> nodes) {
        this.nodes = nodes;
    }

    public ArrayList<CypRel> getRels() {
        return rels;
    }

    public void setRels(ArrayList<CypRel> rels) {
        this.rels = rels;
    }

    public int getInternalID() {
        internalID++;
        return internalID;
    }

    public void resetInternalID() {
        internalID = 0;
    }

    public boolean isVarRel() {
        return varRel;
    }

    public void setVarRel(boolean hasVarRel) {
        this.varRel = hasVarRel;
    }
}
