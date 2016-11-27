package clauseObjects;

import com.google.gson.JsonObject;

/**
 * Class for storing nodes from a Cypher query.
 */
public class CypNode {
    private int posInClause;
    private String id;
    private String labels;
    private JsonObject props;

    public CypNode(int posInClause, String id, String type, JsonObject props) {
        this.posInClause = posInClause;
        this.id = id;
        this.labels = type;
        this.props = props;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return labels;
    }

    public JsonObject getProps() {
        return props;
    }

    public void setProps(JsonObject newProps) {
        this.props = newProps;
    }

    @Override
    public String toString() {
        return "(ID:" + this.id + ",LABELS:" + this.labels + ",PROPS:"
                + this.props + ",POS:" + this.posInClause + ")";
    }

    public int getPosInClause() {
        return posInClause;
    }
}
