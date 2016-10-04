package clauseObjects;

import com.google.gson.JsonObject;
import toolv1.GenerateAlias;

public class CypNode {
    private int posInClause;
    private String id;
    private String type;
    private JsonObject props;
    private String[] alias = {null, null};

    public CypNode(int posInClause, String id, String type, JsonObject props) {
        this.posInClause = posInClause;
        this.id = id;
        this.type = type;
        this.props = props;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public JsonObject getProps() {
        return props;
    }

    @Override
    public String toString() {
        return "(ID:" + this.id + ",TYPE:" + this.type + ",PROPS:"
                + this.props + ",POS:" + this.posInClause + ")";
    }

    public int getPosInClause() {
        return posInClause;
    }

    private void createAlias() {
        alias[0] = this.type;
        alias[1] = GenerateAlias.gen();
    }

    public String[] getAlias() {
        if (alias[0] == null) {
            createAlias();
        }
        return this.alias;
    }
}
