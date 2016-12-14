package clauseObjects;

import java.util.List;

/**
 * Stores the return clause.
 */
public class ReturnClause {
    private List<CypReturn> items;

    public List<CypReturn> getItems() {
        return items;
    }

    public void setItems(List<CypReturn> items) {
        this.items = items;
    }
}
