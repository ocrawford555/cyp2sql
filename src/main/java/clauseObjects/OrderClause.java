package clauseObjects;

import java.util.List;

/**
 * Stores the order clause.
 */
public class OrderClause {
    private List<CypOrder> items;

    public List<CypOrder> getItems() {
        return items;
    }

    public void setItems(List<CypOrder> items) {
        this.items = items;
    }
}
