package clauseObjects;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores the return clause.
 */
public class ReturnClause {
    private List<CypReturn> items;
    private Map<String, String> alias = new HashMap<>();

    public List<CypReturn> getItems() {
        return items;
    }

    public void setItems(List<CypReturn> items) {
        this.items = items;
    }
}
