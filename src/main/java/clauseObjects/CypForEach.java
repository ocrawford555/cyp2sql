package clauseObjects;

import java.util.HashMap;
import java.util.Map;

public class CypForEach {
    private Map<String, String> updateMap;

    public CypForEach(String forEachClause) {
        String part = forEachClause.split(" \\| ")[1];
        String[] kv = part.split(" = ");
        kv[0] = kv[0].split("\\.")[1];
        kv[1] = kv[1].substring(1, kv[1].length() - 3);
        Map<String, String> upMap = new HashMap<>();
        upMap.put(kv[0], kv[1]);
        updateMap = upMap;
    }

    public Map<String, String> getUpdateMap() {
        return updateMap;
    }
}
