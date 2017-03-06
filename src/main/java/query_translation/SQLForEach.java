package query_translation;

import clauseObjects.CypForEach;

public class SQLForEach {
    public static String genQuery(String withTemp, CypForEach cfe) {
        int posOfSelect = withTemp.lastIndexOf("SELECT");
        int posOfFrom = withTemp.lastIndexOf("FROM");

        String newSelect = "doForEachFunc(array_agg(n01.id), '" +
                cfe.getUpdateMap().keySet().iterator().next() +
                "', '" +
                cfe.getUpdateMap().values().iterator().next() +
                "')";

        return withTemp.substring(0, posOfSelect + 7) + newSelect + " " +
                withTemp.substring(posOfFrom, withTemp.length());
    }
}
