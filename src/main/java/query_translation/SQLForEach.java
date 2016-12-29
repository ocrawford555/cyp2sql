package query_translation;

import clauseObjects.CypForEach;

public class SQLForEach {
    public static String genQuery(String withTemp, CypForEach cypForEach) {
        int posOfSelect = withTemp.lastIndexOf("SELECT");
        int posOfFrom = withTemp.lastIndexOf("FROM");

        String newSelect = "doForEachFunc(array_agg(n.id), '" +
                cypForEach.getUpdateMap().keySet().iterator().next() +
                "', '" +
                cypForEach.getUpdateMap().values().iterator().next() +
                "')";

        return withTemp.substring(0, posOfSelect + 7) + newSelect + " " +
                withTemp.substring(posOfFrom, withTemp.length());
    }
}
