package query_translation;

import clauseObjects.CypIterate;
import clauseObjects.CypNode;
import clauseObjects.DecodedQuery;
import com.google.gson.JsonObject;
import production.Reagan_Main_V4;

public class SQLIterate {
    public static String translate(CypIterate cypIter, String typeTranslate) {
        String line = cypIter.getOriginalCypherInput();
        String matchClause = line.substring(8, line.indexOf("loop"));

        line = line.split(" loop ")[1];
        cypIter.setLoopIndexTo(line.split(" ")[0]);
        line = line.split(" on ")[1];
        cypIter.setLoopIndexFrom(line.split(" ")[0]);
        line = line.split(" collect ")[1];
        cypIter.setCollectIndex(line.split(" ")[0]);
        line = line.split(" return ")[1];
        cypIter.setReturnStatement(line);

        String firstStep = matchClause + "return " + cypIter.getLoopIndexTo() + ";";
        cypIter.setFirstQuery(firstStep);

        // get correct loop query
        int posOfLoopFrom = firstStep.indexOf(cypIter.getLoopIndexFrom());
        String lQuery = "MATCH " + firstStep.substring(posOfLoopFrom - 1, firstStep.length());
        cypIter.setLoopQuery(lQuery);

        // generate the traditional translation to SQL for the loop query (store in string as used
        // multiple times)
        DecodedQuery loopDQ = Reagan_Main_V4.convertCypherToSQL(cypIter.getFirstQuery(), typeTranslate);
        String loopSQL = loopDQ.getSqlEquiv();

        String returnSQL = Reagan_Main_V4.convertCypherToSQL(cypIter.getReturnStatement(), typeTranslate).getSqlEquiv();
        returnSQL = returnSQL.substring(0, returnSQL.length() - 1);

        // need to modify loopSQL for the main SQL statement.
        String[] mainParts = loopSQL.split("SELECT n01\\.\\*");
        String mainInitStmt = mainParts[0].trim() + ", firstStep AS (SELECT (array_agg(n01.id)) AS list_ids ";
        mainInitStmt = mainInitStmt + mainParts[1].substring(0, mainParts[1].length() - 1) + "),  ";
        mainInitStmt = mainInitStmt + "collectStep AS (SELECT unnest((cypher_iterate(firstStep.list_ids))) " +
                "AS zz from firstStep) ";
        mainInitStmt = mainInitStmt + returnSQL + " INNER JOIN collectStep c ON n01.id = c.zz;";

        // create the loop_work function with this string
        String loopWorkStr;

        loopDQ = Reagan_Main_V4.convertCypherToSQL(cypIter.getLoopQuery(), typeTranslate);
        int posLoopFrom = calculatePos(cypIter.getLoopIndexFrom(), loopDQ);
        JsonObject obj = loopDQ.getMc().getNodes().get(posLoopFrom - 1).getProps();
        if (obj == null) {
            obj = new JsonObject();
        }
        obj.addProperty("id", "ANY($1)");
        loopDQ.getMc().getNodes().get(posLoopFrom - 1).setProps(obj);

        try {
            loopDQ.setSqlEquiv(SQLTranslate.translateRead(loopDQ, typeTranslate));
        } catch (Exception e) {
            e.printStackTrace();
        }

        mainParts = loopDQ.getSqlEquiv().split("SELECT n01\\.\\*");
        loopWorkStr = mainParts[0];
        loopWorkStr += " SELECT array_agg(n01.id)";
        loopWorkStr += mainParts[1].substring(0, mainParts[1].length() - 1);

        String functionLoop = "CREATE OR REPLACE FUNCTION loop_work(int[]) RETURNS int[] AS $$ " + loopWorkStr +
                " $$ LANGUAGE SQL;";

        cypIter.setSQL(functionLoop + " " + mainInitStmt);

        // the iterate function should always be persistent on the database and shouldn't need modification.
        return cypIter.getSQL();
    }

    private static int calculatePos(String loopIndexFrom, DecodedQuery loopDQ) {
        for (CypNode cN : loopDQ.getMc().getNodes()) {
            if (cN.getId().equals(loopIndexFrom)) return cN.getPosInClause();
        }
        return -1;
    }
}
