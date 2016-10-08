package toolv1;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import parsing_lexing.CypherLexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

class CypherTokenizer {
    static String decode(String cyp) throws Exception {
        // for ease of working with
        cyp = cyp.toUpperCase();
        CypherLexer lexer = new CypherLexer(new ANTLRInputStream(cyp));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        ArrayList<String> tokenList = new ArrayList<String>();
        ArrayList<String> ruleList = new ArrayList<String>();

        Map<String, Integer> m = lexer.getRuleIndexMap();
        Map<Integer, String> ruleIndexes = new HashMap<Integer, String>();
        for (Map.Entry<String, Integer> entry : m.entrySet()) {
            ruleIndexes.put(entry.getValue(), entry.getKey());
        }

        for (Object tree : tokens.getTokens()) {
            CommonToken t = (CommonToken) tree;
            String s = t.getText();

            if (!" ".equals(s) && !"<EOF>".equals(s)) {
                tokenList.add(s);
                int y = t.getType() - 1;
                ruleList.add(ruleIndexes.get(y));
            }
        }

        System.out.println(ruleList.toString());

        StringBuilder sql = new StringBuilder();

        // find out what the query is (RETURNX is RETURN rule but with alias)
        if (ruleList.contains("MATCH") && ruleList.contains("RETURNX") && ruleList.contains("ORDER")
                && (ruleList.contains("L_SKIP") || ruleList.contains("LIMIT"))) {
            return CypherTranslator.MatchAndReturnAndOrderAndSkip(sql, tokenList);
        }
        if (ruleList.contains("MATCH") && ruleList.contains("RETURNX") && ruleList.contains("ORDER")) {
            return CypherTranslator.MatchAndReturnAndOrder(sql, tokenList);
        } else if (ruleList.contains("MATCH") && ruleList.contains("RETURNX")) {
            return CypherTranslator.MatchAndReturn(sql, tokenList);
        }

        return sql.toString();
    }
}
