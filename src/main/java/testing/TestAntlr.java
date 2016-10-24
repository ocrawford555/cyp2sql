package testing;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import parsing_lexing.CypherLexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestAntlr {
    public static void main(String args[]) throws Exception {
        String query = "MATCH (t:Match) RETURN t.h_score LIMIT 10";
        // for ease of working with
        query = query.toLowerCase();
        CypherLexer lexer = new CypherLexer(new ANTLRInputStream(query));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        ArrayList<String> tokenList = new ArrayList<String>();

        Map<String, Integer> m = lexer.getRuleIndexMap();
        Map<Integer, String> ruleIndexes = new HashMap<Integer, String>();
        for (Map.Entry<String, Integer> entry : m.entrySet()) {
            ruleIndexes.put(entry.getValue(), entry.getKey());
        }

        int matchIndex = -1;
        int retrunIndex = -1;
        int limitIndex = -1;
        int i = 0;

        for (Object tree : tokens.getTokens()) {
            CommonToken t = (CommonToken) tree;
            String s = t.getText();

            if (!" ".equals(s) && !"<EOF>".equals(s)) {
                tokenList.add(s);
                int y = t.getType() - 1;
                String rule = ruleIndexes.get(y);
                System.out.println(t.getText() + " - " + rule);
                if(rule.equals("MATCH") && matchIndex == -1) matchIndex=i;
                if(rule.equals("RETURNX") && retrunIndex == -1) retrunIndex=i;
                if(rule.equals("LIMIT") && limitIndex == -1) limitIndex=i;
            }

            i++;
        }
        System.out.println(matchIndex + ", " + retrunIndex + ", " + limitIndex);
        System.out.println(tokenList.toString());
        for (String s1 : tokenList) {
            if (s1.equals("MATCH")) {
                //doMatch(tokenList, tokenList.indexOf(s1) + 1);
            }
        }
    }

    private static void doMatch(ArrayList<String> toks, int startIndex) {
        int x = startIndex;
        if (!toks.get(x).equals("("))
            return;

//        x++;
//        String s = toks.get(x);
//        while(!s.equals(")")){
//            String label = "";
//            if(s.equals(":"))
//        }
    }
}
