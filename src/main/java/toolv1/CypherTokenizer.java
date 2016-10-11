package toolv1;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import parsing_lexing.CypherLexer;
import parsing_lexing.CypherParser;

import java.util.ArrayList;

class CypherTokenizer {
    static String decode(String cyp) throws Exception {
        CypherLexer lexer = new CypherLexer(new ANTLRInputStream(cyp));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        CypherParser parser = new CypherParser(tokens);
        ParseTree tree = parser.cypher();
        ParseTreeWalker walker = new ParseTreeWalker();

        CypherWalker cypherQ = new CypherWalker();
        walker.walk(cypherQ, tree);
        // cypherQ.printInformation();

        ArrayList<String> tokenList = new ArrayList<String>();

        for (Object t : tokens.getTokens()) {
            CommonToken tok = (CommonToken) t;
            String s = tok.getText().toUpperCase();

            if (!" ".equals(s) && !"<EOF>".equals(s))
                tokenList.add(s);
        }

        StringBuilder sql = new StringBuilder();

        switch (cypherQ.getTypeQuery()) {
            case 1:
                return CypherTranslator.MatchAndReturnAndOrderAndSkip(sql, tokenList, cypherQ);
            case 2:
                return CypherTranslator.MatchAndReturnAndOrderAndSkip(sql, tokenList, cypherQ);
            case 3:
                return CypherTranslator.MatchAndReturnAndOrderAndSkip(sql, tokenList, cypherQ);
        }

        return sql.toString();
    }
}
