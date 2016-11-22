package toolv1;

import clauseObjects.DecodedQuery;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import parsing_lexing.CypherLexer;
import parsing_lexing.CypherParser;

import java.util.ArrayList;

public class CypherTokenizer {
    private static CypherWalker cypherWalker;

    public static DecodedQuery decode(String cyp, boolean DEBUG_PRINT) throws Exception {
        return CypherTranslator.generateDecodedQuery(getTokenList(cyp, DEBUG_PRINT), cypherWalker);
    }

    public static ArrayList<String> getTokenList(String cyp, boolean DEBUG_PRINT) {
        CypherLexer lexer = new CypherLexer(new ANTLRInputStream(cyp));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        CypherParser parser = new CypherParser(tokens);
        ParseTree tree = parser.cypher();
        ParseTreeWalker walker = new ParseTreeWalker();

        cypherWalker = null;
        cypherWalker = new CypherWalker();
        walker.walk(cypherWalker, tree);

        if (DEBUG_PRINT) cypherWalker.printInformation();

        ArrayList<String> tokenList = new ArrayList<>();

        for (Object t : tokens.getTokens()) {
            CommonToken tok = (CommonToken) t;
            String s = tok.getText().toLowerCase();

            if (!" ".equals(s) && !"<eof>".equals(s) && !";".equals(s) && !"as".equals(s))
                tokenList.add(s);
        }

        System.out.println(tokenList);

        return tokenList;
    }
}
