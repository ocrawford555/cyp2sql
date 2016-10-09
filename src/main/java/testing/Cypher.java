package testing;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class Cypher {
    public static void main(String[] args) throws Exception {
        ArrayList<String> queries = new ArrayList<String>();
        try (BufferedReader br = new BufferedReader(new FileReader("C:/Users/ocraw/Documents/" +
                "antlrTest2/output for project/exampleQ.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                queries.add(line);
            }
        }

        for (String q : queries) {
            CypherLexer lexer = new CypherLexer(new ANTLRInputStream(q));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            CypherParser parser = new CypherParser(tokens);
            ParseTree tree = parser.cypher();
            ParseTreeWalker walker = new ParseTreeWalker();
            CypherWalker cypherQ = new CypherWalker();
            walker.walk(cypherQ, tree);
            cypherQ.printInformation();
            //printTree(tree, 0);
            System.out.println("\n------\n");
        }
    }

    public static void printTree(ParseTree t, int indent) {
        if (t != null) {
            StringBuffer sb = new StringBuffer(indent);
            for (int i = 0; i < indent; i++)
                sb = sb.append("   ");
            for (int i = 0; i < t.getChildCount(); i++) {
                System.out.println(sb.toString() + t.getChild(i));
                printTree(t.getChild(i), indent + 1);
            }
        }
    }
}
