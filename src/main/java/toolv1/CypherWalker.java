package toolv1;

import parsing_lexing.CypherBaseListener;
import parsing_lexing.CypherParser;

public class CypherWalker extends CypherBaseListener {
    private boolean hasOptional = false;
    private boolean hasDistinct = false;
    private String matchClause = null;
    private String whereClause = null;
    private String returnClause = null;
    private String orderClause = null;
    private String latestOrderDirection = "";
    private int skipAmount = -1;
    private int limitAmount = -1;
    private int typeQuery;

    public void enterCypher(CypherParser.CypherContext ctx) {
        System.out.println("Entering Cypher : " + ctx.getText());
    }

    public void exitCypher(CypherParser.CypherContext ctx) {
        System.out.println("Computing query...");
        computeQuery();
    }

    private void computeQuery() {
        // find out what the query is
        if (matchClause != null && returnClause != null && orderClause != null
                && (skipAmount != -1 || limitAmount != -1))
            typeQuery = 3;
        else if (matchClause != null && returnClause != null && orderClause != null) {
            typeQuery = 2;
        } else if (matchClause != null && returnClause != null) {
            typeQuery = 1;
        }
    }

    public void enterMatch(CypherParser.MatchContext ctx) {
        //optional keyword attached or not
        if (ctx.getText().toUpperCase().startsWith("OPTIONAL "))
            hasOptional = true;
    }

    public void enterPattern(CypherParser.PatternContext ctx) {
        matchClause = ctx.getText();
    }

    public void enterExpression(CypherParser.ExpressionContext ctx) {
        //System.out.println(ctx.getParent().getRuleIndex());
        switch (ctx.getParent().getRuleIndex()) {
            case 23:
                skipAmount = Integer.parseInt(ctx.getText());
                break;
            case 24:
                limitAmount = Integer.parseInt(ctx.getText());
                break;
            case 25:
                if (orderClause == null) {
                    orderClause = ctx.getText() + " " + latestOrderDirection;
                } else {
                    orderClause += ", " + ctx.getText();
                }
                break;
            case 26:
                whereClause = ctx.getText();
                break;
        }
    }

    public void enterReturnMain(CypherParser.ReturnMainContext ctx) {
        //distinct keyword attached or not
        if (ctx.getText().toUpperCase().contains(" DISTINCT "))
            hasDistinct = true;
    }

    public void enterReturnItems(CypherParser.ReturnItemsContext ctx) {
        returnClause = ctx.getText();
    }

    public void enterSortItem(CypherParser.SortItemContext ctx) {
        String orderByString = ctx.getText().toUpperCase();
        if (orderByString.endsWith("DESCENDING") || orderByString.endsWith("DESC")) {
            latestOrderDirection = "DESC";
        } else if (orderByString.endsWith("ASCENDING") || orderByString.endsWith("ASC")) {
            latestOrderDirection = "ASC";
        } else latestOrderDirection = "";
    }

    public void printInformation() {
        System.out.println("\n--- QUERY INFORMATION ---");
        if (matchClause != null) System.out.println("Match Clause: " + matchClause + " -- OPTIONAL = " + hasOptional);
        if (whereClause != null) System.out.println("Where Clause: " + whereClause);
        if (returnClause != null)
            System.out.println("Return Clause: " + returnClause + " -- DISTINCT = " + hasDistinct);
        if (orderClause != null) System.out.println("Order Clause: " + orderClause);
        if (skipAmount != -1) System.out.println("Skip Amount: " + skipAmount);
        if (limitAmount != -1) System.out.println("Limit Amount: " + limitAmount);
        System.out.println("\n");
    }

    public int getTypeQuery() {
        return typeQuery;
    }
}
