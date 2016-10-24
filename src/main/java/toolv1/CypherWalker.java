package toolv1;

import parsing_lexing.CypherBaseListener;
import parsing_lexing.CypherParser;

public class CypherWalker extends CypherBaseListener {
    private boolean hasOptional = false;
    private boolean hasDistinct = false;
    private boolean hasCount = false;
    private String matchClause = null;
    private String whereClause = null;
    private String returnClause = null;
    private String orderClause = null;
    private String withClause = null;
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
        if (ctx.getText().toLowerCase().startsWith("optional "))
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
        if (ctx.getText().toLowerCase().contains(" distinct "))
            hasDistinct = true;
        // is the return query looking at a count
        if (ctx.getText().toLowerCase().contains("count"))
            hasCount = true;
    }

    public void enterReturnItems(CypherParser.ReturnItemsContext ctx) {
        System.out.println(ctx.getParent().getRuleIndex());
        returnClause = ctx.getText();
    }

    public void enterSortItem(CypherParser.SortItemContext ctx) {
        String orderByString = ctx.getText().toLowerCase();
        if (orderByString.endsWith("descending") || orderByString.endsWith("desc")) {
            latestOrderDirection = "desc";
        } else if (orderByString.endsWith("ascending") || orderByString.endsWith("asc")) {
            latestOrderDirection = "asc";
        } else latestOrderDirection = "";
    }

    public void printInformation() {
        System.out.println("\n--- QUERY INFORMATION ---");
        if (matchClause != null) System.out.println("Match Clause: " + matchClause + " -- OPTIONAL = " + hasOptional);
        if (whereClause != null) System.out.println("Where Clause: " + whereClause);
        if (withClause != null) System.out.println("With Clause: " + withClause);
        if (returnClause != null)
            System.out.println("Return Clause: " + returnClause + " -- DISTINCT = " + hasDistinct);
        if (orderClause != null) System.out.println("Order Clause: " + orderClause);
        if (skipAmount != -1) System.out.println("Skip Amount: " + skipAmount);
        if (limitAmount != -1) System.out.println("Limit Amount: " + limitAmount);
        System.out.println("\n");
    }

    public String getReturnClause() {
        return returnClause;
    }

    public int getTypeQuery() {
        return typeQuery;
    }

    public int getSkipAmount() {
        return skipAmount;
    }

    public int getLimitAmount() {
        return limitAmount;
    }

    public boolean doesCluaseHaveWhere() {
        return (whereClause != null);
    }

    public String getWhereClause() {
        return whereClause;
    }

    public boolean hasDistinct() {
        return hasDistinct;
    }

    public boolean hasCount() {
        return hasCount;
    }

    public String getWithClause() {
        return withClause;
    }
}
