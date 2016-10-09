package testing;

class CypherWalker extends CypherBaseListener {
    public void enterCypher(CypherParser.CypherContext ctx) {
        System.out.println("Entering Cypher :");
    }

    public void exitCypher(CypherParser.CypherContext ctx) {
        System.out.println("Exiting Cypher...");
    }

    public void enterClause(CypherParser.ClauseContext ctx) {
        System.out.println("Statement : " + ctx.getText());
    }

    public void enterMatch(CypherParser.MatchContext ctx) {
        String matchClause = ctx.getText();
        System.out.println("Match Clause : " + matchClause);
        //optional keyword attached or not
        if (matchClause.toUpperCase().startsWith("OPTIONAL "))
            System.out.println("Match Clause has optional keyword.");
    }

    public void enterPattern(CypherParser.PatternContext ctx) {
        System.out.println("Pattern Clause : " + ctx.getText());
    }

    public void enterWhere(CypherParser.WhereContext ctx) {
        System.out.println("Where Clause : " + ctx.getText());
    }

    public void enterExpression(CypherParser.ExpressionContext ctx) {
        System.out.println("Expression : " + ctx.getText());
    }

    public void enterReturnMain(CypherParser.ReturnMainContext ctx) {
        String returnClause = ctx.getText();
        System.out.println("Retrun Clause : " + returnClause);
        //optional keyword attached or not
        if (returnClause.toUpperCase().contains(" DISTINCT "))
            System.out.println("Return Clause has distinct keyword.");
    }
}
