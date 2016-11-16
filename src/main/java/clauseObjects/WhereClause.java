package clauseObjects;

import java.util.ArrayList;

/**
 * Stores the where clause.
 */
public class WhereClause {
    private boolean hasOr = false;
    private ArrayList<String[]> orClauses = new ArrayList<>();
    private String clause;

    public WhereClause(String c) {
        this.clause = c;
    }

    // OR functionality and other boolean operations in the where clause not tested yet.
    public boolean isHasOr() {
        return hasOr;
    }

    public void setHasOr(boolean hasOr) {
        this.hasOr = hasOr;
    }

    public void addToOr(String[] orClause) {
        orClauses.add(orClause);
    }

    public ArrayList<String[]> getOrClauses() {
        return this.orClauses;
    }

    public String getClause() {
        return clause;
    }

    public void setClause(String clause) {
        this.clause = clause;
    }
}
