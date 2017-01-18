package database;

import java.sql.SQLException;

public class AddTClosure {
    public static void addTClosure(String dbName) {
        System.out.println("Starting building of tClousre.");
        String createTClosure = "CREATE MATERIALIZED VIEW tclosure AS(WITH RECURSIVE search_graph(idl, idr, depth, " +
                "path, cycle) " +
                "AS (SELECT e.idl, e.idr, 1, ARRAY[e.idl], false FROM edges e UNION ALL SELECT sg.idl, e.idr, " +
                "sg.depth + 1, path || e.idl, e.idl = ANY(sg.path) FROM edges e, search_graph sg WHERE e.idl = " +
                "sg.idr AND NOT cycle) SELECT * FROM search_graph where (not cycle OR not idr = ANY(path)));";
        try {
            DbUtil.createConnection(dbName);
            DbUtil.createInsert(createTClosure);
            DbUtil.closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Finished tClousre.");
    }

}
