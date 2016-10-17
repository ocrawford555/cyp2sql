package database;

import java.sql.SQLException;

public class SQLDriver {
    public static void run(String query) throws SQLException {
        DbUtil.select(query);
    }
}
