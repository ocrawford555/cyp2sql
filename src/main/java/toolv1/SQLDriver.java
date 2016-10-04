package toolv1;

import java.sql.SQLException;

class SQLDriver {
    static void run(String query) throws SQLException {
        DbUtil.select(query);
    }
}
