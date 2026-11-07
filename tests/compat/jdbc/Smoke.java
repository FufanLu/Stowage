// JDBC smoke test against Stowage's Postgres wire protocol.
//   java -cp postgresql-42.x.jar tests/compat/jdbc/Smoke.java jdbc:postgresql://127.0.0.1:5481/stowage
import java.sql.*;

public class Smoke {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "jdbc:postgresql://127.0.0.1:5481/stowage";
        try (Connection c = DriverManager.getConnection(url, "stowage", "")) {
            System.out.println("connected: " + c.getMetaData().getDatabaseProductName() + " " + c.getMetaData().getDatabaseProductVersion());
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS jdbc_accounts");
                s.execute("CREATE TABLE jdbc_accounts (id BIGINT PRIMARY KEY, owner TEXT NOT NULL, balance DOUBLE PRECISION, opened TIMESTAMP)");
            }
            // Extended protocol: a prepared INSERT with typed parameters.
            try (PreparedStatement p = c.prepareStatement("INSERT INTO jdbc_accounts VALUES (?, ?, ?, ?)")) {
                for (int i = 1; i <= 5; i++) {
                    p.setLong(1, i);
                    p.setString(2, "owner" + i);
                    p.setDouble(3, i * 10.5);
                    p.setTimestamp(4, Timestamp.valueOf("2026-11-0" + i + " 09:00:00"));
                    p.executeUpdate();
                }
            }
            // A transaction: move money, commit.
            c.setAutoCommit(false);
            try (PreparedStatement p = c.prepareStatement("UPDATE jdbc_accounts SET balance = balance + ? WHERE id = ?")) {
                p.setDouble(1, -5); p.setLong(2, 1); p.executeUpdate();
                p.setDouble(1, 5); p.setLong(2, 2); p.executeUpdate();
            }
            c.commit();
            c.setAutoCommit(true);
            try (PreparedStatement p = c.prepareStatement("SELECT id, owner, balance, opened FROM jdbc_accounts WHERE balance > ? ORDER BY id")) {
                p.setDouble(1, 20);
                try (ResultSet r = p.executeQuery()) {
                    while (r.next()) {
                        System.out.printf("%d %s %.2f %s%n", r.getLong(1), r.getString(2), r.getDouble(3), r.getTimestamp(4));
                    }
                }
            }
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT sum(balance) FROM jdbc_accounts")) {
                r.next();
                System.out.println("total " + r.getDouble(1));
            }
            // A constraint error carries the Postgres SQLSTATE.
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO jdbc_accounts VALUES (1, 'dup', 0, NULL)");
            } catch (SQLException e) {
                System.out.println("expected error " + e.getSQLState() + ": " + e.getMessage());
            }
        }
    }
}
