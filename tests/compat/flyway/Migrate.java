// Applies a directory of Flyway migrations to Stowage over JDBC, then runs
// Flyway again to check that the history table reads back (nothing to apply).
//
//   java -cp "<jars>:." Migrate jdbc:postgresql://127.0.0.1:5481/stowage <dir>
import org.flywaydb.core.Flyway;

public class Migrate {
  public static void main(String[] a) {
    Flyway f = Flyway.configure().dataSource(a[0], "stowage", "").locations("filesystem:" + a[1]).load();
    var first = f.migrate();
    var second = f.migrate();
    for (var i : f.info().all()) System.out.println("  V" + i.getVersion() + " " + i.getDescription() + ": " + i.getState());
    if (first.migrationsExecuted == 0 || second.migrationsExecuted != 0) {
      System.err.println("unexpected: first run applied " + first.migrationsExecuted + ", second " + second.migrationsExecuted);
      System.exit(1);
    }
  }
}
