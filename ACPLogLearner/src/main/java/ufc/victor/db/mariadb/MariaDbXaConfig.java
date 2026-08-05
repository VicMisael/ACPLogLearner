package ufc.victor.db.mariadb;

public record MariaDbXaConfig(
        String jdbcUrl,
        String user,
        String password
) {
    public MariaDbXaConfig {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl must not be blank");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalArgumentException("user must not be blank");
        }
        if (password == null) {
            throw new IllegalArgumentException("password must not be null");
        }
    }
}
