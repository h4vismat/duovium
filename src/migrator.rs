//! The kernel's migrations, and the table that records them.

use std::sync::LazyLock;

use sqlx::migrate::Migrator;

/// The kernel owns the `events` table, so it ships the migration that creates
/// it, and the application runs this migrator alongside its own.
///
/// `dangerous_set_table_name` earns its name when it repoints a history that
/// already exists, which strands every row in the old table. Here the table is
/// created by this migrator and read by nothing else, so the application keeps
/// `_sqlx_migrations` to itself and neither migrator sees the other's rows.
/// `set_ignore_missing` stays off: each migrator validating its own list is the
/// check worth keeping.
pub static MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.dangerous_set_table_name("_sqlx_migrations_cqrs");
    migrator
});

#[cfg(all(test, feature = "postgres-tests"))]
mod tests {
    use sqlx::PgPool;

    /// The application runs its own migrations in the same database, so the
    /// kernel's history must not land in the table the application will use.
    /// Plain `sqlx::query_scalar` on purpose: a macro here would add an entry
    /// to the committed `.sqlx` cache for a test-only query.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn the_kernel_records_its_migrations_in_its_own_table(pool: PgPool) {
        let kernel: bool =
            sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations_cqrs') IS NOT NULL")
                .fetch_one(&pool)
                .await
                .expect("the query runs");
        let application: bool =
            sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
                .fetch_one(&pool)
                .await
                .expect("the query runs");

        assert!(kernel, "the kernel's migrations table exists");
        assert!(!application, "the application's table is untouched");
    }

    /// The migration the crate ships is the one that creates `events`.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn the_events_table_is_created(pool: PgPool) {
        let events: bool = sqlx::query_scalar("SELECT to_regclass('events') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("the query runs");

        assert!(events, "the events table exists");
    }

    /// `#[sqlx::test]` already ran `MIGRATOR` once to build this pool's
    /// database. A future `core-api` migrator would leave its own history in
    /// a plain `_sqlx_migrations` table in the same database, so this
    /// pre-creates that table with a row in it and runs `MIGRATOR` a second
    /// time, proving the rename is what lets the two histories share a
    /// database without either rejecting the other's rows.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_foreign_migrations_table_survives_a_second_run(pool: PgPool) {
        sqlx::query(
            "CREATE TABLE _sqlx_migrations (
                version BIGINT PRIMARY KEY,
                description TEXT NOT NULL,
                installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
                success BOOLEAN NOT NULL,
                checksum BYTEA NOT NULL,
                execution_time BIGINT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .expect("the foreign history table is created");

        sqlx::query(
            "INSERT INTO _sqlx_migrations
                 (version, description, success, checksum, execution_time)
             VALUES (1, 'core-api placeholder', true, '\\x00', 0)",
        )
        .execute(&pool)
        .await
        .expect("the foreign row is inserted");

        super::MIGRATOR
            .run(&pool)
            .await
            .expect("MIGRATOR re-runs cleanly beside a foreign history");

        let kernel_table: bool =
            sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations_cqrs') IS NOT NULL")
                .fetch_one(&pool)
                .await
                .expect("the query runs");
        let foreign_table: bool =
            sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
                .fetch_one(&pool)
                .await
                .expect("the query runs");
        // `MAX` and a count, not a bare `SELECT version`: the kernel now ships
        // more than one migration, so `fetch_one` over the plain column takes
        // whichever row the heap hands back first and would not notice the
        // later migration going missing.
        let kernel_version: i64 =
            sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations_cqrs")
                .fetch_one(&pool)
                .await
                .expect("the kernel's migration is recorded");
        let kernel_migrations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations_cqrs")
                .fetch_one(&pool)
                .await
                .expect("the query runs");
        let foreign_description: String =
            sqlx::query_scalar("SELECT description FROM _sqlx_migrations WHERE version = 1")
                .fetch_one(&pool)
                .await
                .expect("the foreign row is still there, untouched");

        assert!(kernel_table, "the kernel's migrations table exists");
        assert!(foreign_table, "the foreign migrations table exists");
        assert_eq!(kernel_version, 20260908000001);
        assert_eq!(kernel_migrations, 3, "all kernel migrations are recorded");
        assert_eq!(foreign_description, "core-api placeholder");
    }

    /// Two rows written by one transaction share its id; a later transaction
    /// gets a higher one. This is what makes `xact_id` an ordering key.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn xact_id_identifies_the_writing_transaction(pool: PgPool) {
        let mut first = pool.begin().await.expect("a transaction begins");
        insert_bare_event(&mut first, "a", 1).await;
        insert_bare_event(&mut first, "a", 2).await;
        first.commit().await.expect("the first transaction commits");

        let mut second = pool.begin().await.expect("a transaction begins");
        insert_bare_event(&mut second, "b", 1).await;
        second
            .commit()
            .await
            .expect("the second transaction commits");

        let distinct: i64 = sqlx::query_scalar("SELECT COUNT(DISTINCT xact_id) FROM events")
            .fetch_one(&pool)
            .await
            .expect("the query runs");
        let shared: i64 =
            sqlx::query_scalar("SELECT COUNT(DISTINCT xact_id) FROM events WHERE stream_id = 'a'")
                .fetch_one(&pool)
                .await
                .expect("the query runs");

        assert_eq!(distinct, 2, "two transactions wrote two ids");
        assert_eq!(shared, 1, "one transaction wrote one id");
    }

    /// A fresh checkpoint row starts before every real transaction id, so a
    /// new projection reads the table from the beginning.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_fresh_checkpoint_starts_at_the_beginning(pool: PgPool) {
        sqlx::query("INSERT INTO projection_checkpoints (projection) VALUES ('totals')")
            .execute(&pool)
            .await
            .expect("the row is inserted");

        let row = sqlx::query_as::<_, (String, i64, i32, Option<chrono::DateTime<chrono::Utc>>)>(
            "SELECT cursor_xact::text, cursor_seq, failures, halted_at
               FROM projection_checkpoints WHERE projection = 'totals'",
        )
        .fetch_one(&pool)
        .await
        .expect("the row reads back");

        assert_eq!(row.0, "0", "the cursor starts at transaction zero");
        assert_eq!(row.1, 0, "the cursor starts at sequence zero");
        assert_eq!(row.2, 0, "no failures yet");
        assert!(row.3.is_none(), "not halted");
    }

    /// One row per projection name, so two runners of the same projection
    /// contend for one cursor instead of keeping two.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_projection_has_one_checkpoint_row(pool: PgPool) {
        sqlx::query("INSERT INTO projection_checkpoints (projection) VALUES ('totals')")
            .execute(&pool)
            .await
            .expect("the first row is inserted");

        let clash =
            sqlx::query("INSERT INTO projection_checkpoints (projection) VALUES ('totals')")
                .execute(&pool)
                .await;

        let error = clash.expect_err("the second row collides");
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("23505")
        );
    }

    /// Inserts one row with the columns the schema requires and nothing more,
    /// so `xact_id` comes from its default rather than the caller.
    async fn insert_bare_event(connection: &mut sqlx::PgConnection, stream_id: &str, version: i64) {
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, 'test', $2, $3, 'test.added', 1, '{}'::jsonb, now())",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(stream_id)
        .bind(version)
        .execute(connection)
        .await
        .expect("the row is inserted");
    }
}
