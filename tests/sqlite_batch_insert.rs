use crate::{connection, TestConnection};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};

diesel::table! {
    batch_users {
        id -> Integer,
        name -> Text,
        hair_color -> Nullable<Text>,
    }
}

#[derive(Insertable)]
#[diesel(table_name = batch_users)]
struct NewUser<'a> {
    name: &'a str,
    // `Option` fields are inserted as `DEFAULT` if they are `None`,
    // which is not supported by SQLite in a single statement
    hair_color: Option<&'a str>,
}

#[derive(Insertable)]
#[diesel(table_name = batch_users)]
struct NewUserWithId<'a> {
    id: i32,
    name: &'a str,
    hair_color: Option<&'a str>,
}

async fn setup(conn: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TEMPORARY TABLE batch_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            hair_color TEXT DEFAULT 'green'
        )",
    )
    .execute(conn)
    .await
    .unwrap();
}

async fn load_users(conn: &mut TestConnection) -> Vec<(String, Option<String>)> {
    batch_users::table
        .select((batch_users::name, batch_users::hair_color))
        .order(batch_users::id)
        .load(conn)
        .await
        .unwrap()
}

fn owned(users: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
    users
        .iter()
        .map(|(name, hair_color)| (name.to_string(), hair_color.map(str::to_string)))
        .collect()
}

#[tokio::test]
async fn batch_insert_without_default_values() {
    let conn = &mut connection().await;
    setup(conn).await;

    let count = diesel::insert_into(batch_users::table)
        .values(&vec![
            (
                batch_users::name.eq("Sean"),
                batch_users::hair_color.eq("black"),
            ),
            (
                batch_users::name.eq("Tess"),
                batch_users::hair_color.eq("brown"),
            ),
        ])
        .execute(conn)
        .await
        .unwrap();

    assert_eq!(count, 2);
    assert_eq!(
        load_users(conn).await,
        owned(&[("Sean", Some("black")), ("Tess", Some("brown"))])
    );
}

#[tokio::test]
async fn batch_insert_with_default_values() {
    let conn = &mut connection().await;
    setup(conn).await;

    let count = diesel::insert_into(batch_users::table)
        .values(&vec![
            NewUser {
                name: "Sean",
                hair_color: Some("black"),
            },
            NewUser {
                name: "Tess",
                hair_color: None,
            },
        ])
        .execute(conn)
        .await
        .unwrap();

    assert_eq!(count, 2);
    assert_eq!(
        load_users(conn).await,
        owned(&[("Sean", Some("black")), ("Tess", Some("green"))])
    );
}

#[tokio::test]
async fn batch_insert_with_default_values_is_atomic() {
    let conn = &mut connection().await;
    setup(conn).await;

    let result = diesel::insert_into(batch_users::table)
        .values(&vec![
            NewUserWithId {
                id: 1,
                name: "Sean",
                hair_color: None,
            },
            NewUserWithId {
                id: 1,
                name: "Tess",
                hair_color: None,
            },
        ])
        .execute(conn)
        .await;

    assert!(result.is_err());
    assert_eq!(load_users(conn).await, owned(&[]));

    // the connection is still usable afterwards
    let count = diesel::insert_into(batch_users::table)
        .values(&vec![NewUser {
            name: "Sean",
            hair_color: None,
        }])
        .execute(conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn batch_insert_with_default_values_in_transaction() {
    let conn = &mut connection().await;
    setup(conn).await;

    let result = conn
        .transaction::<(), diesel::result::Error, _>(async |conn| {
            let count = diesel::insert_into(batch_users::table)
                .values(&vec![
                    NewUser {
                        name: "Sean",
                        hair_color: None,
                    },
                    NewUser {
                        name: "Tess",
                        hair_color: None,
                    },
                ])
                .execute(conn)
                .await?;
            assert_eq!(count, 2);
            Err(diesel::result::Error::RollbackTransaction)
        })
        .await;

    assert_eq!(result, Err(diesel::result::Error::RollbackTransaction));
    assert_eq!(load_users(conn).await, owned(&[]));
}

#[tokio::test]
async fn empty_batch_insert() {
    let conn = &mut connection().await;
    setup(conn).await;

    let count = diesel::insert_into(batch_users::table)
        .values(Vec::<NewUser>::new())
        .execute(conn)
        .await
        .unwrap();
    assert_eq!(count, 0);

    let count = diesel::insert_into(batch_users::table)
        .values(Vec::<(_, _)>::from([] as [(
            diesel::dsl::Eq<batch_users::name, &str>,
            diesel::dsl::Eq<batch_users::hair_color, &str>,
        ); 0]))
        .execute(conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn batch_insert_on_conflict_do_nothing() {
    let conn = &mut connection().await;
    setup(conn).await;

    let count = diesel::insert_into(batch_users::table)
        .values(&vec![
            (batch_users::id.eq(1), batch_users::name.eq("Sean")),
            (batch_users::id.eq(1), batch_users::name.eq("Tess")),
        ])
        .on_conflict_do_nothing()
        .execute(conn)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let count = diesel::insert_into(batch_users::table)
        .values(&vec![
            NewUserWithId {
                id: 1,
                name: "Jim",
                hair_color: None,
            },
            NewUserWithId {
                id: 2,
                name: "Tess",
                hair_color: None,
            },
        ])
        .on_conflict_do_nothing()
        .execute(conn)
        .await
        .unwrap();
    assert_eq!(count, 1);

    assert_eq!(
        load_users(conn).await,
        owned(&[("Sean", Some("green")), ("Tess", Some("green"))])
    );
}

// RETURNING requires diesel's `returning_clauses_for_sqlite_3_35` feature,
// which is enabled by the `__sqlite-returning-tests` feature
#[cfg(feature = "__sqlite-returning-tests")]
mod returning {
    use super::*;

    #[derive(Queryable, Selectable, Debug, PartialEq)]
    #[diesel(table_name = batch_users)]
    struct User {
        id: i32,
        name: String,
        hair_color: Option<String>,
    }

    #[tokio::test]
    async fn batch_insert_without_default_values_returning() {
        let conn = &mut connection().await;
        setup(conn).await;

        let users = diesel::insert_into(batch_users::table)
            .values(&vec![
                (
                    batch_users::name.eq("Sean"),
                    batch_users::hair_color.eq("black"),
                ),
                (
                    batch_users::name.eq("Tess"),
                    batch_users::hair_color.eq("brown"),
                ),
            ])
            .returning(User::as_returning())
            .get_results(conn)
            .await
            .unwrap();

        assert_eq!(
            users,
            vec![
                User {
                    id: 1,
                    name: "Sean".into(),
                    hair_color: Some("black".into()),
                },
                User {
                    id: 2,
                    name: "Tess".into(),
                    hair_color: Some("brown".into()),
                },
            ]
        );
    }

    #[tokio::test]
    async fn batch_insert_with_default_values_returning() {
        let conn = &mut connection().await;
        setup(conn).await;

        let users: Vec<User> = diesel::insert_into(batch_users::table)
            .values(&vec![
                NewUser {
                    name: "Sean",
                    hair_color: Some("black"),
                },
                NewUser {
                    name: "Tess",
                    hair_color: None,
                },
            ])
            .get_results(conn)
            .await
            .unwrap();

        assert_eq!(
            users,
            vec![
                User {
                    id: 1,
                    name: "Sean".into(),
                    hair_color: Some("black".into()),
                },
                User {
                    id: 2,
                    name: "Tess".into(),
                    hair_color: Some("green".into()),
                },
            ]
        );

        let names = diesel::insert_into(batch_users::table)
            .values(&vec![
                NewUser {
                    name: "Jim",
                    hair_color: None,
                },
                NewUser {
                    name: "Ruby",
                    hair_color: None,
                },
            ])
            .returning(batch_users::name)
            .get_results::<String>(conn)
            .await
            .unwrap();
        assert_eq!(names, vec!["Jim", "Ruby"]);
    }

    #[tokio::test]
    async fn batch_insert_with_default_values_on_conflict_returning() {
        let conn = &mut connection().await;
        setup(conn).await;

        let names = diesel::insert_into(batch_users::table)
            .values(&vec![
                NewUserWithId {
                    id: 1,
                    name: "Sean",
                    hair_color: None,
                },
                NewUserWithId {
                    id: 1,
                    name: "Tess",
                    hair_color: None,
                },
                NewUserWithId {
                    id: 2,
                    name: "Jim",
                    hair_color: None,
                },
            ])
            .on_conflict_do_nothing()
            .returning(batch_users::name)
            .get_results::<String>(conn)
            .await
            .unwrap();
        // rows skipped due to the conflict do not return anything
        assert_eq!(names, vec!["Sean", "Jim"]);
    }
}
