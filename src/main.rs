#[macro_use]
extern crate clap;
#[macro_use]
extern crate hyper_router;
extern crate hyper;
extern crate postgres;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;
extern crate rusqlite;

#[macro_use]
extern crate quick_error;


use clap::Arg;
use hyper::server::{Request, Response, Server};
use hyper::status::StatusCode;
use hyper::uri::RequestUri;
use hyper_router::{Params, RouteHandler};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;

mod database;
use database::{DatabaseConnection, DatabaseConnector, PostgresConnectionExt};



#[derive(Serialize)]
struct RoomRow {
    event_id: String,
    etype: String,
    state_key: Option<String>,
    depth: i64,
    sender: String,
    state_group: Option<i64>,
    json: serde_json::Value,
    ts: i64,
    edges: Vec<String>,
    stream_ordering: i32,
}

#[derive(Serialize)]
struct StateRow {
    etype: String,
    state_key: String,
    event_id: String,
    content: serde_json::Value,
    depth: i64,
}

fn parse_request_uri(req_uri: RequestUri) -> hyper::Url {
    match req_uri {
        RequestUri::AbsolutePath(s) => hyper::Url::parse(&format!("http://foo{}", s)).unwrap(), // ffs
        RequestUri::AbsoluteUri(s) => s,
        _ => panic!("unsupported uri type"),
    }
}

fn write_200_json<T: Serialize>(mut res: Response, val: &T) {
    *res.status_mut() = StatusCode::Ok;
    res.headers_mut().set_raw("Access-Control-Allow-Headers", vec![b"Origin, X-Requested-With, Content-Type, Accept".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Origin", vec![b"*".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Methods", vec![b"GET, POST, PUT, DELETE, OPTIONS".to_vec()]);
    res.headers_mut().set_raw("Content-Type", vec![b"application/json".to_vec()]);

    let mut res = res.start().expect("failed to prepare response for writing");
    serde_json::to_writer(&mut res, val).expect("failed to write json");
    res.end().expect("failed to finish writing response");
}

struct RoomHandler {
    connector: DatabaseConnector,
}

impl RouteHandler for RoomHandler {
    fn handle(&self, params: Params, req: Request, res: Response) {
        let room_id = params.find("room_id").expect("room_id not in params");

        // please tell me there is an easier way to do this
        let uri = parse_request_uri(req.uri);
        let qs: BTreeMap<_, _> = uri.query_pairs().collect();

        let max_stream: i64 = match qs.get("max_stream") {
            Some(x) => x.parse().expect("unable to parse max_stream"),
            _ => i64::max_value(),
        };

        let page_size = 200;

        let mut conn = self.connector.connect();

        let events = match conn {
            DatabaseConnection::Postgres(ref mut pg_conn) => {
                pg_conn.query_rows(
                    r#"
                    SELECT event_id, events.type, state_key, depth, sender, state_group,
                        json, origin_server_ts, stream_ordering,
                        array(
                            SELECT prev_event_id FROM event_edges
                            WHERE is_state = false and event_id = events.event_id
                        )
                    FROM events
                    JOIN event_json USING (event_id)
                    LEFT JOIN state_events USING (event_id)
                    LEFT JOIN event_to_state_groups USING (event_id)
                    WHERE events.room_id = $1 AND stream_ordering <= $2::bigint
                    ORDER BY stream_ordering DESC
                    LIMIT $3::int
                    "#,
                    &[&room_id, &max_stream, &page_size],
                    |row| RoomRow {
                        event_id: row.get(0),
                        etype: row.get(1),
                        state_key: row.get(2),
                        depth: row.get(3),
                        sender: row.get(4),
                        state_group: row.get(5),
                        json: serde_json::from_str(&row.get::<_, String>(6))
                            .expect("json was not json"),
                        ts: row.get(7),
                        stream_ordering: row.get(8),
                        edges: row.get(9),
                    }
                ).expect("room sql query failed")
            }
            DatabaseConnection::Sqlite(_) => {
                let mut events = conn.query(
                    r#"
                    SELECT event_id, events.type, state_key, depth, sender, state_group, json, origin_server_ts,
                        stream_ordering
                    FROM events
                    JOIN event_json USING (event_id)
                    LEFT JOIN state_events USING (event_id)
                    LEFT JOIN event_to_state_groups USING (event_id)
                    WHERE events.room_id = $1 AND stream_ordering <= $2::bigint
                    ORDER BY stream_ordering DESC
                    LIMIT $3::int
                    "#,
                    &[&room_id, &max_stream, &page_size],
                    |row| RoomRow {
                        event_id: row.get(0),
                        etype: row.get(1),
                        state_key: row.get(2),
                        depth: row.get(3),
                        sender: row.get(4),
                        state_group: row.get(5),
                        json: serde_json::from_str(&row.get::<String>(6))
                            .expect("json was not json"),
                        ts: row.get(7),
                        stream_ordering: row.get(8),
                        edges: Vec::new(),
                    }
                ).expect("room sql query failed");

                for event in &mut events {
                    let edges = conn.query(
                        r#"
                        SELECT prev_event_id FROM event_edges
                        WHERE is_state = 0 and event_id = $1
                        "#,
                        &[&event.event_id],
                        |row| row.get(0)
                    ).expect("room sql query for event edges failed");

                    event.edges = edges;
                }

                events
            }
        };

        write_200_json(res, &events);
    }
}

struct StateHandler {
    connector: DatabaseConnector,
}

impl RouteHandler for StateHandler {
    fn handle(&self, params: Params, _: Request, res: Response) {
        let event_id = params.find("event_id").expect("event_id not in params");

        let mut conn = self.connector.connect();

        let state = conn.query(
            r#"
            WITH RECURSIVE state(state_group) AS (
                SELECT state_group FROM event_to_state_groups WHERE event_id = $1
                UNION ALL
                SELECT prev_state_group FROM state_group_edges e, state s
                WHERE s.state_group = e.state_group
            )
            SELECT event_id, es.type, state_key, ej.json, e.depth
            FROM state_groups_state
            NATURAL JOIN (
                SELECT type, state_key, max(state_group) as state_group FROM state_groups_state
                WHERE state_group IN (
                    SELECT state_group FROM state
                )
                GROUP BY type, state_key
            ) es
            LEFT JOIN events e using (event_id)
            LEFT JOIN event_json ej USING (event_id)
            "#,
            &[&event_id],
            |row| StateRow {
                event_id: row.get(0),
                etype: row.get(1),
                state_key: row.get(2),
                content: content_from_json(row.get(3)),
                depth: row.get(4),
            }
        ).expect("state query failed");

        write_200_json(res, &state);
    }
}

fn content_from_json(s: String) -> serde_json::Value {
    let json: serde_json::Value = serde_json::from_str(&s).expect("content was not json");
    return json["content"].clone()
}

fn index(_: Params, _: Request, res: Response) {
    serve_static("index.html", res);
}

fn asset(params: Params, _: Request, res: Response) {
    let asset = params.find("asset").expect("asset not in params");
    serve_static(&format!("assets/{}", asset), res);
}

fn serve_static(filename: &str, mut res: Response) {
    let content_type = content_type_for_asset(filename);
    println!("Serving {} as {}", filename, content_type);

    let mut f = File::open(filename).expect(&format!("Failed to open '{}'", filename));
    res.headers_mut().set_raw("Content-Type", vec![content_type.to_string().into_bytes()]);

    let mut res = res.start().expect("failed to prepare response for writing");
    io::copy(&mut f, &mut res).expect("failed to write file");
    res.end().expect("failed to finish writing response");
}

fn content_type_for_asset(name: &str) -> &str {
    match name {
        _ if name.ends_with(".css") => "text/css",
        _ if name.ends_with(".js") => "application/javascript",
        _ if name.ends_with(".html") => "text/html",
        _ => "text/plain",
    }
}



fn main() {
    let matches = app_from_crate!()
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .takes_value(true)
            .default_value("12345")
        )
        .arg(Arg::with_name("db")
            .short("d")
            .long("database")
            .takes_value(true)
            .possible_values(&["sqlite", "postgres"])
            .default_value("postgres")
            .help("Which type of database to connect to")
        )
        .arg(Arg::with_name("connection_string")
            .index(1)
            .help("The connection string for the database.\n\
                   For postgres this should be e.g. postgresql://username:password@localhost:5435/synapse\n\
                   For sqlite it should be the path to the database")
            .next_line_help(true)
            .required(true)
        )
        .get_matches();

    let port = value_t_or_exit!(matches, "port", u16);
    let db_type = value_t_or_exit!(matches, "db", String);
    let connstr = value_t_or_exit!(matches, "connection_string", String);


    let connector = match &db_type[..] {
        "postgres" => DatabaseConnector::Postgres(connstr),
        "sqlite" => DatabaseConnector::Sqlite3(connstr),
        _ => panic!("unknown db param"), // shouldn't happen as clap should shout
    };

    connector.connect(); // Let's try to connect now to see if config works.

    let router = create_boxed_router! {
        "/" => Get => index,
        "/assets/:asset" => Get => asset,
        "/room/:room_id" => Get => RoomHandler { connector: connector.clone() },
        "/state/:event_id" => Get => StateHandler { connector: connector.clone() },
    };

    println!("Listening on port {}", port);
    Server::http(("0.0.0.0", port)).unwrap().handle(router).unwrap();
}
