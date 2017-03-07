extern crate getopts;
#[macro_use]
extern crate hyper_router;
extern crate hyper;
extern crate postgres;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use getopts::Options;
use hyper::server::{Request, Response, Server};
use hyper::status::StatusCode;
use hyper::uri::RequestUri;
use hyper_router::{Params, RouteHandler};
use postgres::{Connection, TlsMode};
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io;


fn get_conn(connstr: &str) -> Connection {
    Connection::connect(connstr, TlsMode::None).unwrap()
}


#[derive(Serialize)]
struct RoomRow {
    event_id: String,
    etype: String,
    state_key: Option<String>,
    depth: i64,
    sender: String,
    state_group: Option<i64>,
    content: serde_json::Value,
    ts: i64,
    edges: Vec<String>,
}

#[derive(Serialize)]
struct StateRow {
    etype: String,
    state_key: String,
    event_id: String,
}

fn parse_request_uri(req_uri: RequestUri) -> hyper::Url {
    match req_uri {
        RequestUri::AbsolutePath(s) => hyper::Url::parse(&format!("http://foo{}", s)).unwrap(), // ffs
        RequestUri::AbsoluteUri(s) => s,
        _ => panic!("unsupported uri type"),
    }
}

struct RoomHandler {
    connection_string: String,
}

impl RouteHandler for RoomHandler {
    fn handle(&self, params: Params, req: Request, mut res: Response) {
        let room_id = params.find("room_id").expect("room_id not in params");

        /* please tell me there is an easier way to do this */
        let uri = parse_request_uri(req.uri);
        let qs: BTreeMap<_, _> = uri.query_pairs().collect();

        let max_depth: i64 = match qs.get("max_depth") {
            Some(x) => x.parse().expect("unable to parse max_depth"),
            _ => i64::max_value(),
        };

        let page_size = 200;

        let conn = get_conn(&self.connection_string);

        let rows =conn.query(
            r#"
            SELECT event_id, events.type, state_key, depth, sender, state_group, content, origin_server_ts,
                array(SELECT prev_event_id FROM event_edges WHERE is_state = false and event_id = events.event_id)
            FROM events
            LEFT JOIN state_events USING (event_id)
            LEFT JOIN event_to_state_groups USING (event_id)
            WHERE events.room_id = $1 AND topological_ordering <= $2::bigint
            ORDER BY topological_ordering DESC
            LIMIT $3::int
            "#,
            &[&room_id, &max_depth, &page_size]
        ).expect("room sql query failed");

        let events: Vec<RoomRow> = rows.into_iter()
            .map(|row| {
                RoomRow {
                    event_id: row.get(0),
                    etype: row.get(1),
                    state_key: row.get(2),
                    depth: row.get(3),
                    sender: row.get(4),
                    state_group: row.get(5),
                    content: serde_json::from_str(&row.get::<_, String>(6))
                        .expect("content was not json"),
                    ts: row.get(7),
                    edges: row.get(8),
                }
            })
            .collect();

        *res.status_mut() = StatusCode::Ok;
        res.headers_mut().set_raw("Access-Control-Allow-Headers",
                                  vec![b"Origin, X-Requested-With, Content-Type, Accept".to_vec()]);
        res.headers_mut().set_raw("Access-Control-Allow-Origin", vec![b"*".to_vec()]);
        res.headers_mut().set_raw("Access-Control-Allow-Methods",
                                  vec![b"GET, POST, PUT, DELETE, OPTIONS".to_vec()]);
        res.headers_mut().set_raw("Content-Type", vec![b"application/json".to_vec()]);

        let mut res = res.start().expect("failed to prepare response for writing");
        serde_json::to_writer(&mut res, &events).expect("failed to write json");
        res.end().expect("failed to finish writing response");
    }
}

struct StateHandler {
    connection_string: String,
}

impl RouteHandler for StateHandler {
    fn handle(&self, params: Params, _: Request, mut res: Response) {
        let event_id = params.find("event_id").expect("event_id not in params");

        let conn = get_conn(&self.connection_string);

        let rows = conn.query(
            r#"WITH RECURSIVE state(state_group) AS (
                SELECT state_group FROM event_to_state_groups WHERE event_id = $1
                UNION ALL
                SELECT prev_state_group FROM state_group_edges e, state s
                WHERE s.state_group = e.state_group
            )
            SELECT DISTINCT last_value(event_id) OVER (
                PARTITION BY type, state_key ORDER BY state_group ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS event_id, type, state_key FROM state_groups_state
            WHERE state_group IN (
                SELECT state_group FROM state
            )"#,
            &[&event_id]
        ).expect("state query failed");

        let state: Vec<StateRow> = rows.into_iter()
            .map(|row| {
                StateRow {
                    event_id: row.get(0),
                    etype: row.get(1),
                    state_key: row.get(2),
                }
            })
            .collect();

        *res.status_mut() = StatusCode::Ok;
        res.headers_mut().set_raw("Access-Control-Allow-Headers",
                                  vec![b"Origin, X-Requested-With, Content-Type, Accept".to_vec()]);
        res.headers_mut().set_raw("Access-Control-Allow-Origin", vec![b"*".to_vec()]);
        res.headers_mut().set_raw("Access-Control-Allow-Methods",
                                  vec![b"GET, POST, PUT, DELETE, OPTIONS".to_vec()]);
        res.headers_mut().set_raw("Content-Type", vec![b"application/json".to_vec()]);

        let mut res = res.start().expect("failed to prepare response for writing");
        serde_json::to_writer(&mut res, &state).expect("failed to write json");
        res.end().expect("failed to finish writing response");
    }
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


fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("p", "", "set listen port (default 12345)", "PORT");
    opts.optopt("c", "", "set db connection string", "CONNSTR");
    opts.optflag("h", "help", "print this help menu");
    let parsed_opts = opts.parse(&args[1..]).expect("Error parsing commandline");
    if parsed_opts.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let port = if let Some(x) = parsed_opts.opt_str("p") {
        x.parse::<u16>().expect("unable to parse port")
    } else {
        12345
    };

    let connstr = parsed_opts.opt_str("c")
        .expect("connection string must be supplied. example: \"-c \
                 postgresql://username:password@localhost:5435/synapse\"");

    let router = create_router! {
        "/" => Get => Box::new(index) as Box<RouteHandler>,
        "/assets/:asset" => Get => Box::new(asset) as Box<RouteHandler>,
        "/room/:room_id" => Get => Box::new(RoomHandler { connection_string: connstr.clone() })
            as Box<RouteHandler>,
        "/state/:event_id" => Get => Box::new(StateHandler { connection_string: connstr.clone() })
            as Box<RouteHandler>,
    };

    println!("Listening on port {}", port);
    Server::http(("0.0.0.0", port)).unwrap().handle(router).unwrap();
}
