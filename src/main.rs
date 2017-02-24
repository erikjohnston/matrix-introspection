#[macro_use]
extern crate hyper_router;
extern crate hyper;
extern crate postgres;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::collections::BTreeMap;
use hyper::status::StatusCode;
use hyper::server::{Request, Response, Server};
use hyper::uri::RequestUri;
use hyper_router::{Params, RouteHandler};
use postgres::{Connection, TlsMode};



fn get_conn() -> Connection {
    Connection::connect("postgresql://username:password@localhost:5435/synapse",
                        TlsMode::None)
        .unwrap()
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
        RequestUri::AbsolutePath(s) => {
            // ffs
            return hyper::Url::parse(&format!("http://foo{}", s)).unwrap();
        }
        RequestUri::AbsoluteUri(s) => { return s; }
        _ => { panic!("unsupported uri type"); }
    }
}

fn room(params: Params, req: Request, mut res: Response) {
    let room_id = params.find("room_id").expect("room_id not in params");

    /* please tell me there is an easier way to do this */
    let uri = parse_request_uri(req.uri);
    let qs: BTreeMap<_, _> = uri.query_pairs().collect();

    let max_depth = match qs.get("max_depth") {
        Some(x) => {
            x.parse::<i64>().expect(
                &format!("unable to parse max_depth '{}'", x))
        }
        _ => i64::max_value()
    };

    let page_size = 200;

    let conn = get_conn();

    let rows =
        conn.query(r#"SELECT event_id, events.type, state_key, depth, sender, state_group, content,
                   array(SELECT prev_event_id FROM event_edges WHERE is_state = false and event_id = events.event_id)
                   FROM events
                   LEFT JOIN state_events USING (event_id)
                   LEFT JOIN event_to_state_groups USING (event_id)
                   WHERE events.room_id = $1 AND topological_ordering <= $2::bigint
                   ORDER BY topological_ordering DESC
                   LIMIT $3::int
                   "#,
                   &[&room_id, &max_depth, &page_size])
            .expect("room sql query failed");

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
                edges: row.get(7),
            }
        })
        .collect();

    *res.status_mut() = StatusCode::Ok;
    res.headers_mut().set_raw("Access-Control-Allow-Headers", vec![b"Origin, X-Requested-With, Content-Type, Accept".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Origin", vec![b"*".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Methods", vec![b"GET, POST, PUT, DELETE, OPTIONS".to_vec()]);
    res.headers_mut().set_raw("Content-Type", vec![b"application/json".to_vec()]);

    let mut res = res.start().expect("failed to prepare response for writing");
    serde_json::to_writer(&mut res, &events).expect("failed to write json");
    res.end().expect("failed to finish writing response");
}


fn state(params: Params, _: Request, mut res: Response) {
    let event_id = params.find("event_id").expect("event_id not in params");

    let conn = get_conn();

    let rows = conn.query(r#"WITH RECURSIVE state(state_group) AS (
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
               &[&event_id])
        .expect("state query failed");

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
    res.headers_mut().set_raw("Access-Control-Allow-Headers", vec![b"Origin, X-Requested-With, Content-Type, Accept".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Origin", vec![b"*".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Methods", vec![b"GET, POST, PUT, DELETE, OPTIONS".to_vec()]);
    res.headers_mut().set_raw("Content-Type", vec![b"application/json".to_vec()]);

    let mut res = res.start().expect("failed to prepare response for writing");
    serde_json::to_writer(&mut res, &state).expect("failed to write json");
    res.end().expect("failed to finish writing response");
}


fn main() {
    let router = create_router! {
        "/room/:room_id" => Get => Box::new(room) as Box<RouteHandler>,
        "/state/:event_id" => Get => Box::new(state) as Box<RouteHandler>,
    };

    Server::http("0.0.0.0:12345").unwrap().handle(router).unwrap();
}
