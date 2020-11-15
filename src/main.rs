/// Default Compute@Edge template program.

use fastly::http::{Method, StatusCode};
use fastly::{Body, Error, Request, RequestExt, Response, ResponseExt};
use fastly::http::header::HeaderValue;

use serde_json;
use serde::{Serialize,Deserialize};

#[derive(Serialize,Deserialize)]
struct Session {
	id: u32,
	num_players: u32,
	players: Vec<String>
}

const KV_LOCAL: &str = "kvlocal";
const KV_GLOBAL: &str = "kvglobal";

fn header_val(header: Option<&HeaderValue>) -> &str {
	match header {
		Some(h) => h.to_str().unwrap_or(""),
		None => "",
	}
}

fn get_sessions() -> Result<Vec<Session>, Error> {
	let mut kvreq = Request::builder()
	.method(Method::GET)
	.uri("http://kv-global.vranish.dev/sessions")
	.body(Body::from(""))?;
	let resp = kvreq.send(KV_GLOBAL)?;
	match serde_json::from_str(&resp.into_body().into_string()) {
		Ok(v) => {
			let sessions : Vec<Session> = v;
			Ok(sessions)
		},
		_ => {
			Ok(Vec::<Session>::new())
		}
	}
}
fn write_sessions(sessions: Vec<Session>) {
	let json = serde_json::to_string(&sessions).unwrap();

	let mut kvreq = Request::builder()
	.method(Method::POST)
	.uri("http://kv-global.vranish.dev/sessions")
	.body(Body::from(json)).unwrap();
	let resp = kvreq.send(KV_GLOBAL);

}

fn get_next_id(sessions: &Vec<Session>) -> u32 {
	let mut highest = 0;
	for s in sessions {
		if s.id > highest {
			highest = s.id;
		}
	}
	highest + 1
}

fn create_session(name: &str) -> u32 {
	let mut sessions = get_sessions().unwrap();
	let id = get_next_id(&sessions);
	let mut new_session = Session{
		id: id,
		num_players: 1,
		players: Vec::<String>::new(),
	};
	new_session.players.push(name.to_string());
	sessions.push(new_session);
	write_sessions(sessions);
	id
}

fn update_session(index: usize, name: &str) {
	let mut sessions = get_sessions().unwrap();
	sessions[index].num_players+=1;
	sessions[index].players.push(name.to_string());
	write_sessions(sessions);
}

// let's keep this simple for now
fn rank_session(s: &Session) -> i32 {
	match s.num_players == 4 {
		true => i32::MIN,
		false => s.num_players as i32
	}
	// later perhaps look at update time
}

/// If `main` returns an error, a 500 error response will be delivered to the client.
#[fastly::main]
	fn main(mut req: Request<Body>) -> Result<impl ResponseExt, Error> {
    // Make any desired changes to the client request.
	// req.headers_mut()
    //     .insert("Host", HeaderValue::from_static("example.com"));

    // We can filter requests that have unexpected methods.
    const VALID_METHODS: [Method; 3] = [Method::HEAD, Method::GET, Method::POST];
    if !(VALID_METHODS.contains(req.method())) {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from("This method is not allowed"))?);
    }

    // Pattern match on the request method and path.
    match (req.method(), req.uri().path()) {

		// If request is a `GET` to the `/` path, send a default response.
        (&Method::GET, "/") => Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("Welcome to the Doom@Edge Session Services"))?),

		// get sessions from our kv
		// return them to the client in this form:
		// <number of entries>,[<id>,<num_players>,<player1>...<playerN>],
		(&Method::GET, "/sessions") => {
			let s = get_sessions();
			match s {
				Ok(sessions) => {
					let mut client_string = format!("{}",sessions.len());

					for s in &sessions {
						client_string.push_str(&format!(",{},{}",s.id,s.num_players));
						for p in &s.players {
							client_string.push_str(&format!(",{}",p));
						}
					}

					Ok(Response::builder()
					.status(StatusCode::OK)
					.body(Body::from(client_string))?)
				},
				_ => {
					Ok(Response::builder()
					.status(StatusCode::OK)
					.body(Body::from("0"))?)
				}
			}
		},
		(&Method::GET, "/join_best_session") => {
			let name = header_val(req.headers().get("name"));
			let s = get_sessions();
			match s {
				Ok(sessions) => {
					let mut best = i32::MIN;
					let mut best_index : i32 = -1;
					// if we are already in a session, return that one
					for (i,s) in sessions.iter().enumerate() {
						for p in &s.players {
							if p == name {
								return Ok(Response::builder()
								.status(StatusCode::OK)
								.body(Body::from(format!("{}",s.id)))?);
							}
							let rank = rank_session(s);
							println!("Ranking session {}", rank);
							if rank > best {
								best = rank;
								best_index = i as i32;
							}
						}
					}
					if best_index > -1 {
						let id = sessions[best_index as usize].id;
						update_session(best_index as usize,name);
						return Ok(Response::builder()
						.status(StatusCode::OK)
						.body(Body::from(format!("{}",id)))?);
					} else {
						let id = create_session(name);
						return Ok(Response::builder()
						.status(StatusCode::OK)
						.body(Body::from(format!("{}",id)))?);
					}
				},
				_ => {
					let id = create_session(name);
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.body(Body::from(format!("{}",id)))?);
				}
			}



			Ok(Response::builder()
			.status(StatusCode::OK)
			.body(Body::from("TODOSESSIONID"))?)
		},

        // Catch all other requests and return a 404.
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("The page you requested could not be found"))?),
    }
}
