/// Default Compute@Edge template program.

use fastly::http::{Method, StatusCode};
use fastly::{Body, Error, Request, RequestExt, Response, ResponseExt};
use fastly::http::header::HeaderValue;

use serde_json;
use serde::{Serialize,Deserialize};
extern crate serde_millis;

use std::time::{Duration,Instant};

#[derive(Serialize,Deserialize)]
struct Player {
	name: String,
	id: u32,
	index: usize,
	#[serde(with = "serde_millis")]
	last_heartbeat: Instant,
}

#[derive(Serialize,Deserialize)]
struct Session {
	id: u32,
	pop: String,
	players: Vec<Player>
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
fn write_sessions(sessions: &Vec<Session>) {
	let json = serde_json::to_string(sessions).unwrap();
	println!("write_sessions: {}", json);

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

fn create_session(playerid: u32, name: &str, pop: &str) -> u32 {
	let mut sessions = get_sessions().unwrap();
	let sessionid = get_next_id(&sessions);
	println!("create_session {}", sessionid);
	let mut new_session = Session{
		id: sessionid,
		pop: pop.to_string(),
		players: Vec::<Player>::new(),
	};
	let new_player = Player{
		id: playerid,
		name: name.to_string(),
		index: 0,
		last_heartbeat: Instant::now(),
	};
	println!("create_session: adding player {} {}", new_player.id, new_player.name);

	new_session.players.push(new_player);
	sessions.push(new_session);
	write_sessions(&sessions);
	sessionid
}

fn join_session_by_index(session_index: usize, id: u32, name: &str) -> Result<(usize,String),&'static str> {
	let mut sessions = get_sessions().unwrap();

	let mut slots = [false;4];
	for p in &sessions[session_index].players {
		slots[p.index] = true;
	}
	for i in 0..4 {
		if !slots[i] {
			let new_player = Player{
				id: id,
				name: name.to_string(),
				index: i,
				last_heartbeat: Instant::now()
			};
			println!("join_session: adding player {} {} to slot {} in session {}", id, name, i, session_index);
			sessions[session_index].players.push(new_player);
			write_sessions(&sessions);
			return Ok((i,sessions[session_index].pop.clone()));
		}
	}
	Err("No player slot found")
}

fn join_session(session_id: u32, id: u32, name: &str) -> Result<(usize,String),&'static str> {
	let sessions = get_sessions().unwrap();

	let mut session_index = usize::MAX;
	for (i,s) in sessions.iter().enumerate() {
		if s.id == session_id {
			session_index = i;
			break;
		}
	}
	if session_index == usize::MAX {
		return Err("Couldn't find session");
	}

	join_session_by_index(session_index as usize, id, name)
}

fn prune_stale_sessions(sessions: &mut Vec<Session>, playerid: u32, sessionid: u32, do_update: bool) {
	let now = Instant::now();
	for session in &mut sessions.iter_mut() {
		for p in &mut session.players {
			if do_update && p.id == playerid && sessionid == session.id {
				p.last_heartbeat = now;
			}
		}
		session.players.retain(|p| {
			now.duration_since(p.last_heartbeat) < Duration::from_secs(60 * 1)
		});
	}
	sessions.retain(|s| {
		s.players.len() > 0
	});
}

// let's keep this simple for now
fn rank_session(s: &Session) -> i32 {
	match s.players.len() == 4 {
		true => i32::MIN,
		false => s.players.len() as i32
	}
	// later perhaps look at update time
}

/// If `main` returns an error, a 500 error response will be delivered to the client.
#[fastly::main]
	fn main(mut req: Request<Body>) -> Result<impl ResponseExt, Error> {
    // Make any desired changes to the client request.
	req.headers_mut()
        .insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));

	if req.method() == Method::OPTIONS {
        return Ok(Response::builder()
			.status(StatusCode::OK)
			.header("Access-Control-Allow-Origin","*")
			.header("Access-Control-Allow-Headers","*")
			.header("Vary","Origin")
            .body(Body::from(""))?);
	}
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
		// <number of entries>,[<sessionid>,<num_players>,[playerid,playername]...<playerN>],
		(&Method::GET, "/sessions") => {
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					prune_stale_sessions(&mut sessions,0,0,false);

					Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(serde_json::to_string(&sessions).unwrap()))?)
				},
				_ => {
					Ok(Response::builder()
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.status(StatusCode::OK)
					.body(Body::from("0"))?)
				}
			}
		},
		(&Method::GET, "/join_best_session") => {
			let id = header_val(req.headers().get("id")).parse::<u32>().unwrap();
			let name = header_val(req.headers().get("name"));
			let pop = header_val(req.headers().get("pop"));
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					prune_stale_sessions(&mut sessions,0,0,false);
					let mut best = i32::MIN;
					let mut best_index : i32 = -1;
					// if we are already in a session, return that one
					for (i,s) in sessions.iter().enumerate() {
						for p in &s.players {
							if p.id == id {
								println!("/join_best_session {} rejoining existing session {}", id,s.id);
								return Ok(Response::builder()
								.status(StatusCode::OK)
								.header("Access-Control-Allow-Origin","*")
								.header("Access-Control-Allow-Headers","*")
								.header("Vary","Origin")
								.body(Body::from(format!("{},{}",s.id,p.index)))?);
							}
							let rank = rank_session(s);
							if rank > best {
								best = rank;
								best_index = i as i32;
							}
						}
					}
					if best_index > -1 {
						let sessionid = sessions[best_index as usize].id;
						println!("/join_best_session {} joining existing session {}", id,sessionid);
						match join_session_by_index(best_index as usize,id,name) {
							Ok((index,pop)) => {
								return Ok(Response::builder()
								.status(StatusCode::OK)
								.header("Access-Control-Allow-Origin","*")
								.header("Access-Control-Allow-Headers","*")
								.header("Vary","Origin")
								.body(Body::from(format!("{},{}",sessionid,index)))?);
							},
							_ => {
								return Ok(Response::builder()
								.status(StatusCode::OK)
								.header("Access-Control-Allow-Origin","*")
								.header("Access-Control-Allow-Headers","*")
								.header("Vary","Origin")
								.body(Body::from(format!("-1,-1")))?);
							}
						}
					} else {
						let sessionid = create_session(id,name,pop);
						println!("/join_best_session {} create new session {}", id,sessionid);
						return Ok(Response::builder()
						.header("Access-Control-Allow-Origin","*")
						.header("Access-Control-Allow-Headers","*")
						.header("Vary","Origin")
						.status(StatusCode::OK)
						.body(Body::from(format!("{},{}",sessionid,0)))?);
					}
				},
				_ => {
					let sessionid = create_session(id,name,pop);
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(format!("{},{}",sessionid,0)))?);
				}
			}
		},
		(&Method::GET, "/join_session") => {
			let name = header_val(req.headers().get("name"));
			let player_id = header_val(req.headers().get("playerid")).parse::<u32>().unwrap();
			let session_id = header_val(req.headers().get("sessionid")).parse::<u32>().unwrap();
			match join_session(session_id,player_id,name) {
				Ok((index,pop)) => {
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(format!("{},{}",index,pop)))?);
				}
				_ => {
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from("-1,\"\""))?);
				}
			}
		},
		(&Method::POST, "/update_name_in_session") => {
			let sessionid = header_val(req.headers().get("sessionid")).parse::<u32>().unwrap();
			let playerid = header_val(req.headers().get("playerid")).parse::<u32>().unwrap();
			let name = header_val(req.headers().get("name"));
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					for session in &mut sessions {
						if session.id == sessionid {
							for p in &mut session.players {
								if p.id == playerid {
									p.name = name.to_string();
								}
							}
						}
					}
					write_sessions(&sessions);

					Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?)
				},
				_ => {
					Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?)
				}
			}
		},
		(&Method::POST, "/update_pop_in_session") => {
			let sessionid = header_val(req.headers().get("sessionid")).parse::<u32>().unwrap();
			let pop = header_val(req.headers().get("pop"));
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					for session in &mut sessions {
						if session.id == sessionid {
							session.pop = pop.to_string();
						}
					}
					write_sessions(&sessions);

					Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?)
				},
				_ => {
					Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?)
				}
			}
		},
		(&Method::POST, "/heartbeat") => {
			let sessionid = header_val(req.headers().get("sessionid")).parse::<u32>().unwrap();
			let playerid = header_val(req.headers().get("playerid")).parse::<u32>().unwrap();
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					prune_stale_sessions(&mut sessions,playerid,sessionid,true);
					write_sessions(&sessions);

					Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?)
				},
				_ => {
					Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?)
				}
			}
		},
		// Catch all other requests and return a 404.
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("The page you requested could not be found"))?),
    }
}
