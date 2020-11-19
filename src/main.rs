/// Default Compute@Edge template program.

use fastly::http::{Method, StatusCode};
use fastly::{Body, Error, Request, RequestExt, Response, ResponseExt};
use fastly::http::header::HeaderValue;
use std::collections::HashMap;

use serde_json;
use serde::{Serialize,Deserialize};
extern crate serde_millis;

use std::time::{Duration,Instant};

#[derive(Serialize)]
struct Pop {
	name: &'static str,
	ip: &'static str,
}

const POPS: &'static [&'static Pop] = &[
	&Pop{name: "HKG", ip: "151.101.77.51"},
	&Pop{name: "IAH", ip: "151.101.181.51"},
	&Pop{name: "JAX", ip: "199.232.1.51"},
	&Pop{name: "JNB", ip: "151.101.173.51"},
	&Pop{name: "MCI", ip: "199.232.73.51"},
	&Pop{name: "LCY", ip: "151.101.17.51"},
	&Pop{name: "LON", ip: "199.232.57.51"},
	&Pop{name: "LHR", ip: "151.101.61.51"},
	&Pop{name: "BUR", ip: "151.101.197.51"},
	&Pop{name: "LAX", ip: "151.101.25.51"},
	&Pop{name: "MAD", ip: "151.101.133.51"},
	&Pop{name: "MAN", ip: "199.232.53.51"},
	&Pop{name: "MRS", ip: "199.232.81.51"},
	&Pop{name: "MEL", ip: "151.101.81.51"},
	&Pop{name: "MIA", ip: "151.101.5.51"},
	&Pop{name: "MSP", ip: "151.101.149.51"},
	&Pop{name: "STP", ip: "199.232.29.51"},
	&Pop{name: "YUL", ip: "151.101.137.51"},
	&Pop{name: "BOM", ip: "151.101.153.51"},
	&Pop{name: "LGA", ip: "199.232.37.51"},
	&Pop{name: "EWR", ip: "151.101.209.51"},
	&Pop{name: "ITM", ip: "151.101.89.51"},
	&Pop{name: "OSL", ip: "151.101.237.51"},
	&Pop{name: "PAO", ip: "151.101.189.51"},
	&Pop{name: "CDG", ip: "151.101.121.51"},
	&Pop{name: "GIG", ip: "151.101.177.51"},
	&Pop{name: "SJC", ip: "151.101.41.51"},
	&Pop{name: "SCL", ip: "151.101.221.51"},
	&Pop{name: "GRU", ip: "151.101.93.51"},
	&Pop{name: "SEA", ip: "151.101.53.51"},
	&Pop{name: "SIN", ip: "151.101.9.51"},
	&Pop{name: "STL", ip: "199.232.69.51"},
	&Pop{name: "BMA", ip: "151.101.85.51"},
	&Pop{name: "SYD", ip: "151.101.29.51"},
	&Pop{name: "TYO", ip: "151.101.109.51"},
	&Pop{name: "HND", ip: "151.101.229.51"},
	&Pop{name: "YYZ", ip: "151.101.125.51"},
	&Pop{name: "YVR", ip: "151.101.213.51"},
	&Pop{name: "VIE", ip: "199.232.17.51"},
];

#[derive(Serialize,Deserialize)]
struct Player {
	name: String,
	id: u32,
	index: usize,
	#[serde(with = "serde_millis")]
	last_heartbeat: Instant,
	pops: HashMap<String, u32>,
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
	let kvreq = Request::builder()
	.method(Method::GET)
	.uri("http://kv-global.vranish.dev/sessions")
	.body(Body::from(""))?;
	let resp = kvreq.send(KV_GLOBAL)?;
	let body_str = resp.into_body().into_string();
	match serde_json::from_str(&body_str) {
		Ok(v) => {
			let sessions : Vec<Session> = v;
			Ok(sessions)
		},
		_ => {
			Ok(Vec::<Session>::new())
		}
	}
}

fn add_ping_to_player(player: &mut Player, pop: &str, ping: u32) {
	*player.pops.entry(pop.to_string()).or_insert(0) = ping;
}

fn write_sessions(sessions: &Vec<Session>) {
	let json = serde_json::to_string(sessions).unwrap();

	let kvreq = Request::builder()
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
		pops: HashMap::new(),
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
				last_heartbeat: Instant::now(),
				pops: HashMap::new(),
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

	for p in &sessions[session_index].players {
		if p.id == id {
			return Ok((p.index,sessions[session_index].pop.clone()));
		}
	}

	join_session_by_index(session_index as usize, id, name)
}

fn get_best_pop_and_update(sessions: &Vec<Session>, sessionid: u32) -> Result<&str,&'static str> {
	let mut pop_vecs : Vec<Vec<(&String,&u32)>> = Vec::new();
	for session in sessions {
		if session.id == sessionid {
			for player in &session.players {
				let mut pop_vec: Vec<(&String, &u32)> = player.pops.iter().collect();
				pop_vec.sort_by(|a, b| b.1.cmp(a.1));
				pop_vecs.push(pop_vec);
			}
		}
	}
//	return Ok(&session.pop);

	Err("Could not find session")
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
	write_sessions(&sessions);
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
			let id = match header_val(req.headers().get("id")).parse::<u32>() {
				Ok(id) => {id},
				_ => {
					println!("Couldn't get id from /join_best_session header: {}", header_val(req.headers().get("id")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let name = header_val(req.headers().get("name"));
			let pop = header_val(req.headers().get("pop"));
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					prune_stale_sessions(&mut sessions,0,0,false);
					println!("After prune, we have {} sessions", sessions.len());
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
								.body(Body::from(format!("{},{},{}",s.id,p.index,pop)))?);
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
								.body(Body::from(format!("{},{},{}",sessionid,index,pop)))?);
							},
							_ => {
								return Ok(Response::builder()
								.status(StatusCode::OK)
								.header("Access-Control-Allow-Origin","*")
								.header("Access-Control-Allow-Headers","*")
								.header("Vary","Origin")
								.body(Body::from(format!("-1,-1,0")))?);
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
						.body(Body::from(format!("{},{},{}",sessionid,0,pop)))?);
					}
				},
				_ => {
					let sessionid = create_session(id,name,pop);
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(format!("{},{},{}",sessionid,0,pop)))?);
				}
			}
		},
		(&Method::GET, "/join_session") => {
			let name = header_val(req.headers().get("name"));
			let player_id = match header_val(req.headers().get("playerid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get player id from join_session: {}", header_val(req.headers().get("playerid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let session_id = match header_val(req.headers().get("sessionid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get session id from join_session: {}", header_val(req.headers().get("sessionid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
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
			let player_id = match header_val(req.headers().get("playerid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get player id from update_name_in_session: {}", header_val(req.headers().get("playerid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let session_id = match header_val(req.headers().get("sessionid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get session id from update_name_in_session: {}", header_val(req.headers().get("sessionid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let name = header_val(req.headers().get("name"));
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					for session in &mut sessions {
						if session.id == session_id {
							for p in &mut session.players {
								if p.id == player_id {
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
			let session_id = match header_val(req.headers().get("sessionid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get session id from update_pop_in_session: {}", header_val(req.headers().get("sessionid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let pop = header_val(req.headers().get("pop"));
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					for session in &mut sessions {
						if session.id == session_id {
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
			let player_id = match header_val(req.headers().get("playerid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get player id from heartbeat: {}", header_val(req.headers().get("playerid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let session_id = match header_val(req.headers().get("sessionid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get session id from heartbeat: {}", header_val(req.headers().get("sessionid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					prune_stale_sessions(&mut sessions,player_id,session_id,true);

					match get_best_pop_and_update(&sessions, session_id) {
						Ok(new_pop) => {
							write_sessions(&sessions);
							println!("heartbeat for {} {}, returning {}", session_id, player_id, new_pop);

							Ok(Response::builder()
							.status(StatusCode::OK)
							.header("Access-Control-Allow-Origin","*")
							.header("Access-Control-Allow-Headers","*")
							.header("Vary","Origin")
							.body(Body::from(format!("{}",new_pop)))?)
						},
						_ => {
							Ok(Response::builder()
							.status(StatusCode::OK)
							.header("Access-Control-Allow-Origin","*")
							.header("Access-Control-Allow-Headers","*")
							.header("Vary","Origin")
							.body(Body::from(format!("")))?)
						}
					}
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
		(&Method::GET, "/get_pops") => {
			Ok(Response::builder()
			.status(StatusCode::OK)
			.header("Access-Control-Allow-Origin","*")
			.header("Access-Control-Allow-Headers","*")
			.header("Vary","Origin")
			.body(Body::from(serde_json::to_string(&POPS).unwrap()))?)
		},
		(&Method::POST, "/add_ping_to_session") => {
			let player_id = match header_val(req.headers().get("playerid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get player id from add_ping_to_session: {}", header_val(req.headers().get("playerid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let session_id = match header_val(req.headers().get("sessionid")).parse::<u32>() {
				Ok(id) => id,
				_ => {
					println!("Couldn't get session id from add_ping_to_session: {}", header_val(req.headers().get("sessionid")));
					return Ok(Response::builder()
					.status(StatusCode::OK)
					.header("Access-Control-Allow-Origin","*")
					.header("Access-Control-Allow-Headers","*")
					.header("Vary","Origin")
					.body(Body::from(""))?);
				}
			};
			let ping = header_val(req.headers().get("ping")).parse::<u32>().unwrap();
			let pop = header_val(req.headers().get("pop"));
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					for session in &mut sessions {
						if session.id == session_id {
							for p in &mut session.players {
								if p.id == player_id {
									add_ping_to_player(p,pop,ping);
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
		// Catch all other requests and return a 404.
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("The page you requested could not be found"))?),
    }
}
