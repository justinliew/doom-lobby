/// Default Compute@Edge template program.

use fastly::http::{Method, StatusCode};
use fastly::{Body, Error, Request, RequestExt, Response, ResponseExt};
use fastly::http::header::HeaderValue;
use std::collections::HashMap;
use core::cmp::Ordering::Equal;

use serde::{Serialize,Deserialize};
extern crate serde_millis;

use std::time::{Duration,Instant};

#[derive(Serialize)]
struct StaticPop {
	name: &'static str,
	ip: &'static str,
}

const POPS: &'static [&'static StaticPop] = &[
	&StaticPop{name: "HKG", ip: "151.101.77.51"},
	&StaticPop{name: "IAH", ip: "151.101.181.51"},
	&StaticPop{name: "JAX", ip: "199.232.1.51"},
	&StaticPop{name: "JNB", ip: "151.101.173.51"},
	&StaticPop{name: "MCI", ip: "199.232.73.51"},
	&StaticPop{name: "LCY", ip: "151.101.17.51"},
	&StaticPop{name: "LON", ip: "199.232.57.51"},
	&StaticPop{name: "LHR", ip: "151.101.61.51"},
	&StaticPop{name: "BUR", ip: "151.101.197.51"},
	&StaticPop{name: "LAX", ip: "151.101.25.51"},
	&StaticPop{name: "MAD", ip: "151.101.133.51"},
	&StaticPop{name: "MAN", ip: "199.232.53.51"},
	&StaticPop{name: "MRS", ip: "199.232.81.51"},
	&StaticPop{name: "MEL", ip: "151.101.81.51"},
	&StaticPop{name: "MIA", ip: "151.101.5.51"},
	&StaticPop{name: "MSP", ip: "151.101.149.51"},
	&StaticPop{name: "STP", ip: "199.232.29.51"},
	&StaticPop{name: "YUL", ip: "151.101.137.51"},
	&StaticPop{name: "BOM", ip: "151.101.153.51"},
	&StaticPop{name: "LGA", ip: "199.232.37.51"},
	&StaticPop{name: "EWR", ip: "151.101.209.51"},
	&StaticPop{name: "ITM", ip: "151.101.89.51"},
	&StaticPop{name: "OSL", ip: "151.101.237.51"},
	&StaticPop{name: "PAO", ip: "151.101.189.51"},
	&StaticPop{name: "CDG", ip: "151.101.121.51"},
	&StaticPop{name: "GIG", ip: "151.101.177.51"},
	&StaticPop{name: "SJC", ip: "151.101.41.51"},
	&StaticPop{name: "SCL", ip: "151.101.221.51"},
	&StaticPop{name: "GRU", ip: "151.101.93.51"},
	&StaticPop{name: "SEA", ip: "151.101.53.51"},
	&StaticPop{name: "SIN", ip: "151.101.9.51"},
	&StaticPop{name: "STL", ip: "199.232.69.51"},
	&StaticPop{name: "BMA", ip: "151.101.85.51"},
	&StaticPop{name: "SYD", ip: "151.101.29.51"},
	&StaticPop{name: "TYO", ip: "151.101.109.51"},
	&StaticPop{name: "HND", ip: "151.101.229.51"},
	&StaticPop{name: "YYZ", ip: "151.101.125.51"},
	&StaticPop{name: "YVR", ip: "151.101.213.51"},
	&StaticPop{name: "VIE", ip: "199.232.17.51"},
];

#[derive(Serialize,Deserialize)]
struct Pop {
	name: String,
	ping: u32,
}

#[derive(Serialize,Deserialize)]
struct Player {
	name: String,
	id: u32,
	index: usize,
	#[serde(with = "serde_millis")]
	last_heartbeat: Instant,
	pops: Vec<Pop>,
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

fn add_pings_to_player(player: &mut Player, json: &str) {
	match serde_json::from_str(&json) {
		Ok(p) => {
			player.pops = p;
		},
		_ => {}
	}
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
		pops: Vec::new(),
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
				pops: Vec::new(),
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

fn get_best_pop_and_update(sessions: &Vec<Session>, sessionid: u32) -> Result<String,&'static str> {
	let mut pop_vecs : Vec<Vec<(&String,&u32)>> = Vec::new();
	let mut merged_pops: HashMap<String, Vec<u32>> = HashMap::new();

	for session in sessions {
		if session.id == sessionid {
			for player in &session.players {
				for pop in &player.pops {
					merged_pops.entry(pop.name.to_string()).or_insert(Vec::new()).push(pop.ping);
				}
			}
			let merged_as_vec: Vec<(&String, &Vec<u32>)> = merged_pops.iter().collect();

			//.sort_by(|a,b| (b.1.partial_cmp(&a.1).unwrap_or(Equal)))
			let mut sorted_pops = merged_as_vec.iter().map(|(n,ps)| (n,ps.iter().sum::<u32>() as f32 / ps.len() as f32)).collect::<Vec<(&&String,f32)>>();
			sorted_pops.sort_by(|a,b| (a.1.partial_cmp(&b.1).unwrap_or(Equal)));
			return Ok(sorted_pops[0].0.to_string());
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
		(&Method::POST, "/add_pings_to_session") => {
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
			let json = req.into_body().into_string();
			println!("add_pings_to_session got {}", json);
			let s = get_sessions();
			match s {
				Ok(mut sessions) => {
					for session in &mut sessions {
						if session.id == session_id {
							for p in &mut session.players {
								if p.id == player_id {
									add_pings_to_player(p, &json);
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
