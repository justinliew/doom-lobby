/// Default Compute@Edge template program.

use fastly::http::{Method, StatusCode};
use fastly::{Body, Error, Request, RequestExt, Response, ResponseExt};
use serde_json;
use serde::Deserialize;

#[derive(Deserialize)]
struct Session {
	id: u32,
	num_players: u32,
	players: Vec<String>
}

const KV_LOCAL: &str = "kvlocal";
const KV_GLOBAL: &str = "kvglobal";

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
			let mut kvreq = Request::builder()
			.method(Method::GET)
			.uri("http://kv-global.vranish.dev/sessions")
			.body(Body::from(""))?;
			let resp = kvreq.send(KV_GLOBAL)?;
			let resp_body = resp.into_body().into_string();
			match serde_json::from_str(&resp_body) {
				Ok(v) => {
					let sessions : Vec<Session> = v;
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

        // Catch all other requests and return a 404.
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("The page you requested could not be found"))?),
    }
}
