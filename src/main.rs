extern crate iron;
extern crate json;
extern crate chrono;
extern crate router;
extern crate typed_html;
#[macro_use]
extern crate mime;
#[macro_use]
extern crate lazy_static;

use iron::prelude::*;
use iron::{status, Iron, method::Method};
use std::sync::{mpsc, Arc, Mutex};
use router::{Router};
use std::collections::HashMap;
use std::io::prelude::*;
use std::fmt;
use std::time::{SystemTime, Duration};
use std::thread;
use chrono::offset::Utc;
use chrono::DateTime;
use std::rc::Rc;


#[derive(Debug, Clone)]
pub struct Client 
{
    host: String,
    port: u64,
}

impl Client
{
    pub fn new(_host: String, _port: u64,) -> Client
    {
        Client
        {
            host: _host,
            port: _port,
        }
    }

    fn _get_client_log(this: &Self, queues: HashMap<String, Arc<Mutex<Queue>>>, data_type: &str) -> json::JsonValue
    {
        let mut data:  Vec<json::JsonValue> = vec![];
       
        for (queue_name, q) in queues
        {
            if data_type != "msg"
            {
                match 
                    if   data_type == "pub" { q.lock().unwrap().clone().publishers.iter().position( |user| user == this)  }
                    else                    { q.lock().unwrap().clone().subscribers.iter().position(|user| user == this) }
                    
                {
                    Some(_) => data.push(json::JsonValue::from(queue_name)),
                    None    => (),
                }
            } else {
                let msgs = q.lock().unwrap().clone().data.iter().filter(|meesage| meesage.sender == *this).cloned().collect::<Vec<MSG>>();

                for message in msgs { data.push(json::JsonValue::from(message)); }
            }
        }

        return json::JsonValue::from(data);
    }

    pub fn get_queue_where_client_publisher(this: &Self, queues: HashMap<String, Arc<Mutex<Queue>>>) -> json::JsonValue
    {
        Self::_get_client_log(this, queues, "pub")
    }

    pub fn get_queue_where_client_subscriber(this: &Self, queues: HashMap<String, Arc<Mutex<Queue>>>) -> json::JsonValue
    {
        Self::_get_client_log(this, queues, "sub")
    }

    pub fn get_client_messsages(this: &Self, queues: HashMap<String, Arc<Mutex<Queue>>>) -> json::JsonValue
    {
        Self::_get_client_log(this, queues, "msg")
    }

    pub fn get_my_data(this: &Self, queues: HashMap<String, Arc<Mutex<Queue>>>) -> json::JsonValue
    {
        let mut data: HashMap<String, json::JsonValue> = HashMap::new();

        data.insert(String::from("publisher"),  Self::get_queue_where_client_publisher( this, queues.clone()));
        data.insert(String::from("subscriber"), Self::get_queue_where_client_subscriber(this, queues.clone()));
        data.insert(String::from("messages"),   Self::get_client_messsages(             this, queues.clone()));

        return json::JsonValue::from(data);
    }
}

impl std::convert::From<Client> for json::JsonValue
{
    fn from(_client: Client) -> Self
    {
        json::object!
        {
            "host" => _client.host,
            "port" => _client.port,
        }
    }
}

impl fmt::Display for Client 
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result 
    {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl std::cmp::PartialEq for Client
{
    fn eq(&self, other: &Self) -> bool 
    {
        self.host == other.host && self.port == other.port
    }
}

#[derive(Debug, Clone)]
pub struct MSG
{
    sender:     Client,
    recipients: Vec<Client>,
    created:    SystemTime,
    lifetime:   SystemTime,
    data:       String,
    active:     bool,
    priority:   usize
}

impl MSG
{
    pub fn new(data: String, sender: Client, recipients: Vec<Client>, lifetime: SystemTime, priority: Option<usize>) -> MSG
    {
        MSG
        {
            sender:     sender,
            recipients: recipients,
            created:    SystemTime::now(),
            lifetime:   lifetime,
            data:       data,
            active:     true,
            priority:   priority.unwrap_or(0)
        }
    }
}

impl std::convert::From<MSG> for json::JsonValue
{
    fn from(message: MSG) -> Self
    {
        let created:  DateTime<Utc> = message.created.into();
        let lifetime: DateTime<Utc> = message.created.into();

        json::object!
        {
            "sender"     => message.sender,
            "recipients" => message.recipients,
            "created"    => format!("{}", created.format("%d/%m/%Y %T")),
            "lifetime"   => format!("{}", lifetime.format("%d/%m/%Y %T")),
            "data"       => message.data,
            "active"     => message.active,
            "priority"   => message.priority,
        }
    }
}


#[derive(Debug, Clone)]
pub struct Queue 
{
    name:        String,
    publishers:  Vec<Client>,
    subscribers: Vec<Client>,
    data:        Vec<MSG>
}

impl Queue 
{
    pub fn new(name: String, creator: Client) -> Queue 
    {
        let mut this = Queue 
        {
            name: name,
            publishers:  Vec::new(),
            subscribers: Vec::new(),
            data:        Vec::new(),
        };

        this.publishers.push(creator);

        return this;
    }

    pub fn push(&mut self, data: String, publisher: Client, lifetime: Option<f64>, priority: Option<usize>) -> Result<MSG, String>
    {
        let lt = SystemTime::now();
        
        if let Some(v)=lifetime
        {
            lt.checked_add(Duration::from_secs_f64(v));
        } else {
            lt.checked_add(Duration::from_secs_f64(6.0));
        }

        match self.publishers.iter().position(|user| *user == publisher)
        {
            Some(_) => 
            {
                let msg = MSG::new(data, publisher, self.subscribers.clone(), SystemTime::now(), priority);

                self.data.push(msg.clone());
                Ok(msg)
            }
            None    => Err(format!("\"{}\" is not publisher", publisher)),
        }
    }

    pub fn sub(&mut self, subscriber: Client) -> Result<Vec<Client>, String>
    {
        match self.subscribers.iter().position(|user| *user == subscriber)
        {
            Some(_) => Err(String::from(format!("User \"{}\" allready subscriber of queue.", subscriber))),
            None    => 
            {
                self.subscribers.push(subscriber);
                return Ok(self.subscribers.clone());
            }
        }
    }

    pub fn unsub(&mut self, subscriber: Client) -> Result<Vec<Client>, String>
    {
        match self.subscribers.iter().position(|user| *user == subscriber)
        {
            Some(index) => 
            {
                self.subscribers.remove(index);
                Ok(self.subscribers.clone())
            },
            None => Err(String::from(format!("User \"{}\" is not publisher of queue.", subscriber))),
        }
    }

    pub fn add_publisher(&mut self, publisher: Client) -> Result<Vec<Client>, String>
    {
        match self.publishers.iter().position(|user| *user == publisher)
        {
            Some(_) => Err(String::from(format!("User \"{}\" allready publisher of queue.", publisher))),
            None    => 
            {
                self.publishers.push(publisher);
                Ok(self.publishers.clone())
            }
        }
    }

    pub fn remove_publisher(&mut self, publisher: Client) -> Result<Vec<Client>, String>
    {
        match self.publishers.iter().position(|user| *user == publisher)
        {
            Some(index) => 
            {
                self.publishers.remove(index);
                Ok(self.publishers.clone())
            },
            None       => Err(String::from(format!("User \"{}\" is not subscriber of queue.", publisher))),
        }
    }
}

impl std::convert::From<Queue> for json::JsonValue
{
    fn from(queue: Queue) -> Self
    {
        let data = queue.data.clone();

        json::object!
        {
            "name"        => queue.name.clone(),
            "publisher"   => queue.publishers.clone(),
            "subscribers" => queue.subscribers.clone(),
            "data"        => data.into_iter().map(|el| el.clone()).collect::<Vec<_>>(),
        }
    }
}

impl fmt::Display for Queue 
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result 
    {
        write!(f, "{}", json::JsonValue::from(self.clone()))
    }
}


trait ApplicationResponse
{
    fn json(data:    json::JsonValue, code: status::Status) -> Response;
    fn html(content: String,          code: status::Status) -> Response;
}

impl ApplicationResponse for Response 
{
    fn json(data: json::JsonValue, code: status::Status) -> Response
    {
        let mut response = Response::new();
        
        response.set_mut(code);
        response.set_mut(mime!(Application/Json; Charset=Utf8));
        response.set_mut(data.dump());

        return response;
    }

    fn html(content: String, status_code: status::Status) -> Response 
    {
        let mut response = Response::new();

        response.set_mut(status_code);
        response.set_mut(mime!(Text/Html; Charset=Utf8));
        response.set_mut(content);

        return response;
    }
}


#[derive(Clone)]
enum Handler<'a>
{
    Empty(            &'a dyn Fn()                        -> IronResult<Response>),
    OnlyClient(       &'a dyn Fn(Client)                  -> IronResult<Response>),
    ClientAndFormdata(&'a dyn Fn(Client, json::JsonValue) -> IronResult<Response>),
}

unsafe impl<'a> std::marker::Sync for Handler<'a> {}


struct Server
{
    host:   String, 
    port:   u64,
}

impl<'a> Server
{
    fn new (host:String, port: u64, _router: router::Router) -> Server
    {
        let mut app = Server { host: host, port: port };

        app.run(_router);

        return app;
    }

    fn is_formdata_not_valide(formdata: json::JsonValue, require_params: Vec<String>) -> Option<String>
    {
        for param in &require_params
        {
            if formdata[&*param].is_null()
            {
                return Some(format!(r#""{}" is require."#, param));
            }
        }

        return None;
    }

    fn get_json_data(_request: &mut Request, require_params: Vec<String>) -> Result<json::JsonValue, json::JsonValue>
    {
        let mut buffer = String::from("");
        
        match _request.body.by_ref().read_to_string(&mut buffer)
        {
            Err(e) => Err(json::object!("error"=> format!("{}", e))),
            Ok(_)  => match json::parse(&*buffer) 
            {
                Err(e)       => Err(json::object!("error"=> format!("{}", e))),
                Ok(formdata) => 
                {
                    if let Some(err) = Self::is_formdata_not_valide(formdata.clone(), require_params)
                    {
                        Err(json::object!("error"=> err))
                    } else {
                        Ok(formdata)
                    }
                },
            }
        }
    }

    fn _handler(request: &mut Request, method: String, handler: Handler, require_params: Vec<String>) -> IronResult<Response>
    {
        let valide = if &*method != "get" && require_params.len() > 0 { Server::get_json_data(request, require_params) } else { Ok(json::object!{}) };
        let client =  Client::new(format!("{}", request.remote_addr.ip()), request.remote_addr.port() as u64);

        match valide
        {
            Ok(formdata) =>
            {
                match handler
                {
                    Handler::Empty(func)             => func(),
                    Handler::OnlyClient(func)        => func(client),
                    Handler::ClientAndFormdata(func) => func(client, formdata),
                }
            },
            Err(txt) => Ok(Response::json(json::object!{"error" => format!("\"{}\"", txt)}, status::BadRequest)),
        }
    }

    pub fn run(&mut self, _router: Router)
    {
        Iron::new(_router).http(format!("{}:{}", self.host, self.port)).unwrap();
    }
}


mod qgatawey
{
    use super::*;

    lazy_static!
    {
        pub static ref QUEUES: Arc<Mutex<HashMap<String, Arc<Mutex<Queue>>>>> = Arc::new(Mutex::new(HashMap::new()));
    }

    pub fn _json_response_finalize(rx: std::sync::mpsc::Receiver<Result<json::JsonValue, String>>) -> IronResult<Response> 
    {
        match rx.recv().unwrap()
        {
            Ok(queues) => Ok(Response::json(queues, status::Ok)),
            Err(txt)   => Ok(Response::json(json::object!{"error" => txt}, status::BadRequest)),
        }
    }

    pub fn queues_to_json() -> json::JsonValue
    {
        let mut data = json::object!{};

        for (k, v) in Arc::clone(&QUEUES).lock().unwrap().clone()
        {
            data[k] = json::JsonValue::from((&*v.clone().lock().unwrap()).clone());
        }

        return data;
    }

    pub fn get_user_log(client: Client) -> IronResult<Response>
    {
        Ok(Response::json(Client::get_my_data(&client, QUEUES.clone().lock().unwrap().clone()), status::Ok))
    }

    pub fn queue_insert(queue: Queue) -> IronResult<Response>
    {
        let (tr, rx) = mpsc::channel();
        
        thread::spawn(move || {
                
            if !QUEUES.lock().unwrap().contains_key(&*queue.name.clone())
            {
                QUEUES.lock().unwrap().insert(queue.name.clone(), Arc::new(Mutex::new(queue)));

                tr.send(Ok(queues_to_json())).unwrap();
            } else {
                tr.send(Err(format!("\"{}\" allready exists", queue.name.clone()))).unwrap();
            }
        });

        return _json_response_finalize(rx);
    }

    pub fn new_queue(client: Client, formdata: json::JsonValue) -> IronResult<Response>
    {
        let queue = Queue::new(format!("{}", formdata["name"]), client);   
                
        return queue_insert(queue);
    }

    fn _sub_or_unsub(client: Client, formdata: json::JsonValue, sub: bool) -> IronResult<Response>
    {
        let queue_name = format!("{}", formdata["name"]);
        let (tr, rx)   = mpsc::channel();

        thread::spawn(move || {
               
            match QUEUES.lock().unwrap().get_mut(&*queue_name.clone())
            {
                Some(q) =>
                {
                    let mut queue = (**&q.lock().unwrap()).clone();

                    match if sub { queue.sub(client) } else { queue.unsub(client) }
                    {
                        Ok(qs) => 
                        {
                            *q = Arc::new(Mutex::new(queue.clone()));
                            tr.send(Ok(json::JsonValue::from(qs))).unwrap()
                        },
                        Err(txt) => tr.send(Err(txt)).unwrap()
                    }
                },
                None    => tr.send(Err(format!("\"{}\" not exists", &*queue_name.clone()))).unwrap(),
            }
        });

        return _json_response_finalize(rx);
    }

    pub fn sub(client: Client, formdata: json::JsonValue) -> IronResult<Response>
    {
        _sub_or_unsub(client, formdata, true)
    }

    pub fn unsub(client: Client, formdata: json::JsonValue) -> IronResult<Response>
    {
        _sub_or_unsub(client, formdata, false)
    }

    fn _pub_or_unpub(client: Client, formdata: json::JsonValue, publ: bool) -> IronResult<Response>
    {
        let (tr, rx)   = mpsc::channel();
        let queue_name = format!("{}", formdata["name"]);

        thread::spawn(move || {
               
            match QUEUES.lock().unwrap().get_mut(&*queue_name.clone())
            {
                Some(q) =>
                {
                    let mut queue = (**&q.lock().unwrap()).clone();

                    match if publ { queue.add_publisher(client) } else { queue.remove_publisher(client) }
                    {
                        Ok(qs) => 
                        {
                            *q = Arc::new(Mutex::new(queue.clone()));
                            tr.send(Ok(json::JsonValue::from(qs))).unwrap()
                        },
                        Err(txt) => tr.send(Err(txt)).unwrap()
                    }
                },
                None    => tr.send(Err(format!("\"{}\" not exists", &*queue_name.clone()))).unwrap(),
            }
        });

        return _json_response_finalize(rx);
    }

    pub fn _pub(client: Client, formdata: json::JsonValue) -> IronResult<Response>
    {
        _pub_or_unpub(client, formdata, true)
    }

    pub fn unpub(client: Client, formdata: json::JsonValue) -> IronResult<Response>
    {
        _pub_or_unpub(client, formdata, false)
    }

    pub fn push_in_queue(client: Client, formdata: json::JsonValue) -> IronResult<Response>
    {
        let (tr, rx) = mpsc::channel();

        thread::spawn(move || {
               
            match QUEUES.lock().unwrap().get_mut(&*format!("{}", formdata["name"]))
            {
                Some(q) =>
                {
                    let mut queue = (**&q.lock().unwrap()).clone();

                    match queue.push(format!("{}", formdata["data"]), client, formdata["lifetime"].as_f64(), formdata["priority"].as_usize())
                    {
                        Ok(qs) => 
                        {
                            *q = Arc::new(Mutex::new(queue.clone()));
                            tr.send(Ok(json::JsonValue::from(qs))).unwrap()
                        },
                        Err(txt) => tr.send(Err(txt)).unwrap()
                    }
                },
                None    => tr.send(Err(format!("\"{}\" not exists", &*format!("{}", formdata["name"])))).unwrap(),
            }
        });

        return _json_response_finalize(rx);
    }

    pub fn full_map() -> IronResult<Response>
    {
        return IronResult::Ok(Response::json(queues_to_json(), status::Ok));
    }
}


fn router_add_path(_router: &mut router::Router, path: &str, method: &str, handler: &'static Handler, require_params: Option<Vec<&str>>)
{
    let rp: Vec<String> = match require_params
    {
        Some(params) => params.iter().map(|el| format!("{}", el)).collect::<Vec<String>>(),
        None         => Vec::new(),
    };
    let m = format!("{}", method.clone());


    if method == "post"
    {
        _router.post(path,   move |r: &mut Request| Server::_handler(r, m.clone(), handler.clone(), rp.clone()), path.clone());
    } else if method == "put" {
        _router.put(path,    move |r: &mut Request| Server::_handler(r, m.clone(), handler.clone(), rp.clone()), path.clone());
    } else if method == "get" {
        _router.get(path,    move |r: &mut Request| Server::_handler(r, m.clone(), handler.clone(), rp.clone()), path.clone());
    } else if method == "delete" {
        _router.delete(path, move |r: &mut Request| Server::_handler(r, m.clone(), handler.clone(), rp.clone()), path.clone());
    } else {
        _router.patch(path,  move |r: &mut Request| Server::_handler(r, m.clone(), handler.clone(), rp.clone()), path.clone());
    }
}


mod config
{
    use super::*;

    pub fn routes() -> router::Router
    {
        let mut _router = router::Router::new();

        router_add_path(&mut _router, "/new_queue", "post", &Handler::ClientAndFormdata(&qgatawey::new_queue),     Some(vec!["name"]));
        router_add_path(&mut _router, "/",          "get",  &Handler::Empty(            &qgatawey::full_map),      None);
        router_add_path(&mut _router, "/sub",       "post", &Handler::ClientAndFormdata(&qgatawey::sub),           Some(vec!["name"]));
        router_add_path(&mut _router, "/unsub",     "post", &Handler::ClientAndFormdata(&qgatawey::unsub),         Some(vec!["name"]));
        router_add_path(&mut _router, "/pub",       "post", &Handler::ClientAndFormdata(&qgatawey::_pub),          Some(vec!["name"]));
        router_add_path(&mut _router, "/unpub",     "post", &Handler::ClientAndFormdata(&qgatawey::unpub),         Some(vec!["name"]));
        router_add_path(&mut _router, "/push",      "post", &Handler::ClientAndFormdata(&qgatawey::push_in_queue), Some(vec!["name", "data"]));
        router_add_path(&mut _router, "/user_log",  "get",  &Handler::OnlyClient(       &qgatawey::get_user_log),  None);
        
        return _router;
    }
}

fn main()
{
    Server::new(String::from("localhost"), 1000, config::routes());
}