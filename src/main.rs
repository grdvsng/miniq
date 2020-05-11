extern crate iron;
extern crate json;
extern crate router;
extern crate typed_html;
#[macro_use]
extern crate mime;
#[macro_use]
extern crate lazy_static;

use iron::prelude::*;
use iron::status;
use iron::Iron;
use std::sync::{mpsc, Arc, Mutex};
use router::Router;
use std::collections::HashMap;
use std::io::prelude::*;
use std::fmt;
use std::time::{SystemTime, Duration};
use std::thread;
use chrono::offset::Utc;
use chrono::DateTime;


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


lazy_static!
{
    pub static ref QUEUES: Arc<Mutex<HashMap<String, Arc<Mutex<Queue>>>>> = Arc::new(Mutex::new(HashMap::new()));
}


struct Server
{
    host:   String, 
    port:   u64,
}

impl Server
{
    fn new (host:String, port: u64) -> Server
    {
        Server 
        {
            host:   host, 
            port:   port
        }
    }

    fn is_formdata_not_valide(formdata: json::JsonValue, require_params: Vec<&str>) -> Option<String>
    {
        for param in &require_params
        {
            if formdata[*param].is_null()
            {
                return Some(format!(r#""{}" is require."#, param));
            }
        }

        return None;
    }

    fn get_json_data(_request: &mut Request, require_params: Vec<&str>) -> Result<json::JsonValue, json::JsonValue>
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
    
    fn queues_to_json() -> json::JsonValue
    {
        let mut data = json::object!{};

        for (k, v) in Arc::clone(&QUEUES).lock().unwrap().clone()
        {
            data[k] = json::JsonValue::from((&*v.clone().lock().unwrap()).clone());
        }

        return data;
    }

    fn queue_insert(queue: Queue) -> IronResult<Response>
    {
        let (tr, rx) = mpsc::channel();
        
        thread::spawn(move || {
                
            if !QUEUES.lock().unwrap().contains_key(&*queue.name.clone())
            {
                QUEUES.lock().unwrap().insert(queue.name.clone(), Arc::new(Mutex::new(queue)));

                tr.send(Ok(Self::queues_to_json())).unwrap();
            } else {
                tr.send(Err(format!("\"{}\" allready exists", queue.name.clone()))).unwrap();
            }
        });

        return Self::_json_response_finalize(rx);
    }

    fn new_queue(request: &mut Request) -> IronResult<Response>
    {
        match Self::get_json_data(request, vec!["name"])
        {
            Err(error)   => Ok(Response::json(error, status::BadRequest)),
            Ok(formdata) =>
            {
                let queue = Queue::new(format!("{}", formdata["name"]), Client::new(format!("{}", request.remote_addr.ip()), request.remote_addr.port() as u64));   
                
                return Self::queue_insert(queue);
            }
        }
    }

    fn _json_response_finalize(rx: std::sync::mpsc::Receiver<Result<json::JsonValue, String>>) -> IronResult<Response> 
    {
        match rx.recv().unwrap()
        {
            Ok(queues) => Ok(Response::json(queues, status::Ok)),
            Err(txt)   => Ok(Response::json(json::object!{"error" => txt}, status::BadRequest)),
        }
    }

    fn queue_sub(queue_name: String, client: Client) -> IronResult<Response>
    {
        let (tr, rx) = mpsc::channel();
        
        thread::spawn(move || {
               
            match QUEUES.lock().unwrap().get_mut(&*queue_name.clone())
            {
                Some(q) =>
                {
                    let mut queue = (**&q.lock().unwrap()).clone();

                    match queue.sub(client)
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

        return Self::_json_response_finalize(rx);
    }

    
    fn queue_pub(queue_name: String, client: Client) -> IronResult<Response>
    {
        let (tr, rx) = mpsc::channel();
        
        thread::spawn(move || {
               
            match QUEUES.lock().unwrap().get_mut(&*queue_name.clone())
            {
                Some(q) =>
                {
                    let mut queue = (**&q.lock().unwrap()).clone();

                    match queue.add_publisher(client)
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

        return Self::_json_response_finalize(rx);
    }

    fn queue_unsub(queue_name: String, client: Client) -> IronResult<Response>
    {
        let (tr, rx) = mpsc::channel();
        
        thread::spawn(move || {
               
            match QUEUES.lock().unwrap().get_mut(&*queue_name.clone())
            {
                Some(q) =>
                {
                    let mut queue = (**&q.lock().unwrap()).clone();

                    match queue.unsub(client)
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

        return Self::_json_response_finalize(rx);
    }

    fn queue_unpub(queue_name: String, client: Client) -> IronResult<Response>
    {
        let (tr, rx) = mpsc::channel();
        
        thread::spawn(move || {
               
            match QUEUES.lock().unwrap().get_mut(&*queue_name.clone())
            {
                Some(q) =>
                {
                    let mut queue = (**&q.lock().unwrap()).clone();

                    match queue.remove_publisher(client)
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

        return Self::_json_response_finalize(rx);
    }

    fn request_analizator(request: &mut Request, reqiered_paras: Vec<&str>, callback: &dyn Fn(json::JsonValue)->IronResult<Response>) -> IronResult<Response>
    {
        match Self::get_json_data(request, reqiered_paras)
        {
            Err(error)   => Ok(Response::json(error, status::BadRequest)),
            Ok(formdata) => callback(formdata),
        }
    }
    
    fn sub(request: &mut Request) -> IronResult<Response>
    {
        let client = Client::new(format!("{}", request.remote_addr.ip()), request.remote_addr.port() as u64);

        Self::request_analizator(request, vec!["name"], &|formdata: json::JsonValue| Self::queue_sub(format!("{}", formdata["name"]), client.clone()))
    }

    fn unsub(request: &mut Request) -> IronResult<Response>
    {
        let client = Client::new(format!("{}", request.remote_addr.ip()), request.remote_addr.port() as u64);

        Self::request_analizator(request, vec!["name"], &|formdata: json::JsonValue| Self::queue_unsub(format!("{}", formdata["name"]), client.clone()))
    }

    fn _pub(request: &mut Request) -> IronResult<Response>
    {
        let client = Client::new(format!("{}", request.remote_addr.ip()), request.remote_addr.port() as u64);

        Self::request_analizator(request, vec!["name"], &|formdata: json::JsonValue| Self::queue_pub(format!("{}", formdata["name"]), client.clone()))
    }

    fn unpub(request: &mut Request) -> IronResult<Response>
    {
        let client = Client::new(format!("{}", request.remote_addr.ip()), request.remote_addr.port() as u64);

        Self::request_analizator(request, vec!["name"], &|formdata: json::JsonValue| Self::queue_unpub(format!("{}", formdata["name"]), client.clone()))
    }

    fn _push_in_queue(formdata: json::JsonValue, client: Client) -> IronResult<Response>
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

        return Self::_json_response_finalize(rx);
    }

    fn push_in_queue(request: &mut Request) -> IronResult<Response>
    {
        let client = Client::new(format!("{}", request.remote_addr.ip()), request.remote_addr.port() as u64);

        Self::request_analizator(request, vec!["name", "data"], &|formdata: json::JsonValue| Self::_push_in_queue(formdata, client.clone()))
    }

    pub fn run(self)
    {
        let mut router = Router::new();

        router.post("/new_queue", Self::new_queue,     "new_queue");
        router.post("/sub",       Self::sub,           "sub");
        router.post("/unsub",     Self::unsub,         "unsub");
        router.post("/pub",       Self::_pub,          "pub");
        router.post("/unpub",     Self::unpub,         "unpub");
        router.post("/push",      Self::push_in_queue, "push");
        router.get("/", |_: &mut Request| {IronResult::Ok(Response::json(Self::queues_to_json(), status::Ok))}, "full_map");
        Iron::new(router).http(format!("{}:{}", self.host, self.port)).unwrap();
    }   
}


fn main() 
{
    let app = Server::new(String::from("localhost"), 1000);

    app.run();
}