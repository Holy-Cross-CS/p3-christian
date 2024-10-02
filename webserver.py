#!/usr/bin/env python3

# Author: K. Walsh <kwalsh@cs.holycross.edu>
# Date: 15 January 2015
# Updated: 17 September 2020 - update to python3, add classes
# Updated: 15 September 2022 - bug fixes

# A simple web server from scratch in Python. Run it like this:
#   python3 webserver.py  localhost  8888
# or:
#   ./webserver.py  localhost  8888
#
# The two parameters are the hostname of the server, and the port number to
# listen on. An optional third parameter specifies a the server root directory.
# - For the port number, use any number between 1024 and 49151 that is not being
#   used by another program. Port 80 would be ideal for HTTP, as it is the
#   default used by web browsers, but using port numbers below 1024 requires
#   administrator privileges.
# - For the server hostname, using "localhost" or "127.0.0.1" will ensure that
#   your server is only accessible to browsers on your own machine, and not from
#   attackers trying to access your server from over the internet. Using an
#   ampty string "" for the server hostname would allow all connections, even
#   from outside attackers.
# - By default, files from the "./web_root" directory will be served to clients.
#   If you want to serve files from a different directory, you can specify this
#   as a third argument, for example:
#       python3 webserver.py localhost 8888 ~/Desktop/Stuff
#
# Note: This code is not "pythonic" at all; there are much more concise ways to
# write this code by using various python features like dicts and string
# interpolation. We also avoid use of any modules except for the following very
# basic things:

import os  # for file and directory stuff, like os.path.isfile()
import socket  # for socket stuff
import sys  # for sys.argv and sys.exit()
import urllib.parse  # for urllib.parse.unquote() and urllib.parse.unquote_plus()
import time  # for time of day and date functions
import threading  # for concurrent threads and locks
import random  # for random numbers
import re  # for regex split() to split up strings
import string  # for various string operations
from dataclasses import dataclass
from dataclasses import dataclass

# Global configuration variables.
# These never change once the server has finished initializing, so they don't
# need any special protection even if used concurrently.
server_host = None  # e.g. localhost, 127.0.0.1, logos.holycross.edu, or similar
server_port = None  # e.g. 8888 or similar
server_root = "./web_root"
server_ip = None
banned_user_agents = [
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36,gzip(gfe)",
]

hello_count = 0


@dataclass
class Topic:
    name: str
    messages_count: int = 0
    likes: int = 0


topic_version: int = 0

topics: list[Topic] = [Topic("whatever", 7, 2), Topic("holycross", 15, 6)]


# Global variables to keep track of statistics, with initial values. These get
# updated by different connection handler threads. To avoid concurrency
# problems, these must only be accessed within a "with" block, like this:
#     x = ...
#     with stats.lock:
#        stats.tot_time += x
#        if x > stats.max_time:
#            stats.max_time = x
#        ...
class Statistics:
    def __init__(self):
        self.lock = threading.Condition()  # protects all variables below
        self.total_connections = 0
        self.active_connections = 0
        self.num_requests = 0
        self.num_errors = 0
        self.max_time = 0  # max time spent handling a request
        self.tot_time = 0  # total time spent handling requests
        self.avg_time = 0  # average time spent handling requests


stats = Statistics()


# Request objects are used to hold information associated with a single HTTP
# request from a client.
class Request:
    def __init__(self):
        self.method = ""  # GET, POST, PUT, etc. for this request
        self.path = ""  # url path for this request
        self.version = ""  # http version for this request
        self.headers = []  # headers from client for this request
        self.length = 0  # length of the request body, if any
        self.body = None  # contents of the request body, if any


# Response objects are used to hold information associated with a single HTTP
# response that will be sent to a client. The code is required, and should be
# something like "200 OK" or "404 NOT FOUND". The mime_type and body are
# optional. If present, the mime_type should be something like "text/plain" or
# "image/png", and the body should be a string or bytes object containing
# contents appropriate for that mime type.
class Response:
    def __init__(self, code, mime_type=None, body=None):
        self.code = code  # example: "200 OK"
        self.mime_type = mime_type  # example: "image.png"
        self.body = body  # a bytes object, or a string
        self.cookies = None  # a list of name=value strings (optional)


# Helper function to check if a string looks like a common IPv4 address. Note:
# This is intentionally picky, only accepting the most common
# 4-numbers-with-dots notation, to avoid likely user input errors.
def isTypicalIPv4Address(s):
    parts = s.split(".")
    try:
        return len(parts) == 4 and all(0 <= int(p) < 256 for p in parts)
    except ValueError:
        return False


# SocketError objects represent errors that can occur with sockets.
class SocketError:
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "Socket Error: " + self.msg


# ERR_SOCKET_WAS_CLOSED means the other side unexpectedly closed the connection.
ERR_SOCKET_WAS_CLOSED = SocketError("Connection Closed")
# ERR_SOCKET_HAD_TIMEOUT means it's been a long time the other side sent data.
ERR_SOCKET_HAD_TIMEOUT = SocketError("Read Timeout")
# ERR_SOCKET_HAD_ERROR means something unknown went wrong.
ERR_SOCKET_HAD_ERROR = SocketError("Read/Write Failure")

# This variable controls how long the server is willing to wait for data from a
# client. If set to None, the server will wait indefinitely.
SOCKET_TIMEOUT = (
    None  # Or use 10.0 to give up after 10 seconds waiting for data from client.
)


# Connection objects are used to hold information associated with a single HTTP
# connection, like the socket for the connection, the client's IP address,
# statistics specific to that connection, any leftover data from the client that
# hasn't yet been processed, etc.
class Connection:
    def __init__(self, connected_socket, addr):
        self.sock = connected_socket  # the socket connected to the client
        self.client_addr = addr  # IP address of the client
        self.leftover_data = b""  # data from client, not yet processed
        self.num_requests = 0  # number of requests from client handled so far
        self.start_time = time.time()  # time connection was established
        self.last_active_time = time.time()  # time connection was last used

    # wait_until_data_arrives() examines the socket and waits until some data
    # has arrived from the client. Normally, this function returns None, but if
    # something goes wrong, this function instead returns:
    # - ERR_SOCKET_HAD_TIMEOUT if a timeout occurs before data arrives,
    # - ERR_SOCKET_WAS_CLOSED if the socket was closed before any data arrives,
    # - ERR_SOCKET_HAD_ERROR if some other error is encountered.
    def wait_until_data_arrives(self):
        if len(self.leftover_data) > 0:
            return None
        try:
            # Set the timeout value, if present, to prevent infinite waiting.
            if SOCKET_TIMEOUT is not None:
                self.sock.settimeout(SOCKET_TIMEOUT)
            # Read (up to) another 4KB of data from the client
            more_data = self.sock.recv(4096)
            if not more_data:  # Connection has died?
                log("Client %s closed the socket." % (str(self.client_addr)))
                return ERR_SOCKET_WAS_CLOSED
            self.leftover_data = self.leftover_data + more_data
            return None
        except socket.timeout as err:
            log(
                "Client %s has not sent data in %s seconds."
                % (str(self.client_addr), SOCKET_TIMEOUT)
            )
            return ERR_SOCKET_HAD_TIMEOUT
        except:
            log("Error reading from client %s socket" % (str(self.client_addr)))
            return ERR_SOCKET_HAD_ERROR
        finally:
            # Remove timeout, if present, so future operations are unaffected.
            if SOCKET_TIMEOUT is not None:
                self.sock.settimeout(None)

    # read_until_blank_line() returns data from the client up to (but not
    # including) the next blank line, i.e. "\r\n\r\n". The "\r\n\r\n" sequence
    # is discarded. Any leftovers after the blank line is saved for later. This
    # function returns one of the ERR_SOCKET values if an error is encountered.
    def read_until_blank_line(self):
        data = self.leftover_data
        try:
            # Set the timeout value, if present, to prevent infinite waiting.
            if SOCKET_TIMEOUT is not None:
                self.sock.settimeout(SOCKET_TIMEOUT)
            # Keep reading until we get a blank line.
            while b"\r\n\r\n" not in data:
                # Read (up to) another 4KB of data from the client
                more_data = self.sock.recv(4096)
                if not more_data:  # Connection has died?
                    log("Client %s closed the socket." % (self.client_addr))
                    self.leftover_data = data  # save it all for later?
                    return ERR_SOCKET_WAS_CLOSED
                data = data + more_data
            # The part we want is everything up to the first blank line.
            data, self.leftover_data = data.split(b"\r\n\r\n", 1)
            return data.decode()
        except socket.timeout as err:
            log(
                "Client %s has not sent data in %s seconds."
                % (self.client_addr, SOCKET_TIMEOUT)
            )
            self.leftover_data = data  # save it all for later?
            return ERR_SOCKET_HAD_TIMEOUT
        except:
            log("Error reading from client %s socket" % (self.client_addr))
            self.leftover_data = data  # save it all for later?
            return ERR_SOCKET_HAD_ERROR
        finally:
            # Remove timeout, if present, so future operations are unaffected.
            if SOCKET_TIMEOUT is not None:
                self.sock.settimeout(None)

    # read_amount(n) returns the next n bytes of data from the client. Any
    # leftovers after the n bytes are saved for later. This function returns
    # None if an error is encountered. It does not use timeouts, but instead
    # will wait indefinitely for enough data to arrive.
    def read_amount(self, n):
        data = self.leftover_data
        try:
            while len(data) < n:
                more_data = self.sock.recv(n - len(data))
                if not more_data:  # Connection has died?
                    self.leftover_data = data  # save it all for later
                    return None
                data = data + more_data
            # The part we want is the first n bytes.
            data, self.leftover_data = (data[0:n], data[n:])
            return data.decode()
        except:
            log("Error reading from client %s socket" % (self.client_addr))
            self.leftover_data = data  # save it all for later
            return None


# log(msg) prints a message to standard output. Since multi-threading can jumble
# up the order of output on the screen, we print out the current thread's name
# on each line of output along with the message.
# Example usage:
#   log("Hello %s, you are customer number %d, have a nice day!" % (name, n))
# You can also use python's f-strings instead of the modulo operator:
#   log(f"Hello {name}, you are customer number {n}, have a nice day!")
def log(msg):
    # Convert msg to a string, if it is not already
    if not isinstance(msg, str):
        msg = str(msg)
    # Each python thread has a name. Use current thread's in the output message.
    myname = threading.current_thread().name
    # When printing multiple lines, indent each line a bit
    # indent = (" " * len(myname))
    indent = "    "
    linebreak = "\n" + indent + ": "
    lines = msg.splitlines()
    msg = linebreak.join(lines)
    # Print it all out, prefixed by this thread's name.
    print(myname + ": " + msg)


# get_header_value() finds a specific header value from within a list of header
# key-value pairs. If the requested key is not found, None is returned instead.
# The headers list comes from an HTTP request sent from the client. The key
# should usually be a standard HTTP header, like "Content-Type",
# "Content-Length", "Connection", etc. This will properly handle upper-case,
# lower-case, and mixed-case header names.
def get_header_value(headers, key):
    for hdr in headers:
        if hdr.lower().startswith(key.lower() + ": "):
            val = hdr.split(" ", 1)[1]
            return val
    return None


# get_cookies() returns the entire "Cookie" header, or None if it's not present.
def get_cookies(headers):
    vals = get_header_value(headers, "Cookie")
    return vals


# get_cookie() extracts the value of a specific named cookie from "Cookie"
# header, if the header is present and there is a matching "name=value" pair in
# that header. This returns None if there was no header, or the header didn't
# have the given "name=value" pair. The name is case-sensitive.
def get_cookie(headers, name):
    vals = get_header_value(headers, "Cookie")
    if vals == None:
        return None
    # There are many possible formats for the Cookie header...
    # - a single named value: "name=something"
    # - a single named value: "name1=value1; name2=value2; name3=value3"
    # - a single unnamed value: "something"
    pairs = vals.split("; ")  # separator is exactly one semi-colon and one space
    for pair in pairs:
        keyval = pair.split("=", 1)
        if len(keyval) == 2:
            # If this part is like "name1=value1", use those as key and val
            key = keyval[0].rstrip()
            val = keyval[1].lstrip()
        else:
            # If this part is like "value1", key is blank, entire thing is val
            key = ""
            val = keyval.lstrip()
        if key == name:
            return val
    return None


def make_html_form(data) -> str:
    # ref: https://www.w3schools.com/html/html_forms.asp
    form = "<form method='POST' action='/hello'>"
    for key, value in data.items():
        form += f"{key}: <input type='text' name='{key}' value='{value}'><br>"
    form += "<input type='submit' value='Greet Me!'></form>"
    return form


# make_printable() does some substitutions on a string so that it prints nicely
# on the console while still showing unprintable characters (like "\r" or "\n")
# in a sensible way.
printable = string.ascii_letters + string.digits + string.punctuation + " \r\n\t"


def make_printable(s):
    if isinstance(s, bytes):  # if s is raw binary...
        try:
            s = s.decode()
        except:
            return "{binary data, %d bytes total, not shown here}\n" % (len(s))
    if not isinstance(s, str):  # if s is not a string...
        body = str(s)  # ... convert to string
    s = s.replace("\n", "\\n\n")
    s = s.replace("\r", "\\r")
    s = s.replace("\t", "\\t")
    return "".join(c if c in printable else r"\x{0:02x}".format(ord(c)) for c in s)


# handle_one_http_request() reads one HTTP request from the client, parses it,
# decides what to do with it, then sends an appropriate response back to the
# client.
def handle_one_http_request(conn):
    # The HTTP request is everything up to the first blank line
    data = conn.read_until_blank_line()
    if data == ERR_SOCKET_WAS_CLOSED:
        # Client disconnected... that's fine, nothing more to do here.
        return  # caller will close socket
    if data == ERR_SOCKET_HAD_TIMEOUT:
        # Client is not sending requests... let's close the connection.
        log("Connection has been idle more than %s seconds, closing immediately.")
        return  # caller will close socket
    if data == ERR_SOCKET_HAD_ERROR:
        # Unknown error... let's close the connection.
        return  # caller will close socket

    conn.last_active_time = time.time()

    log(
        "Request %d has arrived...\n%s"
        % (conn.num_requests, make_printable(data + "\r\n\r\n"))
    )

    # Make a Request object to hold all the info about this request
    req = Request()

    # The first line is the request-line, the rest is the headers.
    lines = data.splitlines()
    if len(lines) == 0:
        log("Request is missing the required HTTP request-line")
        resp = Response("400 BAD REQUEST", "text/plain", "You need a request-line!")
        send_http_response(conn, resp)
        return
    request_line = lines[0]  # first line is the request line
    req.headers = lines[1:]  # remaining lines are the headers

    # The request-line can be further split into method, path, and version.
    words = request_line.split()
    if len(words) != 3:
        log("The request-line is malformed: '%s'" % (request_line))
        resp = Response(
            "400 BAD REQUEST", "text/plain", "Your request-line is malformed!"
        )
        send_http_response(conn, resp)
        return
    req.method = words[0]
    req.path = words[1]
    req.version = words[2]

    if get_header_value(req.headers, "User-Agent") in banned_user_agents:
        log("User agent is banned")
        resp = Response("403 FORBIDDEN", "text/plain", "User-Agent is banned")
        send_http_response(conn, resp)
        return

    log(
        "Request has method=%s, path=%s, version=%s, and %d headers"
        % (req.method, req.path, req.version, len(req.headers))
    )

    # The path will look like either "/foo/bar" or "/foo/bar?key=val&baz=boo..."
    # Unmangle any '%'-signs in the path, but just the part before any '?'-mark
    if "?" in req.path:
        req.path, params = req.path.split("?", 1)
        req.path = urllib.parse.unquote(req.path) + "?" + params
    else:
        req.path = urllib.parse.unquote(req.path)

    # Browsers that use chunked transfer encoding are tricky, don't bother.
    if get_header_value(req.headers, "Transfer-Encoding") == "chunked":
        log("The request uses chunked transfer encoding, which isn't yet supported")
        resp = Response(
            "411 LENGTH REQUIRED",
            "text/plain",
            "Your request uses chunked transfer encoding, sorry!",
        )
        send_http_response(conn, resp)

        return

    # If request has a Content-Length header, get the body of the request.
    n = get_header_value(req.headers, "Content-Length")
    if n is not None:
        req.length = int(n)
        req.body = conn.read_amount(int(n))

    cookies = {
        "name": get_cookie(req.headers, "name"),
        "favcolor": get_cookie(req.headers, "favcolor"),
    }

    # Finally, look at the method and path to decide what to do.
    if req.method == "GET":
        resp = handle_http_get(req, conn, cookies)
    elif req.method == "POST":
        resp = handle_http_post(req, conn)
    elif req.method == "PUT":
        log("HTTP method '%s' is not yet supported by this server" % (req.method))
        resp = Response(
            "405 METHOD NOT ALLOWED",
            "text/plain",
            "PUT method not yet supported",
        )
    else:
        log("HTTP method '%s' is not recognized by this server" % (req.method))
        resp = Response(
            "405 METHOD NOT ALLOWED", "text/plain", "Unrecognized method: " + req.method
        )

    # Now send the response to the client.
    send_http_response(conn, resp)
    return get_header_value(req.headers, "Connection") == "keep-alive"


# send_http_response() sends an HTTP response to the client. The response code
# should be something like "200 OK" or "404 NOT FOUND". The mime_type and body
# are sent as the contents of the response.
def send_http_response(conn, resp):
    # If this is anything other than code 200, tally it as an error.
    if not resp.code.startswith("200 "):
        with stats.lock:  # update overall server statistics
            stats.num_errors += 1
    # Make a response-line and all the necessary headers.
    data = "HTTP/1.1 " + resp.code + "\r\n"
    data += "Server: csci356\r\n"
    data += (
        "Date: "
        + time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(time.time()))
        + "\r\n"
    )

    if resp.cookies != None:
        # set cookies to expire in 1 week
        expiration = time.strftime(
            "%a, %d %b %Y %H:%M:%S %Z", time.gmtime(time.time() + 7 * 24 * 60 * 60)
        )
        for cookie in resp.cookies:
            data += "Set-Cookie: " + cookie + "; Expires=" + expiration + "\r\n"

    body = None
    if resp.mime_type == None:
        data += "Content-Length: 0\r\n"
    else:
        if isinstance(resp.body, bytes):  # if response body is raw binary...
            body = resp.body  # ... no need to encode it
        elif isinstance(resp.body, str):  # if response body is a string...
            body = resp.body.encode()  # ... convert to raw binary
        else:  # if response body is anything else...
            body = str(resp.body).encode()  # ... convert it to raw binary
        data += "Content-Type: " + resp.mime_type + "\r\n"
        data += "Content-Length: " + str(len(body)) + "\r\n"

    data += "\r\n"

    # Send response-line, headers, and body
    log("Sending response-line and headers...\n%s" % (make_printable(data)))
    conn.sock.sendall(data.encode())
    if body is not None:
        log(
            "Response body (not shown) has %d bytes, mime type '%s'"
            % (len(body), resp.mime_type)
        )
        # If you want to see the body in the console, uncomment this next line
        log("\n====BEGIN BODY====\n" + make_printable(body) + "=====END BODY====")
        conn.sock.sendall(body)


def handle_http_post_hello(req: Request, conn):
    global hello_count
    hello_count += 1
    # Parse the request body
    body = urllib.parse.parse_qs(req.body)
    name = body.get("Enter your name", [""])[0]
    color = body.get("Favorite color", [""])[0]
    cookies = {"name": name, "favcolor": color}
    return handle_http_get_hello(req, conn, cookies)


def handle_http_post(req, conn):
    if req.path == "/hello":
        resp = handle_http_post_hello(req, conn)
    else:
        resp = Response(
            "405 METHOD NOT ALLOWED",
            "text/plain",
            "Post method not yet supported",
        )
    return resp


# handle_http_get_status() returns a response for GET /status
def handle_http_get_status(conn):
    log("Handling http get status request")
    msg = "Web server for csci 356, version 0.1\n"
    msg += "\n"
    msg += "Server Statistics:\n"
    with stats.lock:
        msg += str(stats.total_connections) + " connections in total\n"
        msg += str(stats.active_connections) + " active connections\n"
        msg += str(stats.num_requests) + " requests handled\n"
        msg += str(stats.num_errors) + " errors encountered\n"
        msg += "%.3f ms average request handling time\n" % (stats.avg_time * 1000)
        msg += "%.3f ms slowest request handling time\n" % (stats.max_time * 1000)
    msg += "\n"
    msg += "Connection Statistics:\n"
    msg += str(conn.num_requests) + " requests handled on this connection so far\n"
    msg += "%.3f s elapsed since start of this connection\n" % (
        time.time() - conn.start_time
    )
    return Response("200 OK", "text/plain", msg)


def handle_http_get_whisper_topics(req: Request, conn: Connection):
    log("Handling http get whisper topics request")
    version = int(req.path.split("=")[-1])
    msg = f"{version}\n"

    while topic_version < version:
        pass

    for topic in topics:
        msg += f"{topic.messages_count} {topic.likes} {topic.name}\n"

    return Response("200 OK", "text/plain", msg)


# handle_http_get_hello() returns a response for GET /hello
def handle_http_get_hello(req, conn, cookies):
    log("Handling http get hello request")
    global hello_count
    hello_count += 1
    name = cookies.get("name", None)
    favcolor = cookies.get("favcolor", None)
    name = name if name else "You"
    favcolor = favcolor if favcolor else "teal"

    msg = "<html><head><title>Hello!</title></head>"
    msg += "<body>"
    msg += f"<h1>Welcome, {name}!</h1>"
    msg += f"This page has been visited {hello_count} times.</br>"
    msg += "The current date is %s.</br>" % (time.strftime("%a, %d %b %Y %H:%M:%S %Z"))
    msg += "You have visited this page an unknown numbrer of times before.</br>"
    msg += f"Maybe your favorite color is... {favcolor}??</br>"
    msg += "Hit page refresh (F5) to refresh this page and see the contents MOSTLY STAY THE SAME??</br>"
    msg += "You can also go to these exciting pages:</br>"
    endpoints = {
        "/hello": "this page",
        "/status": "status and statistics",
        "/quote": "a random quote",
        "/welcome.html": "a welcome page",
        "/index.html": "some fancy web page",
        "/whoami": "information about you",
    }
    msg += "<ul>"
    for endpoint, description in endpoints.items():
        msg += f'<li><a href="http://{server_host}:{server_port}{endpoint}">{description}</a></li>'
    msg += "</ul>"
    name_ = ""
    favcolor_ = ""
    data = {"Enter your name": name_, "Favorite color": favcolor_}
    msg += make_html_form(data)
    msg += "</body></html>"
    resp = Response("200 OK", "text/html", msg)
    resp.cookies = [f"name={name}; Path=/", f"favcolor={favcolor}; Path=/"]
    return resp


def handle_http_get_whoami(req, conn):
    msg = "<html><head><title>Who Am I!</title></head>"
    msg += "<body>"
    msg += f"<h1>Who Am I!</h1>"
    msg += f"<h3>Your Request</h3>"
    msg += f"{str(req.method)}</br>"
    msg += f"{str(req.path)}</br>"
    msg += f"{str(req.version)}</br>"
    msg += "Headers:</br>"
    msg += "<ul>"
    for header in req.headers:
        msg += f"<li>{header}</li>"
    msg += "</ul>"
    msg += f"<h3>Your Connection Information</h3>"
    msg += f"Client Address: {str(conn.client_addr)}</br>"
    msg += f"Number of Requests: {str(conn.num_requests)}</br>"
    start_time = time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(conn.start_time))
    msg += f"Start Time: {start_time}</br>"
    last_active_time = time.strftime(
        "%a, %d %b %Y %H:%M:%S %Z", time.gmtime(conn.last_active_time)
    )
    msg += f"Last Active Time: {last_active_time}</br>"
    msg += "</body></html>"
    return Response("200 OK", "text/html", msg)


# handle_http_get_quote() returns a response for the GET /quote
def handle_http_get_quote():
    log("Handling http get quote request")
    with open("quotations.txt") as f:
        quotes = re.split("(?m)^%$", f.read())
    msg = "<html><head><title>Quotes!</title></head>"
    msg += "<body>"
    msg += "<p>Here is a randomly generated quote from"
    msg += '  <a href="https://www.cs.cmu.edu/~pattis/quotations.html">Richard Pattis\' page</a> at CMU.'
    msg += "<pre>%s</pre>" % (random.choice(quotes))
    msg += '<p>Hit page refresh (F5) or <a href="/quote">click here</a> to refresh this page.</p>'
    msg += '<p>You can also check the <a href="/status">server status</a>, '
    msg += (
        '  a <a href="/index.html">copy of the Holy Cross home page or something</a>, '
    )
    msg += "</body></html>"
    return Response("200 OK", "text/html", msg)


def handle_directory_listing(path):
    log("Handling directory listing")
    msg = "<html><head><title>Directory Listing!</title></head>"
    msg += "<body>"
    msg += "<ul>"
    for item in os.listdir(server_root + path):
        msg += f'<li><a href="{path}/{item}">{item}</a></li>'
    msg += "</ul>"
    msg += "</body></html>"
    return Response("200 OK", "text/html", msg)


# handle_http_get_file() returns an appropriate response for a GET request that
# seems to be for a file, rather than a special URL. If the file can't be found,
# or if there are any problems, an error response is generated.
def handle_http_get_file(url_path):
    log("Handling http get file request, for " + url_path)

    file_path = server_root + url_path

    # There is a very real security risk that the requested file_path could
    # include things like "..", allowing a malicious or curious client to access
    # files outside of the server's web_root directory. We take several
    # precautions here to make sure that there is no funny business going on.

    # First security precaution: "normalize" to eliminate ".." elements
    file_path = os.path.normpath(file_path)

    if os.path.isdir(file_path):
        if os.path.isfile(file_path + "/index.html"):
            file_path = file_path + "/index.html"
        else:
            return handle_directory_listing(url_path)

    # Second security precaution: make sure the requested file is in server_root
    if os.path.commonprefix([file_path, server_root]) != server_root:
        log("Path traversal attack detected: " + url_path)
        return Response("403 FORBIDDEN", "text/plain", "Permission denied: " + url_path)

    # Third security precaution: check if the path is actually a file
    if not os.path.isfile(file_path):
        log("File was not found: " + file_path)
        return Response("404 NOT FOUND", "text/plain", "No such file: " + url_path)

    # Finally, attempt to read data from the file, and return it
    try:
        with open(file_path, "rb") as f:  # "rb" mode means read "raw bytes"
            data = f.read()

        if file_path.endswith(".html"):
            mime_type = "text/html"
        elif file_path.endswith(".css"):
            mime_type = "text/css"
        elif file_path.endswith(".js"):
            mime_type = "application/javascript"
        elif file_path.endswith(".png"):
            mime_type = "image/png"
        elif file_path.endswith(".jpg") or file_path.endswith(".jpeg"):
            mime_type = "image/jpeg"
        elif file_path.endswith(".gif"):
            mime_type = "image/gif"
        elif file_path.endswith(".ico"):
            mime_type = "image/x-icon"
        else:
            mime_type = "text/plain"

        return Response("200 OK", mime_type, data)
    except:
        log("Error encountered reading from file")
        return Response("403 FORBIDDEN", "text/plain", "Permission denied: " + url_path)


# handle_http_get() returns an appropriate response for a GET request
def handle_http_get(req, conn, cookies):
    # Generate a response
    if req.path == "/status":
        resp = handle_http_get_status(conn)
    elif req.path == "/hello":
        resp = handle_http_get_hello(req, conn, cookies)
    elif req.path == "/quote":
        resp = handle_http_get_quote()
    elif req.path == "/whoami":
        resp = handle_http_get_whoami(req, conn)
    elif req.path.split("/")[1] == "whisper":
        resp = handle_http_get_whisper_topics(req, conn)
    else:
        resp = handle_http_get_file(req.path)
    return resp


# handle_http_connection() reads one or more HTTP requests from a client, parses
# each one, and sends back appropriate responses to the client.
def handle_http_connection(conn):
    with stats.lock:  # update overall server statistics
        stats.active_connections += 1
    log("Handling connection from " + str(conn.client_addr))
    try:
        keep_alive = True
        while keep_alive:
            # Process one HTTP request from client
            start = time.time()
            keep_alive = handle_one_http_request(conn)
            end = time.time()
            duration = end - start

            # Do end-of-request statistics and cleanup
            conn.num_requests += 1  # counter for this connection
            log(
                "Done handling request %d from %s"
                % (conn.num_requests, conn.client_addr)
            )
            with stats.lock:  # update overall server statistics
                stats.num_requests += 1
                stats.tot_time = stats.tot_time + duration
                stats.avg_time = stats.tot_time / stats.num_requests
                if duration > stats.max_time:
                    stats.max_time = duration
    finally:
        conn.sock.close()
        log("Done with connection from " + str(conn.client_addr))
        with stats.lock:  # update overall server statistics
            stats.active_connections -= 1


# This remainder of this file is the main program, which listens on a server
# socket for incoming connections from clients, and starts a handler thread for
# each one.

# Get command-line parameters
if len(sys.argv) not in [3, 4]:
    print("This program expects 2 or 3 arguments.")
    print("  python3 webserver.py  SERVER_HOSTNAME  SERVER_PORTNUM [SERVER_ROOT_DIR]")
    print("For example:")
    print("  python3 webserver.py  localhost  8888")
    print("  python3 webserver.py  127.0.0.1  8000")
    print("  python3 webserver.py  logos.holycross.edu  9001")
    print("  python3 webserver.py  192.133.83.134  8765")
    print("The optional last argument specifies the server root directory:")
    print("  python3 webserver.py  localhost  8123  ./testing/my_files/")
    print("If the last argument is omitted, then '" + server_root + "'")
    print("  will be used as the server root directory.")
    sys.exit(1)
server_host = sys.argv[1]
server_port = int(sys.argv[2])
if len(sys.argv) >= 4:
    server_root = sys.argv[3]

# Ensure root path has a slash at the end
server_root = os.path.normpath(server_root + "/")

# Determine the IP address for listening
if isTypicalIPv4Address(server_host):
    server_ip = server_host
else:
    try:
        short_name = server_host.split(".")[0]
        server_ip = socket.gethostbyname(short_name)
    except:
        print("Could not determine IP address for listening.")
        sys.exit(1)


# Print a welcome message
log("Starting web server.")
log(f"Serving files from directory {server_root}")
log(f"Attempting to listen at IP address {server_ip} port {server_port}")

# Create the server welcoming socket, and set it up to listen for connections
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_addr = (server_host, server_port)
s.bind(server_addr)
s.listen(5)

log("Server can be accessed at URLs such as:")
log(f"    http://{server_host}:{server_port}/")
log(f"    http://{server_host}:{server_port}/welcome.html")
log(f"    http://{server_host}:{server_port}/status.html")
log("Ready for connections...")

try:
    # Repeatedly accept and handle connections
    while True:
        sock, client_addr = s.accept()
        # A new client socket connection has been accepted. Count it.
        with stats.lock:
            stats.total_connections += 1
        # Put the info into a Connection object.
        conn = Connection(sock, client_addr)
        # Start a thread to handle the new connection.
        t = threading.Thread(target=handle_http_connection, args=(conn,))
        t.daemon = True
        t.start()
finally:
    log("Shutting down...")
    s.close()

log("Done")
