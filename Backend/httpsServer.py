from http import server

class myHandler(server.SimpleHTTPRequestHandler):
    def do_GET(self):
        print("doing GET!")
        self.send_response(200, "Hello")
        self.end_headers()
        print("Headers ended")
        self.wfile.write("World!".encode("utf-8"))
        return "World!"


class myServer(server.HTTPServer):
    


    @property
    def dbconnection(self):
        if self._dbconnection is None:
            self._dbconnection = DBConnection()
        return self.dbconnection



if __name__ == "__main__":
    PORT = 8000
    with server.HTTPServer(("", PORT), myHandler) as httpd:
        print("Starting Server")
        httpd.serve_forever()