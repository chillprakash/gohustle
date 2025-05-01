package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	// Create a simple file server to serve our test HTML
	fs := http.FileServer(http.Dir("."))
	
	// Serve the test_cors.html file
	http.Handle("/", fs)
	
	fmt.Println("CORS Debug server running at http://localhost:8000")
	fmt.Println("Open http://localhost:8000/test_cors.html in your browser")
	
	// Start the server on a different port than your main API
	log.Fatal(http.ListenAndServe(":8000", nil))
}
