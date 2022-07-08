package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/pprof/profile"
	opensearch "github.com/opensearch-project/opensearch-go"
	opensearchapi "github.com/opensearch-project/opensearch-go/opensearchapi"
)

var GlobalStoreAddress = ""
var TheClient *opensearch.Client

const IndexName = "go-test-profiling"

type Client struct {
	Name       string `json:"name"`
	Value      string `json:"value"`
	Periodtype string `json:"periodtype"`
	Period     int64  `json:"period"`
	Time       string `json:"time"`
	Duration   string `json:"duration"`
	Samples    string `json:"samples"`
	Locations  string `json:"locations"`
	Mappings   string `json:"mappings"`
}

//openseachusername and opensearch password
//use as authentication credentials
//sample
func GoCreateClient(theStoreAddress string) {
	// Initialize the client with SSL/TLS enabled.
	//check if it starts with http or https and then append if not there
	GlobalStoreAddress = "http://" + theStoreAddress
	fmt.Printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
	fmt.Printf("The address is %s\n", GlobalStoreAddress)
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{GlobalStoreAddress},
		//Username:  "admin",
		//Password:  "admin",
	})
	TheClient = client
	//replace with logger
	if err != nil {
		fmt.Println("Error creating the OpenSearch client", err)
	}
	// Print OpenSearch version information on console.
	//remove once used elsewhere
	fmt.Println(client.Info())

	// Define index mapping.
	mapping := strings.NewReader(`{
		'settings': {
		  'index': {
			   'number_of_shards': 4
			   }
			 }
		}`)
	// Create an index with non-default settings.
	//create only once after making client.
	res := opensearchapi.IndicesCreateRequest{
		Index: IndexName,
		Body:  mapping,
	}
	fmt.Println("creating index", res)
}

//Create global variable for client or pass it in. create client only once
func GoClientTest(key string, value string, prof *profile.Profile) {
	// mapping := strings.NewReader(`{
	// 	"mappings": {
	// 		"properties": {
	// 			"name":         { "type" : "keyword" },
	// 			"value":        { "type" : "keyword" },
	// 			"periodtype":   { "type" : "keyword" },
	// 			"period":       { "type" : "integer" },
	// 			"time":   	    { "type" : "date" },
	// 			"duration":     { "type" : "float" },
	// 			"samples":      { "type" : "object"},
	// 			"locations": 	{ "type" : "object" },
	// 			"mappings": 	{ "type" : "object" }
	// 		}
	// 	}
	// 	}`)

	clients := Client{
		"theName",
		"theValue",
		"ThePeriodType",
		100,
		"TheTime",
		"43.43",
		"sample",
		"location",
		"mapping",
	}

	clients.Name = key
	clients.Value = value
	if pt := prof.PeriodType; pt != nil {
		clients.Periodtype = fmt.Sprintf("%s %s", pt.Type, pt.Unit)
	}
	clients.Period = prof.Period
	clients.Time = fmt.Sprintf("%v", time.Unix(0, prof.TimeNanos))
	clients.Duration = fmt.Sprintf("%.4v", time.Duration(prof.DurationNanos))
	clients.Samples = fmt.Sprintf("%s", prof.Sample)
	clients.Locations = fmt.Sprintf("%s", prof.Location)
	clients.Mappings = fmt.Sprintf("%s", prof.Mapping)

	//finalJson, err := json.Marshal(clients)
	finalJson, err := json.MarshalIndent(clients, "", "\t")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", finalJson) // Printing to check

	// Add a document to the index.
	document := bytes.NewReader(finalJson)

	req := opensearchapi.IndexRequest{
		Index: IndexName,
		Body:  document,
	}
	insertResponse, err := req.Do(context.Background(), TheClient)

	if err != nil {
		fmt.Println("failed to insert document ", err)
	}
	fmt.Println(insertResponse)
}
