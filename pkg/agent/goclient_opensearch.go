package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/pprof/profile"
	"github.com/joho/godotenv"
	opensearch "github.com/opensearch-project/opensearch-go"
	opensearchapi "github.com/opensearch-project/opensearch-go/opensearchapi"
)

var IsInsecure = false
var GlobalStoreAddress = ""
var TheClient *opensearch.Client

const IndexName = "parca_agent_profile"

type Client struct {
	Name       string `json:"name"`
	Value      string `json:"value"`
	Periodtype string `json:"periodtype"`
	Period     int64  `json:"period"`
	Time       int64  `json:"time"`
	Duration   string `json:"duration"`
	Samples    string `json:"samples"`
	Locations  string `json:"locations"`
	Mappings   string `json:"mappings"`
	Functions  string `json:"functions"`
}

func GoCreateClient(theStoreAddress string) {
	//check if it starts with http or https and then append if not there
	if theStoreAddress[0:1] == "h" {
		GlobalStoreAddress = theStoreAddress
	} else {
		GlobalStoreAddress = "http://" + theStoreAddress
	}

	//Load the Username and Password
	envErr := godotenv.Load(".env")
	if envErr != nil {
		log.Print("Could not load .env file\n")
	}

	//fmt.Printf("The address is %s\n", GlobalStoreAddress)
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: IsInsecure},
		},
		Addresses: []string{GlobalStoreAddress},
		Username:  os.Getenv("OPENSEARCH_USERNAME"),
		Password:  os.Getenv("OPENSEARCH_PASSWORD"),
	})
	//Make it a .env file located at top of directory
	//OPENSEARCH_USERNAME=??
	//OPENSEARCH_PASSWORD=??
	//fmt.Printf("InSecure setto %s\n", IsInsecure)
	//fmt.Printf("Opensearch Username is %s\n", os.Getenv("OPENSEARCH_USERNAME"))
	//fmt.Printf("Opensearch Password is %s\n", os.Getenv("OPENSEARCH_PASSWORD"))
	TheClient = client

	if err != nil {
		log.Fatalf("Error creating the OpenSearch client")
		//fmt.Println("Error creating the OpenSearch client", err)
	}
	// Print OpenSearch version information on console.
	log.Println(client.Info())

	// Define index mapping.
	mapping := strings.NewReader(`{
		"mappings": {
			"properties": {
				"name": { "type" : "keyword" },
				"value": { "type" : "keyword" },
				"periodtype": { "type" : "keyword" },
				"period": { "type" : "integer" },
				"time": { "type" : "date" },
				"duration": { "type" : "float" },
				"samples": { "type" : "keyword"},
				"locations": { "type" : "keyword" },
				"mappings": { "type" : "keyword" },
				"functions": { "type" : "keyword" }
			}
		}
	}`)

	//Create an index with non-default settings.
	//opensearch if exist request see if Index name
	resp, err := opensearchapi.IndicesExistsRequest{
		Index: []string{IndexName},
	}.Do(context.Background(), TheClient)
	//fmt.Printf("The responce is %s\n", resp)
	//If status code is 404 then it does not exist already
	if resp.StatusCode == 404 {
		req := opensearchapi.IndicesCreateRequest{
			Index: IndexName,
			Body:  mapping,
		}
		res, err := req.Do(context.Background(), TheClient)
		fmt.Println("creating index", res)
		if err != nil {
			log.Fatalf("failed to create index")
			//fmt.Println("failed to create index ", err)
		}
	}
}

//Create global variable for client or pass it in. create client only once
func GoClientWrite(key string, value string, prof *profile.Profile) error {
	clients := Client{
		"theName",
		"theValue",
		"ThePeriodType",
		0,
		0,
		"01.01",
		"sample",
		"location",
		"mapping",
		"function",
	}

	clients.Name = key
	clients.Value = value
	if pt := prof.PeriodType; pt != nil {
		clients.Periodtype = fmt.Sprintf("%s %s", pt.Type, pt.Unit)
	}
	clients.Period = prof.Period
	clients.Time = prof.TimeNanos / 1000000 //To convert time from nanos
	clients.Duration = fmt.Sprintf("%.4v", time.Duration(prof.DurationNanos))

	//clients.Samples = fmt.Sprintln(prof.Sample)
	clients.Samples = fmt.Sprintf("%s", prof.Sample)
	clients.Locations = fmt.Sprintf("%s", prof.Location)
	clients.Mappings = fmt.Sprintf("%s", prof.Mapping)
	clients.Functions = fmt.Sprintf("%s", prof.Function)

	//finalJson, err := json.Marshal(clients)
	finalJson, err := json.MarshalIndent(clients, "", "\t")
	if err != nil {
		panic(err)
	}
	log.Printf("%s\n", finalJson) // Printing to check

	//Add a document to the index.
	document := bytes.NewReader(finalJson)

	req := opensearchapi.IndexRequest{
		Index: IndexName,
		Body:  document,
	}
	insertResponse, err := req.Do(context.Background(), TheClient)

	if err != nil {
		log.Fatal(insertResponse)
		//fmt.Println("failed to insert document ", err)
	}
	//fmt.Println(insertResponse)
	return err
}
