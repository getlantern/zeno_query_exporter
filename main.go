package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/getlantern/zenodb/rpc"
)

var (
	configPath = flag.String("config", "config.yml", "The path of the config file")
	zenoAddr   = flag.String("zenoaddr", "", "The ZenoDB address to which to connect with gRPC over TLS")
	password   = flag.String("password", "", "The password used to authenticate against ZenoDB server")
	addr       = flag.String("addr", "", "The address to which the exporter HTTP service listens on")
	path       = flag.String("path", "/metrics", "The HTTP path to export the metrics")
)

var config *Config
var client rpc.Client

func main() {
	flag.Parse()
	checkFlags()
	var err error
	config, err = loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	client, err = createClient(*zenoAddr, *password)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle(*path, http.HandlerFunc(handleMetrics))
	fmt.Printf("Starting Zeno Query Exporter at %s\n", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func checkFlags() {
	if *zenoAddr == "" {
		log.Fatal("Missing zenoaddr")
	}
	if *password == "" {
		log.Fatal("Missing password")
	}
	if *addr == "" {
		log.Fatal("Missing addr")
	}
}

func loadConfig(path string) (*Config, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(b, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func handleMetrics(rw http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get("job")
	if name == "" {
		rw.WriteHeader(http.StatusBadRequest)
		io.WriteString(rw, "missing job parameter")
		return
	}
	job, exists := config.Jobs[name]
	if !exists {
		rw.WriteHeader(http.StatusNotFound)
		io.WriteString(rw, "job not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runJob(ctx, client, job, rw)
}
