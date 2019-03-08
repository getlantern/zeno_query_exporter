package main

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/getlantern/zenodb/rpc"
)

var (
	jobsPath = flag.String("jobspath", ".", "The path of the job definitions. *.yaml files under the path will be loaded.")
	zenoAddr = flag.String("zenoaddr", "", "The ZenoDB address to which to connect with gRPC over TLS")
	password = flag.String("password", "", "The password used to authenticate against ZenoDB server")
	addr     = flag.String("addr", "", "The address to which the exporter HTTP service listens on")
	strict   = flag.Bool("strict", true, "if specified, raises error when there are missing data from 1 or more partitions")
)

var jobs map[string]Job = make(map[string]Job)
var client rpc.Client

const defaultJobTimeout = 3 * time.Minute

func main() {
	flag.Parse()
	checkFlags()
	var err error
	err = loadJobs(*jobsPath)
	if err != nil {
		log.Fatal(err)
	}
	client, err = createClient(*zenoAddr, *password)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/metrics", http.HandlerFunc(handleMetrics))
	log.Printf("Starting Zeno Query Exporter at %s", *addr)
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

func loadJobs(jobsPath string) error {
	files, err := filepath.Glob(path.Join(jobsPath, "*.yaml"))
	if err != nil {
		return err
	}
	for _, p := range files {
		fname := path.Base(p)
		name := strings.Split(fname, ".")[0]
		b, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}
		var job Job
		err = yaml.Unmarshal(b, &job)
		if err != nil {
			return err
		}
		log.Printf("Loaded job '%s'", name)
		jobs[name] = job
	}
	return nil
}

func handleMetrics(rw http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	name := query.Get("job")
	if name == "" {
		rw.WriteHeader(http.StatusBadRequest)
		io.WriteString(rw, "job not specified\n")
		return
	}
	job, exists := jobs[name]
	if !exists {
		rw.WriteHeader(http.StatusNotFound)
		io.WriteString(rw, "job not found\n")
		return
	}
	timeout, err := time.ParseDuration(query.Get("timeout"))
	if err != nil {
		timeout = defaultJobTimeout
	}
	query.Del("job")
	query.Del("timeout")
	params := make(map[string]string)
	for k, l := range query {
		params[k] = l[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err = runJob(ctx, client, job, params, rw)
	if err != nil {
		select {
		case <-ctx.Done():
			rw.WriteHeader(http.StatusGatewayTimeout)
			log.Printf("Job '%s' timed out", name)
		default:
			rw.WriteHeader(http.StatusInternalServerError)
			log.Printf("Job '%s' failed: %v", name, err)
		}
	}
}
