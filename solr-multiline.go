package solr

import (
	//"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	//"sync"
	"time"
	"io/ioutil"
	"encoding/json"
	"log"

	docker "github.com/fsouza/go-dockerclient"

	"github.com/gliderlabs/logspout/router"
	"github.com/rtt/Go-Solr"
)


func init() {
	router.AdapterFactories.Register(NewSolrAdapter, "solr")
}


// SolrAdapter is an adapter that streams multiline to Logstash.
type SolrAdapter struct {
	conn           *solr.Connection
	route          *router.Route
	containerTags  map[string][]string
	logstashFields map[string]map[string]string
	decodeJsonLogs map[string]bool
}

// NewSolrAdapter returnas a configured solr.Adapter
func NewSolrAdapter(route *router.Route) (a router.LogAdapter, err error) {
	hostname := os.Getenv("SOLR_HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}

	solr_port := 8983
	port := os.Getenv("SOLR_PORT")
	if port != "" {
		solr_port, err = strconv.Atoi(port)
		if err != nil {
			return nil, err
		}
	}

	solr_collectionname := "collection1"
	collectionname := os.Getenv("SOLR_COLLECTIONNAME")
	if collectionname != "" {
		solr_collectionname = collectionname
	}

	solrconn, err := solr.Init(hostname, solr_port, solr_collectionname)
	if err != nil {
		return nil, err
	}

	return &SolrAdapter{
		conn:       solrconn,
		route:      route,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
	}, nil

}

// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *SolrAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	tags := []string{}
	tagsStr := os.Getenv("LOGSTASH_TAGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tagsStr = strings.TrimPrefix(e, "LOGSTASH_TAGS=")
			break
		}
	}

	if len(tagsStr) > 0 {
		tags = strings.Split(tagsStr, ",")
	}

	a.containerTags[c.ID] = tags
	return tags
}

// Get logstash fields configured with the environment variable LOGSTASH_FIELDS
func GetLogstashFields(c *docker.Container, a *SolrAdapter) map[string]string {
	if fields, ok := a.logstashFields[c.ID]; ok {
		return fields
	}

	fieldsStr := os.Getenv("LOGSTASH_FIELDS")
	fields := map[string]string{}

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_FIELDS=") {
			fieldsStr = strings.TrimPrefix(e, "LOGSTASH_FIELDS=")
		}
	}

	if len(fieldsStr) > 0 {
		for _, f := range strings.Split(fieldsStr, ",") {
			sp := strings.Split(f, "=")
			k, v := sp[0], sp[1]
			fields[k] = v
		}
	}

	a.logstashFields[c.ID] = fields

	return fields
}

// Get boolean indicating whether json logs should be decoded (or added as message),
// configured with the environment variable DECODE_JSON_LOGS
func IsDecodeJsonLogs(c *docker.Container, a *SolrAdapter) bool {
	if decodeJsonLogs, ok := a.decodeJsonLogs[c.ID]; ok {
		return decodeJsonLogs
	}

	decodeJsonLogsStr := os.Getenv("DECODE_JSON_LOGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "DECODE_JSON_LOGS=") {
			decodeJsonLogsStr = strings.TrimPrefix(e, "DECODE_JSON_LOGS=")
		}
	}

	decodeJsonLogs := decodeJsonLogsStr != "false"

	a.decodeJsonLogs[c.ID] = decodeJsonLogs

	return decodeJsonLogs
}

// Get hostname of container, searching first for /etc/host_hostname, otherwise
// using the hostname assigned to the container (typically container ID).
func GetContainerHostname(c *docker.Container) string {
	content, err := ioutil.ReadFile("/etc/host_hostname")
	if err == nil && len(content) > 0 {
		return strings.Trim(string(content), "\r\n")
	}

	return c.Config.Hostname
}

// Stream sends log data to the next adapter
func (a *SolrAdapter) Stream(logstream chan *router.Message) { //nolint:gocyclo
	for m := range logstream {
		log.Println("stream:", m)
		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: GetContainerHostname(m.Container),
		}

		// Check if we are sending logs for this container
		if !containerIncluded(dockerInfo.Name) {
			continue
		}

		if os.Getenv("DOCKER_LABELS") != "" {
			dockerInfo.Labels = make(map[string]string)
			for label, value := range m.Container.Config.Labels {
				dockerInfo.Labels[strings.Replace(label, ".", "_", -1)] = value
			}
		}

		tags := GetContainerTags(m.Container, a)
		fields := GetLogstashFields(m.Container, a)

		var js []byte
		var data map[string]interface{}
		var err error

		// Try to parse JSON-encoded m.Data. If it wasn't JSON, create an empty object
		// and use the original data as the message.
		if IsDecodeJsonLogs(m.Container, a) {
			err = json.Unmarshal([]byte(m.Data), &data)
		}
		if err != nil || data == nil {
			data = make(map[string]interface{})
			data["message"] = m.Data
		}

		for k, v := range fields {
			data[k] = v
		}

		data["docker"] = dockerInfo
		data["stream"] = m.Source
		data["tags"] = tags

		// Return the JSON encoding
		if js, err = json.Marshal(data); err != nil {
			// Log error message and continue parsing next line, if marshalling fails
			log.Println("logstash: could not marshal JSON:", err)
			continue
		}

	       log.Println("data:", js)
		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		for {
			// build an update document, in this case adding two documents
			f := map[string]interface{}{
				"add": []interface{}{
					map[string]interface{}{"id": time.Now().Unix(), "data": js},
				},
			}
			// send off the update (2nd parameter indicates we also want to commit the operation)
			_, err := a.conn.Update(f, true)
			if err == nil {
				break
			}

			if os.Getenv("RETRY_SEND") == "" {
				log.Fatal("logstash: could not write:", err)
			} else {
				time.Sleep(2 * time.Second)
			}
			// if err != nil {
			// 	fmt.Println("error =>", err)
			// } else {
			// 	fmt.Println("resp =>", resp)
			// }
			// _, err := a.conn.U(js)

			// if err == nil {
			// 	break
			// }

			// if os.Getenv("RETRY_SEND") == "" {
			// 	log.Fatal("logstash: could not write:", err)
			// } else {
			// 	time.Sleep(2 * time.Second)
			// }
		}
	}
}


// containerIncluded Returns true if this container is in INCLUDE_CONTAINERS, or not env var is set
func containerIncluded(inputContainerName string) bool {
	if includeContainers := os.Getenv("INCLUDE_CONTAINERS"); includeContainers != "" {
		for _, containerName := range strings.Split(includeContainers, ",") {
			if inputContainerName == containerName {
				// This contain is included, send this log
				return true
			}
		}
		return false
	} else if includeContainers := os.Getenv("INCLUDE_CONTAINERS_REGEX"); includeContainers != "" {
		compiled, err := regexp.Compile(includeContainers)
		if err != nil {
			// Return true by default
			return true
		}
		if compiled.MatchString(inputContainerName) {
			// This contain is included, send this log
			return true
		}
		return false
	}
	return true
}

type DockerInfo struct {
	Name     string            `json:"name"`
	ID       string            `json:"id"`
	Image    string            `json:"image"`
	Hostname string            `json:"hostname"`
	Labels   map[string]string `json:"labels"`
}
