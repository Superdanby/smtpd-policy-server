package main

import (
    "bufio"
    "container/heap"
    "crypto/tls"
    "flag"
    "fmt"
    "io"
    "io/ioutil"
    "encoding/json"
    "math"
    "net"
    "net/http"
    "strings"
    "sync"
    "time"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value * string // The value of the item; arbitrary.
	priority int64    // The priority of the item in the queue.
}

type APIError struct {
    Error string `json:"Error"`
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue [] * Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func respond(smtpd_request *map[string]string, writer *bufio.Writer, api_client *(http.Client), response_queue *PriorityQueue, receive_cnt int64, transmit_cnt *int64, lock *sync.Mutex, client_semaphore chan int64) {
    // Wait for the results from proton_api and send it to the client, requests are labelled with receive_cnt and pushed into a min heap to ensure FIFO order

    // build API request
    req := "https://localhost/api/mail/incoming/recipient?Email=" + (*smtpd_request)["recipient"]
    // fmt.Println(req)

    // get API request
    response := ""
    resp, err := api_client.Get(req)
    if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
        fmt.Println(err)
        response = "action=DEFER connection timeout\n\n"
    } else if err != nil {
        fmt.Println(err)
        response = "action=DEFER connection error\n\n"
    }
    if response == "" {
        defer resp.Body.Close()
        // print(resp.StatusCode)

        // build smtpd response
        var api_error APIError
        if resp.StatusCode == 200 || resp.StatusCode == 204 {
            response = "action=OK\n\n"
        } else if resp.StatusCode == 422 {
            err = json.NewDecoder(resp.Body).Decode(&api_error)
            if err != nil {
                panic("Decode Error field failed!")
            }
            response = "action=REJECT " + api_error.Error + "\n\n"
        } else if resp.StatusCode == 400 {
            response = "action=REJECT " + resp.Status[4:] + "\n\n"
        } else if resp.StatusCode == 503 {
            response = "action=DEFER " + resp.Status[4:] + "\n\n"
        } else {
            err = json.NewDecoder(resp.Body).Decode(&api_error)
            if err != nil {
                fmt.Println("Decode Error field failed!")
                response = "action=REJECT " + resp.Status[4:] + "\n\n"
            } else {
                response = "action=REJECT " + api_error.Error + "\n\n"
            }
        }
        io.Copy(ioutil.Discard, resp.Body) // discard remaining messages in order to reuse the connection
    }

    // prepare to send back the smtpd response
    lock.Lock() // ensure only 1 goroutine perform actions on response_queue
    heap.Push(response_queue, & Item{&response, receive_cnt})
    for len(*response_queue) > 0 && *transmit_cnt == (*response_queue)[0].priority {
        leaving := heap.Pop(response_queue).(*Item)
        fmt.Println(*(leaving.value))
        _, err := writer.WriteString(*(leaving.value))
        if err != nil {
            panic("Write failed!")
        }
        writer.Flush()
        *transmit_cnt += 1
    }
    lock.Unlock()
    client_semaphore <- *transmit_cnt
}

func handleConnection(conn net.Conn, api_client *http.Client, client_semaphore chan int64) {
    // This function reads request data from the client after a connection is established. After reading one request, it will initiate the respond() function to process the request and send it back, but instead of waiting respond() to finish its task, this function will continue to read the next request immediately.
    defer conn.Close()
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    defer writer.Flush()
    var response_queue PriorityQueue // min heap
    var receive_cnt, transmit_cnt int64
    var lock sync.Mutex // only 1 goroutine can modify the min heap

    // get and process new requests
    for {
        if receive_cnt == math.MaxInt64 {
            fmt.Println("Max receive count reached. Reseting connection.")
            return
        }
        <- client_semaphore // limit buffered requests

        // build smtpd request dictionary
        smtpd_request := make(map[string]string) // smtpd request
        for {
            smtpd_field, err := reader.ReadString('\n')
            if err == io.EOF {
                fmt.Println("Reader EOF reached")
                return
            } else if err != nil {
                fmt.Println("Client connection failure")
                return
            }
            if smtpd_field == "\n" { // smtpd requests stop with an empty line: http://postfix.cs.utah.edu/SMTPD_POLICY_README.html
                break
            }
            key_val := strings.SplitN(smtpd_field, "=", 2)
            smtpd_request[key_val[0]] = strings.TrimSuffix(key_val[1], "\n")
        }
        // fmt.Println(smtpd_request)

        go respond(&smtpd_request, writer, api_client, &response_queue, receive_cnt, &transmit_cnt, &lock, client_semaphore)
        receive_cnt += 1
    }
}

func main() {
    server_socket_ptr := flag.String("socket", "127.0.0.1:8888", "Socket to listen on. Default: 127.0.0.1:8888")
    max_conns_per_host_ptr := flag.Int("max_per_host", 10, "Max connections per host. Default: 10")
    max_req_buf := flag.Int64("max_req_buf", 100, "Max requests buffered from all clients. More requests will be read after the number of buffered requests goes down. Default: 100")
    flag.Parse()

    // setup tcp socket listener
    ln, err := net.Listen("tcp", *server_socket_ptr)
    if err != nil {
        fmt.Println(err)
    	panic("Cannot listen on socket:" + *server_socket_ptr + "\n")
    }
    tr := &http.Transport{
        MaxIdleConns: *max_conns_per_host_ptr,
    	MaxIdleConnsPerHost: *max_conns_per_host_ptr,
        MaxConnsPerHost: *max_conns_per_host_ptr,
    	IdleConnTimeout: 300 * time.Second,
    	// DisableCompression: true,
        TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        // add read timeout
    }
    api_client := &http.Client{Transport: tr, Timeout: time.Duration(5 * time.Second)}

    // clinet buffer
    client_semaphore := make (chan int64, *max_req_buf)
    for i := int64(0); i < *max_req_buf; i++ {
        client_semaphore <- 0
    }

    // accept connections
    for {
    	conn, err := ln.Accept()
        if err != nil {
            fmt.Println(err)
        	fmt.Println("Cannot accept client connection")
            continue
        }
    	go handleConnection(conn, api_client, client_semaphore)
    }
}
