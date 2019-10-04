package main

import (
    "bufio"
    "flag"
    "fmt"
    "io"
    "math/rand"
    "net"
    "os"
    "sync"
    "testing"
    "time"
)

type info struct {
    socket string
}

var testing_info info

var OK_req = `request=smtpd_access_policy
protocol_state=RCPT
protocol_name=SMTP
helo_name=some.domain.tld
queue_id=8045F2AB23
sender=foo@bar.tld
recipient=bart@protonmail.dev
recipient_count=0
client_address=1.2.3.4
client_name=another.domain.tld
reverse_client_name=another.domain.tld
instance=123.456.7
sasl_method=plain
sasl_username=you
sasl_sender=
size=12345
ccert_subject=solaris9.porcupine.org
ccert_issuer=Wietse+20Venema
ccert_fingerprint=C2:9D:F4:87:71:73:73:D9:18:E7:C2:F3:C1:DA:6E:04
encryption_protocol=TLSv1/SSLv3
encryption_cipher=DHE-RSA-AES256-SHA
encryption_keysize=256
etrn_domain=
stress=

`

var OK_resp = `action=OK

`

var REJECT_req = `request=smtpd_access_policy
protocol_state=RCPT
protocol_name=SMTP
helo_name=some.domain.tld
queue_id=8045F2AB23
sender=foo@bar.tld
recipient=bar@foo.tld
recipient_count=0
client_address=1.2.3.4
client_name=another.domain.tld
reverse_client_name=another.domain.tld
instance=123.456.7
sasl_method=plain
sasl_username=you
sasl_sender=
size=12345
ccert_subject=solaris9.porcupine.org
ccert_issuer=Wietse+20Venema
ccert_fingerprint=C2:9D:F4:87:71:73:73:D9:18:E7:C2:F3:C1:DA:6E:04
encryption_protocol=TLSv1/SSLv3
encryption_cipher=DHE-RSA-AES256-SHA
encryption_keysize=256
etrn_domain=
stress=

`

var REJECT_resp = `action=REJECT

`

func TestOk(t *testing.T) {
    conn, err := net.Dial("tcp", testing_info.socket)
    if err != nil {
        fmt.Println("Cannot establish connection to server at: " + testing_info.socket)
    }
    defer conn.Close()
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    defer writer.Flush()
    _, werr := writer.WriteString(OK_req)
    if werr != nil {
        panic("Write failed!")
    }
    writer.Flush()

    var response string

    for {
        line, rerr := reader.ReadString('\n')
        if rerr == io.EOF {
        fmt.Println("Reader EOF reached")
        } else if rerr != nil {
            panic("Read failed!")
        }
        response += line
        if line == "\n" {
            break
        }
    }
    if response != OK_resp {
        t.Errorf("OK request got %s; want %s", response, OK_resp)
    }
}

func TestREJECT(t *testing.T) {
    conn, err := net.Dial("tcp", testing_info.socket)
    if err != nil {
        fmt.Println("Cannot establish connection to server at: " + testing_info.socket)
    }
    defer conn.Close()
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    defer writer.Flush()
    _, werr := writer.WriteString(REJECT_req)
    if werr != nil {
        panic("Write failed!")
    }
    writer.Flush()

    var response string

    for {
        line, rerr := reader.ReadString('\n')
        if rerr == io.EOF {
        fmt.Println("Reader EOF reached")
        } else if rerr != nil {
            panic("Read failed!")
        }
        response += line
        if line == "\n" {
            break
        }
    }
    if response[:13] != REJECT_resp[:13] || response[len(response) - 2:] != REJECT_resp[len(REJECT_resp) - 2:] {
        t.Errorf("OK request got %s; want %s ... %s", response, REJECT_resp[:6], REJECT_resp[6:])
    }
}

func TestSingleConnectionOrder(t *testing.T) {
    conn, err := net.Dial("tcp", testing_info.socket)
    if err != nil {
        fmt.Println("Cannot establish connection to server at: " + testing_info.socket)
    }
    defer conn.Close()
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    defer writer.Flush()
    requests := []string{OK_req, REJECT_req}
    rand.Seed(time.Now().UTC().UnixNano())
    repeat := 5
    quantity := 10
    for i := 0; i < repeat; i++ {
        var indices [] int
        for j := 0; j < quantity; j++ {
            idx := rand.Intn(len(requests))
            indices = append(indices, idx)
            _, werr := writer.WriteString(requests[idx])
            if werr != nil {
                panic("Write failed!")
            }
            writer.Flush()
        }
        for j := 0; j < quantity; j++ {
            var response string
            for {
                line, rerr := reader.ReadString('\n')
                if rerr == io.EOF {
                    fmt.Println("Reader EOF reached")
                } else if rerr != nil {
                    panic("Read failed!")
                }
                response += line
                if line == "\n" {
                    break
                }
            }
            switch indices[j] {
            case 0:
                if response != OK_resp {
                    t.Errorf("OK request got %s; want %s", response, OK_resp)
                }
            case 1:
                if response[:13] != REJECT_resp[:13] || response[len(response) - 2:] != REJECT_resp[len(REJECT_resp) - 2:] {
                    t.Errorf("OK request got %s; want %s ... %s", response, REJECT_resp[:6], REJECT_resp[6:])
                }
            default:
                panic("index of indices[]string out of range: " + string(indices[j]))
            }
        }
    }
}

func TestMultiConnectionOrder(t *testing.T) {
    connections := 5
    repeat := 1
    quantity := 10
    var wg sync.WaitGroup
    wg.Add(connections)
    for k := 0; k < connections; k++ {
        go func () {
            defer wg.Done()
            conn, err := net.Dial("tcp", testing_info.socket)
            if err != nil {
                fmt.Println("Cannot establish connection to server at: " + testing_info.socket)
            }
            defer conn.Close()
            reader := bufio.NewReader(conn)
            writer := bufio.NewWriter(conn)
            defer writer.Flush()
            requests := []string{OK_req, REJECT_req}
            rand.Seed(time.Now().UTC().UnixNano())
            for i := 0; i < repeat; i++ {
                var indices [] int
                for j := 0; j < quantity; j++ {
                    idx := rand.Intn(len(requests))
                    indices = append(indices, idx)
                    _, werr := writer.WriteString(requests[idx])
                    if werr != nil {
                        panic("Write failed!")
                    }
                    writer.Flush()
                }
                for j := 0; j < quantity; j++ {
                    var response string
                    for {
                        line, rerr := reader.ReadString('\n')
                        if rerr == io.EOF {
                            fmt.Println("Reader EOF reached")
                        } else if rerr != nil {
                            panic("Read failed!")
                        }
                        response += line
                        if line == "\n" {
                            break
                        }
                    }
                    switch indices[j] {
                    case 0:
                        if response != OK_resp {
                            t.Errorf("OK request got %s; want %s", response, OK_resp)
                        }
                    case 1:
                        if response[:13] != REJECT_resp[:13] || response[len(response) - 2:] != REJECT_resp[len(REJECT_resp) - 2:] {
                            t.Errorf("OK request got %s; want %s ... %s", response, REJECT_resp[:6], REJECT_resp[6:])
                        }
                    default:
                        panic("index of indices[]string out of range: " + string(indices[j]))
                    }
                }
            }
        }()
    }
    wg.Wait()
}

func TestMain(m *testing.M) {
    server_socket_ptr := flag.String("socket", "127.0.0.1:8888", "Socket to listen on.")
    flag.Parse()
    testing_info.socket = *server_socket_ptr

	os.Exit(m.Run())
}
