# smtpd-policy-server

## Prerequisite

`Go 1.12`

## Execution

`go run server.go`

### Arguments

```shell=
-max_per_host int
      Max connections per host. (default 10)
-max_req_buf int
      Max requests buffered from all clients. More requests will be read after the number of buffered requests goes down. (default 100)
-socket string
      Socket to listen on. (default "127.0.0.1:8888")
```

## Testing

`go test -v` or `go test -v -socket [socket(defalut: 127.0.0.1:8888)]`
