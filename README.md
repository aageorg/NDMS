# Queue broker with REST interface

The pre-interview testing task

### Usage

#### Install

Download, build and start application as a daemon

```
$ git clone https://github.com/aageorg/NDMS
$ go build -o qubro
$ ./qubro --port 8080 &
```

#### Put to queue

Call curl to put something to queue with any name

```
curl -XPUT http://127.0.0.1:8080/pet?v=dog
curl -XPUT http://127.0.0.1:8080/car?v=volvo
...
```

#### Get from queue

```
curl http://127.0.0.1:8080/pet => dog
curl http://127.0.0.1:8080/pet => {empty body + 404 (not found)}
curl http://127.0.0.1:8080/car => volvo
```

You also can use a timeout parameter in seconds. If the value will come to queue, while client is waiting, it will be sent to client immediatelly


```
curl http://127.0.0.1:8080/pet?timeout=2
```

#### Challenge

Try to run multiple PUT ang GET requests at the same time. The application works stable.
