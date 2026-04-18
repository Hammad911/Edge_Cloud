// Command kvsmoke is a minimal CLI for exercising the KV gRPC API from the
// shell. It is deliberately tiny so it can double as a Milestone 3 smoke
// test without pulling in grpcurl.
//
// Usage:
//
//	kvsmoke -addr 127.0.0.1:7001 put foo bar
//	kvsmoke -addr 127.0.0.1:7001 get foo
//	kvsmoke -addr 127.0.0.1:7001 del foo
//	kvsmoke -addr 127.0.0.1:7001 bench 100
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "edge-cloud-replication/gen/proto/edgecloud/kv/v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7001", "KV gRPC endpoint")
	timeout := flag.Duration("timeout", 5*time.Second, "per-RPC timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		die("dial: %v", err)
	}
	defer conn.Close()

	cli := kvv1.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "put":
		if len(args) != 3 {
			die("usage: kvsmoke put <key> <value>")
		}
		resp, err := cli.Put(ctx, &kvv1.PutRequest{Key: args[1], Value: []byte(args[2])})
		if err != nil {
			die("put: %v", err)
		}
		fmt.Printf("OK token=%s\n", fmtToken(resp.GetToken()))
	case "get":
		if len(args) != 2 {
			die("usage: kvsmoke get <key>")
		}
		resp, err := cli.Get(ctx, &kvv1.GetRequest{Key: args[1]})
		if err != nil {
			die("get: %v", err)
		}
		if !resp.GetFound() {
			fmt.Printf("NOT FOUND token=%s\n", fmtToken(resp.GetToken()))
			return
		}
		fmt.Printf("OK value=%q token=%s\n", resp.GetValue(), fmtToken(resp.GetToken()))
	case "del":
		if len(args) != 2 {
			die("usage: kvsmoke del <key>")
		}
		resp, err := cli.Delete(ctx, &kvv1.DeleteRequest{Key: args[1]})
		if err != nil {
			die("del: %v", err)
		}
		fmt.Printf("OK token=%s\n", fmtToken(resp.GetToken()))
	case "bench":
		n := 100
		if len(args) == 2 {
			_, err := fmt.Sscanf(args[1], "%d", &n)
			if err != nil {
				die("bench: invalid count %q", args[1])
			}
		}
		runBench(cli, n)
	default:
		usage()
		os.Exit(2)
	}
}

func runBench(cli kvv1.KVClient, n int) {
	start := time.Now()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := cli.Put(ctx, &kvv1.PutRequest{
			Key:   fmt.Sprintf("bench/%06d", i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		cancel()
		if err != nil {
			die("bench put %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("put %d keys in %s (%.1f/s)\n", n, elapsed, float64(n)/elapsed.Seconds())
}

func fmtToken(t *kvv1.CausalToken) string {
	if t == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d.%d", t.GetPhysical(), t.GetLogical())
}

func usage() {
	fmt.Fprintf(os.Stderr, `kvsmoke: minimal KV gRPC client

  kvsmoke -addr HOST:PORT put KEY VALUE
  kvsmoke -addr HOST:PORT get KEY
  kvsmoke -addr HOST:PORT del KEY
  kvsmoke -addr HOST:PORT bench N
`)
}

func die(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", args...)
	os.Exit(1)
}
