// Command conclave runs a conclave server, talks to a cluster, benchmarks
// it, and runs the deterministic simulator.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/useless-husband/conclave/client"
	"github.com/useless-husband/conclave/internal/raft"
	"github.com/useless-husband/conclave/internal/server"
	"github.com/useless-husband/conclave/internal/vfs"
)

var version = "dev"

const usage = `conclave: a replicated, linearizable key-value store

Server:
  conclave serve -id N -data DIR [-bootstrap] [-raft ADDR] [-api ADDR] [-fsync full|fsync|none]

Client (-addr takes one or more API addresses, comma-separated; default $CONCLAVE_ADDR):
  conclave get KEY
  conclave put KEY VALUE
  conclave delete KEY
  conclave cas [-absent] KEY EXPECTED NEW
  conclave status
  conclave members add -id N -raft ADDR -api ADDR
  conclave members remove -id N
  conclave transfer -to N
  conclave bench [-clients 16] [-duration 10s] [-mix put|get|mixed] [-keys 1000] [-value 64]

Simulator:
  conclave sim -seed N [-trace]
  conclave sim -seeds A-B [-workers 4]

  conclave version
`

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "serve":
		err = serve(args)
	case "get", "put", "delete", "cas":
		err = kvCommand(cmd, args)
	case "status":
		err = status(args)
	case "members":
		err = members(args)
	case "transfer":
		err = transfer(args)
	case "bench":
		err = bench(args)
	case "sim":
		err = simCommand(args)
	case "version":
		fmt.Println("conclave", version)
	case "help", "-h", "-help", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "conclave: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		msg := err.Error()
		if !strings.HasPrefix(msg, "conclave:") {
			msg = "conclave: " + msg
		}
		fmt.Fprintln(os.Stderr, msg)
		os.Exit(1)
	}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	id := fs.Uint64("id", 0, "server ID (positive, unique, never reused)")
	data := fs.String("data", "", "data directory")
	raftAddr := fs.String("raft", "", "listen address for other servers (default: previous run's, else 127.0.0.1:0)")
	apiAddr := fs.String("api", "", "listen address for clients (default: previous run's, else 127.0.0.1:0)")
	bootstrap := fs.Bool("bootstrap", false, "create a new cluster with this server as its only member")
	fsync := fs.String("fsync", "full", "durability: full (F_FULLFSYNC on macOS), fsync, or none")
	tick := fs.Duration("tick", 10*time.Millisecond, "logical clock tick")
	snap := fs.Uint64("snapshot-every", 10000, "entries between snapshots")
	quiet := fs.Bool("q", false, "log only errors")
	fs.Parse(args)
	if *data == "" {
		return errors.New("serve: -data is required")
	}
	mode, err := vfs.ParseSyncMode(*fsync)
	if err != nil {
		return err
	}
	logger := log.New(os.Stderr, fmt.Sprintf("n%d ", *id), log.LstdFlags|log.Lmicroseconds)
	if *quiet {
		logger = log.New(discard{}, "", 0)
	}
	s, err := server.Start(server.Config{
		ID: raft.NodeID(*id), DataDir: *data, RaftAddr: *raftAddr, APIAddr: *apiAddr,
		Bootstrap: *bootstrap, Fsync: mode, Tick: *tick, SnapshotEvery: *snap, Logger: logger,
	})
	if err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Printf("shutting down")
	return s.Close()
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func addrFlag(fs *flag.FlagSet) *string {
	return fs.String("addr", os.Getenv("CONCLAVE_ADDR"), "API address(es) of the cluster, comma-separated")
}

func newClient(addr string) (*client.Client, error) {
	if addr == "" {
		return nil, errors.New("no server address: use -addr or set CONCLAVE_ADDR")
	}
	return client.New(strings.Split(addr, ","), client.Options{}), nil
}

func kvCommand(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	addr := addrFlag(fs)
	timeout := fs.Duration("timeout", 10*time.Second, "give up after this long")
	absent := fs.Bool("absent", false, "cas: succeed only if the key does not exist (EXPECTED is ignored)")
	fs.Parse(args)
	c, err := newClient(*addr)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	a := fs.Args()
	need := map[string]int{"get": 1, "put": 2, "delete": 1, "cas": 3}[cmd]
	if len(a) != need {
		return fmt.Errorf("%s takes %d argument(s)", cmd, need)
	}
	switch cmd {
	case "get":
		v, err := c.Get(ctx, a[0])
		if err != nil {
			return err
		}
		fmt.Println(v)
	case "put":
		if err := c.Put(ctx, a[0], a[1]); err != nil {
			return err
		}
		fmt.Println("ok")
	case "delete":
		old, existed, err := c.Delete(ctx, a[0])
		if err != nil {
			return err
		}
		if !existed {
			return client.ErrNotFound
		}
		fmt.Printf("deleted (was %q)\n", old)
	case "cas":
		ok, cur, found, err := c.CAS(ctx, a[0], a[1], *absent, a[2])
		if err != nil {
			return err
		}
		if !ok {
			if !found {
				return errors.New("cas failed: the key does not exist")
			}
			return fmt.Errorf("cas failed: current value is %q", cur)
		}
		fmt.Println("ok")
	}
	return nil
}

func status(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	addr := addrFlag(fs)
	fs.Parse(args)
	c, err := newClient(*addr)
	if err != nil {
		return err
	}
	defer c.Close()
	for _, a := range strings.Split(*addr, ",") {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		st, err := c.Status(ctx, a)
		cancel()
		if err != nil {
			fmt.Printf("%s: %v\n", a, err)
			continue
		}
		b, _ := json.MarshalIndent(st, "", "  ")
		fmt.Printf("%s\n", b)
	}
	return nil
}

func members(args []string) error {
	if len(args) == 0 || (args[0] != "add" && args[0] != "remove") {
		return errors.New("usage: members add|remove [flags]")
	}
	fs := flag.NewFlagSet("members "+args[0], flag.ExitOnError)
	addr := addrFlag(fs)
	id := fs.Uint64("id", 0, "member ID")
	raftAddr := fs.String("raft", "", "add: the new member's raft address")
	apiAddr := fs.String("api", "", "add: the new member's API address")
	fs.Parse(args[1:])
	c, err := newClient(*addr)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if args[0] == "add" {
		err = c.Admin(ctx, "POST", "/v1/members", server.MemberRequest{ID: *id, Raft: *raftAddr, API: *apiAddr})
	} else {
		err = c.Admin(ctx, "DELETE", fmt.Sprintf("/v1/members/%d", *id), nil)
	}
	if err == nil {
		fmt.Println("ok")
	}
	return err
}

func transfer(args []string) error {
	fs := flag.NewFlagSet("transfer", flag.ExitOnError)
	addr := addrFlag(fs)
	to := fs.Uint64("to", 0, "member to hand leadership to")
	fs.Parse(args)
	c, err := newClient(*addr)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Admin(ctx, "POST", fmt.Sprintf("/v1/transfer/%d", *to), nil); err != nil {
		return err
	}
	fmt.Println("ok")
	return nil
}
