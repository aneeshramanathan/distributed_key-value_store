// Command kvnode runs one replica of the key/value store.
//
// Example: a three-node cluster on one machine.
//
//	PEERS=http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003
//	kvnode -id 0 -peers $PEERS -data ./data/0 &
//	kvnode -id 1 -peers $PEERS -data ./data/1 &
//	kvnode -id 2 -peers $PEERS -data ./data/2 &
//
//	curl -L -X PUT --data-binary 'world' http://127.0.0.1:7001/kv/hello
//	curl -L http://127.0.0.1:7002/kv/hello
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/node"
)

func main() {
	id := flag.Int("id", -1, "this node's index in -peers (required)")
	peers := flag.String("peers", "", "comma-separated base URLs of all nodes, in ID order (required)")
	dataDir := flag.String("data", "", "data directory (default ./data/<id>)")
	maxRaftState := flag.Int("snapshot-bytes", 4<<20, "take a snapshot once the Raft log reaches this size")
	noSync := flag.Bool("nosync", false, "do not fsync the log (benchmarking only: unsafe)")
	electionMin := flag.Duration("election-timeout", 250*time.Millisecond, "minimum election timeout (the maximum is twice this)")
	heartbeat := flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
	flag.Parse()

	if *id < 0 || *peers == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/%d", *id)
	}
	raftCfg := node.DefaultRaftConfig()
	raftCfg.ElectionTimeoutMin = *electionMin
	raftCfg.ElectionTimeoutMax = 2 * *electionMin
	raftCfg.HeartbeatInterval = *heartbeat

	n, err := node.Start(node.Config{
		ID:           *id,
		Peers:        strings.Split(*peers, ","),
		DataDir:      *dataDir,
		Raft:         raftCfg,
		MaxRaftState: *maxRaftState,
		NoSync:       *noSync,
	})
	if err != nil {
		log.Fatalf("kvnode: %v", err)
	}
	log.Printf("kvnode %d listening on %s, data in %s", *id, n.Addr(), *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("kvnode %d shutting down", *id)
	n.Close()
}
