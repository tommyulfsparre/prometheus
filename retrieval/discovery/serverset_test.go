package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/config"
)

func newTestServersetDiscovery(server, path string) (chan config.TargetGroup, *ServersetDiscovery) {
	ch := make(chan config.TargetGroup)
	sd := NewServersetDiscovery(&config.ServersetSDConfig{
		Servers: []string{server},
		Paths:   []string{path},
		Timeout: config.Duration(10 * time.Second),
	})
	return ch, sd
}

func TestServersetDelete(t *testing.T) {
	zt, err := newZkTest()
	if err != nil {
		if err == errZkStartFailed {
			t.Skip("cant start zk test cluster, skipping test")
		}
		t.Fatal(err)
	}
	defer func() {
		if err := zt.Stop(); err != nil {
			t.Log(err)
		}
	}()

	// Create root.
	if err := zt.recursiveCreate("/retrieval"); err != nil {
		t.Fatalf("failed to create zk path: %+v", err)
	}

	zkServer := fmt.Sprintf("127.0.0.1:%d", zt.cluster.Servers[0].Port)
	// Start serverset discovery from root.
	ch, sd := newTestServersetDiscovery(zkServer, "/retrieval")

	done := make(chan struct{})
	go sd.Run(ch, done)

	paths := []string{
		"/retrieval/discovery/serverset_1",
		"/retrieval/discovery/serverset_2",
	}

	expectedMembers := []*serversetMember{
		&serversetMember{
			ServiceEndpoint: serversetEndpoint{
				Host: "127.0.0.1",
				Port: 1111,
			},
		},
		&serversetMember{
			ServiceEndpoint: serversetEndpoint{
				Host: "127.0.0.1",
				Port: 2222,
			},
		},
	}

	// Number of serverset members times amount of additions (addServers).
	expectedTargetUpdates := len(expectedMembers) * 2

	// Start watching for target updates.
	signal := make(chan struct{})
	go func() {
		cnt := 0
		for {
			select {
			case tg := <-ch:
				for _, target := range tg.Targets {
					if _, ok := target["__address__"]; ok {
						cnt++
					}
				}
			}

			if cnt >= expectedTargetUpdates {
				break
			}
		}
		close(signal)
	}()

	addServerset := func() {
		for i, path := range paths {
			if err := zt.recursiveCreate(path); err != nil {
				t.Fatalf("failed to create zk path: %+v", err)
			}

			if err := zt.createServerset(path, expectedMembers[i]); err != nil {
				t.Fatalf("failed to create zk serverset: %+v", err)
			}
		}
	}

	addServerset()
	if err := zt.recursiveDelete("/retrieval/discovery"); err != nil {
		t.Fatalf("failed to delete zk path: %+v", err)
	}
	addServerset()

	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for target updates")
	}

	var result []string
	for _, source := range sd.Sources() {
		result = append(result, source)
	}

	if len(result) != len(expectedMembers) {
		t.Fatalf("expected %d members got %d", len(expectedMembers), len(result))
	}
}
