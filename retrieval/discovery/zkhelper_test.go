package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var errZkStartFailed = errors.New("cant start zk test cluster")

type zkTest struct {
	*zk.Conn
	cluster *zk.TestCluster
}

func newZkTest() (*zkTest, error) {
	ts, err := zk.StartTestCluster(1, os.Stdout, os.Stderr)
	if err != nil {
		return nil, errZkStartFailed
	}

	zkConn, _, err := ts.ConnectAll()
	if err != nil {
		return nil, err
	}

	zt := &zkTest{zkConn, ts}
	retry, maxRetry := 0, 3
	for {
		if zt.State() != zk.StateHasSession {
			time.Sleep(time.Duration(retry) * time.Second)
			retry++
		} else {
			break
		}

		if retry >= maxRetry {
			return nil, errors.New("can not establish session with zk")
		}
	}

	return zt, nil
}

func (c *zkTest) createServerset(path string, member *serversetMember) error {
	byt, err := json.Marshal(member)
	if err != nil {
		return err
	}

	memberPath := fmt.Sprintf("%s/%s", path, serversetNodePrefix)

	_, err = c.Create(memberPath, byt, zk.FlagEphemeral+zk.FlagSequence, zk.WorldACL(zk.PermAll))
	return err
}

func (c *zkTest) recursiveCreate(path string) error {
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) {
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) {
		j--
	}

	if j > 1 {
		if err := c.recursiveCreate(path[0 : j-1]); err != nil {
			return err
		}
	}

	_, err := c.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		return nil
	}

	return err
}

func (c *zkTest) recursiveDelete(path string) error {
	children, _, err := c.Children(path)
	if err != nil {
		return err
	}

	for _, child := range children {
		if err := c.recursiveDelete(path + "/" + child); err != nil {
			return err
		}
	}

	if err := c.Delete(path, -1); err != nil && err != zk.ErrNoNode {
		return err
	}

	return nil
}

func (c *zkTest) Stop() error {
	c.Close()
	return c.cluster.Stop()
}
