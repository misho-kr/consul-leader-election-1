package election

import (
	"errors"
	"testing"
	"time"

	. "github.com/franela/goblin"
	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

type FakeClient struct {
	Key             string
	GetKeyOutput    string
	AcquireKeyError bool
	ReleaseKeyError bool
}

type FakeConsulClient struct {
	Client FakeClient
}

func (fcc *FakeConsulClient) GetAgentName() string {
	return "my node"
}

func (fcc *FakeConsulClient) GetKey(key string) (*api.KVPair, error) {
	kv := &api.KVPair{Key: key, Value: []byte("my node"), Session: fcc.Client.Key}
	if fcc.Client.GetKeyOutput == "kv" {
		return kv, nil
	}
	return nil, errors.New("key not found")
}

func (fcc *FakeConsulClient) ReleaseKey(*api.KVPair) (bool, error) {
	if fcc.Client.ReleaseKeyError {
		return false, errors.New("ERROR RELEASE KEY")
	}
	fcc.Client.GetKeyOutput = ""
	return true, nil
}

func (fcc *FakeConsulClient) GetSession(name string) string {
	return name
}

func (fcc *FakeConsulClient) AcquireSessionKey(_, _ string) (bool, error) {
	if fcc.Client.AcquireKeyError {
		return false, errors.New("ERROR")
	}
	fcc.Client.GetKeyOutput = "kv"
	return true, nil
}

func TestLeaderElection(t *testing.T) {

	g := Goblin(t)
	g.Describe("LeaderElection", func() {
		g.It("StepDown() Failure", func() {
			fakeClient := FakeClient{Key: "service/leader-election/leader", GetKeyOutput: "kv", ReleaseKeyError: true}
			fake := &FakeConsulClient{Client: fakeClient}
			le := LeaderElection{
				LeaderKey:     fakeClient.Key,
				StopElection:  make(chan bool),
				WatchWaitTime: 2 * time.Second,
				Client:        fake,
			}
			go le.ElectLeader()
			time.Sleep(le.WatchWaitTime)
			le.CancelElection()
			err := le.StepDown()
			g.Assert(err != nil).IsTrue()

		})
		g.It("StepDown()", func() {
			fakeClient := FakeClient{Key: "service/leader-election/leader", GetKeyOutput: "kv"}
			fake := &FakeConsulClient{Client: fakeClient}
			le := LeaderElection{
				LeaderKey:     fakeClient.Key,
				StopElection:  make(chan bool),
				WatchWaitTime: 2 * time.Second,
				Client:        fake,
			}
			go le.ElectLeader()
			time.Sleep(le.WatchWaitTime)
			le.CancelElection()
			_ = le.StepDown()
			g.Assert(le.IsLeader()).IsFalse()

		})
		g.It("ElectLeader() Failure", func() {
			fakeClient := FakeClient{Key: "service/leader-election/leader", AcquireKeyError: true}
			fake := &FakeConsulClient{Client: fakeClient}
			le := LeaderElection{
				LeaderKey:     fakeClient.Key,
				StopElection:  make(chan bool),
				WatchWaitTime: 2 * time.Second,
				Client:        fake,
			}
			go le.ElectLeader()
			time.Sleep(le.WatchWaitTime)
			le.CancelElection()
			g.Assert(le.IsLeader()).IsFalse()
		})
		g.It("ElectLeader()", func() {
			fakeClient := FakeClient{Key: "service/leader-election/leader"}
			fake := &FakeConsulClient{Client: fakeClient}
			le := LeaderElection{
				LeaderKey:     fakeClient.Key,
				StopElection:  make(chan bool),
				WatchWaitTime: 2 * time.Second,
				Client:        fake,
			}
			go le.ElectLeader()
			time.Sleep(le.WatchWaitTime)
			le.CancelElection()
			g.Assert(le.IsLeader()).IsTrue()
		})
		g.It("CancelElection()", func() {
			fakeClient := FakeClient{Key: "service/leader-election/leader", GetKeyOutput: "kv"}
			fake := &FakeConsulClient{Client: fakeClient}
			le := LeaderElection{
				LeaderKey:     fakeClient.Key,
				StopElection:  make(chan bool),
				WatchWaitTime: time.Second,
				Client:        fake,
			}
			go le.ElectLeader()
			le.CancelElection()
			g.Assert(le.IsLeader()).IsTrue()
		})

		g.It("IsLeader()", func() {
			fakeClient := FakeClient{Key: "service/leader-election/leader"}
			fake := &FakeConsulClient{Client: fakeClient}
			le := LeaderElection{
				LeaderKey:     fakeClient.Key,
				StopElection:  make(chan bool),
				WatchWaitTime: time.Second,
				Client:        fake,
			}

			g.Assert(le.IsLeader()).IsFalse()

			fakeClient = FakeClient{Key: "service/leader-election/leader", GetKeyOutput: "kv"}
			fake = &FakeConsulClient{Client: fakeClient}
			le = LeaderElection{
				LeaderKey:     fakeClient.Key,
				StopElection:  make(chan bool),
				WatchWaitTime: time.Second,
				Client:        fake,
			}

			g.Assert(le.IsLeader()).IsTrue()
		})
	})

}
