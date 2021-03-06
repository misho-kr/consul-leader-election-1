package election

import (
	"time"

	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
)

const minElectionCheckInterval = time.Second

type ConsulInterface interface {
	GetAgentName() string
	GetKey(string) (*api.KVPair, error)
	GetSession(string) string
	AcquireSessionKey(string, string) (bool, error)
	ReleaseKey(string, string) (bool, error)
}

type LeaderElection struct {
	LeaderKey     string
	WatchWaitTime time.Duration
	StopElection  chan bool
	Client        ConsulInterface
}

func (le *LeaderElection) CancelElection() {
	le.StopElection <- true
}

func (le *LeaderElection) StepDown() error {
	if !le.IsLeader() {
		return nil
	}

	released, err := le.Client.ReleaseKey(le.LeaderKey, le.GetSession(le.LeaderKey))
	if err == nil && released {
		log.Infof("released leadership: %s", le.LeaderKey)
	}

	return err
}

func (le *LeaderElection) IsLeader() bool {
	kv, err := le.Client.GetKey(le.LeaderKey)
	if err != nil {
		log.Errorf("get [key=%s]: %s", le.LeaderKey, err)
		return false
	}
	if kv == nil {
		log.Infof("leadership key is missing: %s", le.LeaderKey)
		return false
	}

	return le.Client.GetAgentName() == string(kv.Value) &&
		le.GetSession(le.LeaderKey) == kv.Session
}

func (le *LeaderElection) GetSession(sessionName string) string {
	return le.Client.GetSession(sessionName)
}

func (le *LeaderElection) ElectLeader() {
	if le.WatchWaitTime < minElectionCheckInterval {
		le.WatchWaitTime = minElectionCheckInterval
	}

	name := le.Client.GetAgentName()
	for {
		select {
		case <-le.StopElection:
			log.Info("stopping election")
			return
		default:
			if !le.IsLeader() {
				switch acquired, err := le.Client.AcquireSessionKey(
					le.LeaderKey, le.GetSession(le.LeaderKey)); {
				case err != nil:
					log.Warn(err)
				case acquired:
					log.Infof("now the leader is: %s", name)
				}
			}

			kv, err := le.Client.GetKey(le.LeaderKey)
			if err != nil {
				log.Error(err)
			} else {
				if kv != nil && kv.Session != "" {
					log.Debugf("current leader=%s, session=%s",
						string(kv.Value), kv.Session)
				}
			}

			time.Sleep(le.WatchWaitTime)
		}
	}
}
