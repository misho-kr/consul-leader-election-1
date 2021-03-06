package client

import (
	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
)

type ConsulClient struct {
	Client *api.Client
}

func (cc *ConsulClient) GetSession(sessionName string) string {
	name := cc.GetAgentName()
	sessions, _, err := cc.Client.Session().List(nil)
	for _, session := range sessions {
		if session.Name == sessionName && session.Node == name {
			return session.ID
		}
	}

	log.Info("No leadership sessions found, creating...")

	sessionEntry := &api.SessionEntry{Name: sessionName}
	session, _, err := cc.Client.Session().Create(sessionEntry, nil)
	if err != nil {
		log.Warn(err)
	}
	return session
}

func (cc *ConsulClient) AcquireSessionKey(key, session string) (bool, error) {

	pair := &api.KVPair{
		Key:     key,
		Value:   []byte(cc.GetAgentName()),
		Session: session,
	}

	aquired, _, err := cc.Client.KV().Acquire(pair, nil)

	return aquired, err
}

func (cc *ConsulClient) GetAgentName() string {
	agent, _ := cc.Client.Agent().Self()
	return agent["Config"]["NodeName"].(string)
}

func (cc *ConsulClient) GetKey(keyName string) (*api.KVPair, error) {
	kv, _, err := cc.Client.KV().Get(keyName, nil)
	return kv, err
}

func (cc *ConsulClient) ReleaseKey(key *api.KVPair) (bool, error) {
	released, _, err := cc.Client.KV().Release(key, nil)
	return released, err
}
