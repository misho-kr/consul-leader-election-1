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

	log.Infof("leadership session not found, creating: %s", sessionName)

	session, _, err := cc.Client.Session().Create(
		&api.SessionEntry{
			Name: sessionName,
		}, nil)
	if err != nil {
		log.Warn(err)
	}
	return session
}

func (cc *ConsulClient) GetAgentName() string {
	agent, _ := cc.Client.Agent().Self()
	return agent["Config"]["NodeName"].(string)
}

func (cc *ConsulClient) GetKey(keyName string) (*api.KVPair, error) {
	kv, _, err := cc.Client.KV().Get(keyName, nil)
	return kv, err
}

func (cc *ConsulClient) AcquireSessionKey(key, session string) (bool, error) {
	acquired, _, err := cc.Client.KV().Acquire(
		&api.KVPair{
			Key:     key,
			Value:   []byte(cc.GetAgentName()),
			Session: session,
		}, nil)

	return acquired, err
}

func (cc *ConsulClient) ReleaseKey(key, session string) (bool, error) {
	released, _, err := cc.Client.KV().Release(
		&api.KVPair{
			Key:     key,
			Value:   []byte(cc.GetAgentName()),
			Session: session,
		}, nil)

	return released, err
}
