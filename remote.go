package viper_remote

import (
	"errors"
	"io"

	"github.com/spf13/viper"
)

var Provider = &provider{registered: make(map[string]RemoteConfigFactory)}
var ErrNotFound = errors.New("provider not found")

func init() {
	Provider.Register("etcd", new(etcdProvider))
}

type provider struct {
	registered map[string]RemoteConfigFactory
}

type RemoteConfigFactory interface {
	Get(rp viper.RemoteProvider) (io.Reader, error)
	Watch(rp viper.RemoteProvider) (io.Reader, error)
	WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool)
}

func (p provider) Register(provider string, tc RemoteConfigFactory) {
	p.registered[provider] = tc
}

func (p provider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	r, exist := p.registered[rp.Provider()]
	if !exist {
		return nil, ErrNotFound
	}

	return r.Get(rp)
}

func (p provider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	r, exist := p.registered[rp.Provider()]
	if !exist {
		return nil, ErrNotFound
	}

	return r.Watch(rp)
}

func (p provider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	r, exist := p.registered[rp.Provider()]
	if !exist {
		return make(<-chan *viper.RemoteResponse), nil
	}

	return r.WatchChannel(rp)
}
