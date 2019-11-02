package viper_remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	"github.com/spf13/viper"
	"go.etcd.io/etcd/clientv3"
)

type etcdProvider struct {
	client *clientv3.Client
}

func (p *etcdProvider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	client, err := p.getClient(rp.Endpoint())
	if err != nil {
		return nil, err
	}

	resp, err := client.Get(context.Background(), rp.Path())
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("kv length equal 0")
	}

	return bytes.NewReader(resp.Kvs[0].Value), nil
}

func (p *etcdProvider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	return p.Get(rp)
}

func (p *etcdProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	rr := make(chan *viper.RemoteResponse)

	client, err := p.getClient(rp.Endpoint())
	if err != nil {
		log.Printf("get client failed: %v", err)
		rr <- &viper.RemoteResponse{Error: err}
		close(rr)
		return rr, nil
	}

	done := make(chan bool)
	watcher := client.Watch(context.Background(), rp.Path())
	go func(done chan bool) {
		for {
			select {
			case <-done:
				return
			case wr := <-watcher:
				if wr.Canceled {
					return
				}

				for _, ev := range wr.Events {
					rr <- &viper.RemoteResponse{Value: ev.Kv.Value, Error: err}
				}
			}
		}
	}(done)

	return rr, done
}

func (p *etcdProvider) getClient(endpoint string) (clnt *clientv3.Client, err error) {
	if p.client != nil {
		return p.client, nil
	}

	clnt, err = clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoint, ","),
		DialTimeout: time.Second * 30,
	})

	if err != nil {
		return nil, err
	}

	p.client = clnt
	return
}
