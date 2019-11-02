package viper_remote

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/spf13/viper"
	"go.etcd.io/etcd/clientv3"
)

func TestEtcd(t *testing.T) {
	type (
		Parent struct {
			Name     string `toml:"name"`
			Age      int    `toml:"age"`
			Relation string `toml:"relation"`
		}

		TestConfig struct {
			Id      int      `toml:"id"`
			Name    string   `toml:"name"`
			Parents []Parent `toml:"parents"`
		}
	)

	var err error
	var check = func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	configPath := "/students"
	etcdAddr := os.Getenv("ETCD_ADDR")
	configValue := `
id = 1
name = "pill"
[[parents]]
  name = "lily"
  age = 40
  relation = "mother"
[[parents]]
  name = "billy"
  age = 42
  relation = "father"`

	// write testing config
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}, DialTimeout: time.Second * 3})
	check(err)

	ctx := context.Background()
	lease := clientv3.NewLease(client)
	lgr, err := lease.Grant(ctx, 30)
	check(err)

	_, err = client.Put(ctx, configPath, configValue, clientv3.WithLease(lgr.ID))
	check(err)

	viper.RemoteConfig = Provider
	vp := viper.New()
	err = vp.AddRemoteProvider("etcd", etcdAddr, configPath)
	check(err)
	vp.SetConfigFile(".toml")

	t.Run("Get", func(t *testing.T) {
		err = vp.ReadRemoteConfig()

		cfg := new(TestConfig)
		err = vp.Unmarshal(cfg)
		check(err)

		want := &TestConfig{
			Id:   1,
			Name: "pill",
			Parents: []Parent{
				{"lily", 40, "mother"},
				{"billy", 42, "father"},
			},
		}

		if !reflect.DeepEqual(cfg, want) {
			t.Fatalf("got: %+v, want: %+v", cfg, want)
		}
	})

	t.Run("WatchChannel", func(t *testing.T) {
		err = vp.WatchRemoteConfigOnChannel()
		check(err)

		configValue := `
id = 2
name = "pill"
[[parents]]
  name = "lily"
  age = 40
  relation = "mother"`
		_, err = client.Put(ctx, configPath, configValue, clientv3.WithLease(lgr.ID))
		check(err)

		time.Sleep(1e8)
		cfg := new(TestConfig)
		err = vp.Unmarshal(cfg)
		check(err)

		want := &TestConfig{
			Id:   2,
			Name: "pill",
			Parents: []Parent{
				{"lily", 40, "mother"},
			},
		}

		if !reflect.DeepEqual(cfg, want) {
			t.Fatalf("got: %+v, want: %+v", cfg, want)
		}
	})

}
