// +build myzk

package client

import (
	"strings"
	"sync"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	"github.com/smallnest/rpcx/log"
	"errors"
	"strconv"
	"bytes"
	"encoding/json"
)

func init() {
	zookeeper.Register()
}

// ZookeeperDiscovery is a zoopkeer service discovery.
// It always returns the registered servers in zookeeper.
type MyzkDiscovery struct {
	basePath string
	kv       store.Store
	pairs    []*KVPair
	chans    []chan []*KVPair
	mu       sync.Mutex

	// -1 means it always retry to watch until zookeeper is ok, 0 means no retry.
	RetriesAfterWatchFailed int

	stopCh chan struct{}
}

type zkDiscoverServIDVers struct {
	ID   int64      `json:"ID"`
	Vers []zkDiscoverVersIP `json:"Vers"`
}

type zkDiscoverVersIP struct {
	Ver string `json:"Ver"`
	IP  string `json:"IP"`
}

// NewZookeeperDiscovery returns a new ZookeeperDiscovery.
func NewMyzkDiscovery(basePath string, servicePath string, zkAddr []string, options *store.Config) ServiceDiscovery {
	if basePath[0] == '/' {
		basePath = basePath[1:]
	}

	if len(basePath) > 1 && strings.HasSuffix(basePath, "/") {
		basePath = basePath[:len(basePath)-1]
	}

	kv, err := libkv.NewStore(store.ZK, zkAddr, options)
	if err != nil {
		log.Infof("cannot create store: %v", err)
		panic(err)
	}

	//return NewMyzkDiscoveryWithStore(basePath+"/L/"+servicePath, kv)
	return NewMyzkDiscoveryWithStore(basePath, servicePath, kv)
}

func comPareTwoVersion (Variable1 string, Variable2 string) (string, error) {
	Vs1 := strings.Split(Variable1, ".")
	Vs2 := strings.Split(Variable2, ".")

	if len(Vs1) != 4 || len(Vs2) != 4 {
		return "", errors.New("The Format of ServerIDVersion error v1:[" + Variable1 + "]v2:["+ Variable2 + "]")
	}

	for i:=0; i<4; i++ {
		var t1 int
		var t2 int
		var err error

		t1, err = strconv.Atoi(Vs1[i])
		t2, err = strconv.Atoi(Vs1[i])
		_ = err

		if t1 > t2 {
			return Variable1, nil
		} else {
			return Variable2, nil
		}
	}
	//this means the two string have exactly same Value
	return Variable1, nil

}

func getTopVersion(Versions []zkDiscoverVersIP) (string, error) {

	var topVersion string
	var p string

	for _, v := range Versions {

		//Format check
		if len(strings.Split(v.Ver, ".")) != 4 {
			continue
		}

		//On first enter
		if topVersion == "" {
			topVersion = v.Ver
		}

		//temp Variable
		p = v.Ver

		t, err := comPareTwoVersion(topVersion, p)
		if err != nil {
			log.Infof("comPareTwoVersion err:", err)
			continue
		}
		topVersion = t
	}
	return topVersion, nil
}

// NewZookeeperDiscoveryWithStore returns a new ZookeeperDiscovery with specified store.
func NewMyzkDiscoveryWithStore(basePath string, servicePath string, kv store.Store) ServiceDiscovery {
	if basePath[0] == '/' {
		basePath = basePath[1:]
	}
	d := &MyzkDiscovery{basePath: basePath + "/L/" + servicePath, kv: kv}
	d.stopCh = make(chan struct{})

	ps, err := kv.List(basePath + "/L/" + servicePath)
	if err != nil {
		log.Infof("get kv.List path:%s, err: %v", basePath, err)
		panic(err)
	}

	var realIPPORT []*store.KVPair

	for _, p := range ps {

		//strPerID := basePath+"/I/"+string(p.Value)

		//The magic ID for Notify
		var ID int
		ID, err = strconv.Atoi(p.Key)
		if ID < 0 {
			continue
		}

		//Get All the Versions of the ID
		//BASE/L/ServerName/ServerID
		strPerID := basePath + "/L/" + servicePath + "/" + string(p.Key)
		VersionPerID, err1 := kv.Get(strPerID)
		if err1 != nil {
			log.Infof("get kv.Get path:%s, err: %v", strPerID, err1)
			panic(err1)
		}

		var jsonValue bytes.Buffer
		jsonValue.Write(VersionPerID.Value)

		//Parse json to Struct
		var zkServID zkDiscoverServIDVers
		err1 = json.Unmarshal(jsonValue.Bytes(), &zkServID)
		if err1 != nil {
			log.Errorf("json.Unmarshal zk path %s: %v", string(jsonValue.Bytes()), err1)
			panic(err1)
		}

		topV, err2 := getTopVersion(zkServID.Vers)
		if err2 != nil {
			log.Infof("getTopVersion error path:%s, err:%v", strPerID, err1)
			panic(err1)
		}

		//Get the latest Version
		for _, p := range zkServID.Vers {
			if p.Ver == topV {
				realIPPORT = append(realIPPORT, &store.KVPair{Key:strconv.Itoa(int(zkServID.ID)), Value:[]byte(p.IP)})
			}
		}
	}

	var pairs = make([]*KVPair, 0, len(ps))
	for _, p := range realIPPORT {
		if string(p.Value) == "" {
			continue
		}
		pairs = append(pairs, &KVPair{Key: string(p.Value), Value: p.Key})
	}

	d.pairs = pairs
	d.RetriesAfterWatchFailed = -1
	go d.watch()

	return d
}

// NewZookeeperDiscoveryTemplate returns a new ZookeeperDiscovery template.
func MyzkDiscoveryTemplate(basePath string, zkAddr []string, options *store.Config) ServiceDiscovery {
	if basePath[0] == '/' {
		basePath = basePath[1:]
	}

	if len(basePath) > 1 && strings.HasSuffix(basePath, "/") {
		basePath = basePath[:len(basePath)-1]
	}

	kv, err := libkv.NewStore(store.ZK, zkAddr, options)
	if err != nil {
		log.Infof("cannot create store: %v", err)
		panic(err)
	}

	return &MyzkDiscovery{basePath: basePath, kv: kv}
}

// Clone clones this ServiceDiscovery with new servicePath.
func (d MyzkDiscovery) Clone(servicePath string) ServiceDiscovery {
	return NewMyzkDiscoveryWithStore(d.basePath, servicePath, d.kv)
}

// GetServices returns the servers
func (d MyzkDiscovery) GetServices() []*KVPair {
	return d.pairs
}

// WatchService returns a nil chan.
func (d *MyzkDiscovery) WatchService() chan []*KVPair {
	ch := make(chan []*KVPair, 10)
	d.chans = append(d.chans, ch)
	return ch
}

func (d *MyzkDiscovery) RemoveWatcher(ch chan []*KVPair) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var chans []chan []*KVPair
	for _, c := range d.chans {
		if c == ch {
			continue
		}

		chans = append(chans, c)
	}

	d.chans = chans
}

func (d *MyzkDiscovery) watch() {
	for {
		var err error
		var c <-chan []*store.KVPair
		var tempDelay time.Duration

		retry := d.RetriesAfterWatchFailed
		for d.RetriesAfterWatchFailed == -1 || retry > 0 {
			c, err = d.kv.WatchTree(d.basePath, nil)
			if err != nil {
				if d.RetriesAfterWatchFailed > 0 {
					retry--
				}
				if tempDelay == 0 {
					tempDelay = 1 * time.Second
				} else {
					tempDelay *= 2
				}
				if max := 30 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Warnf("can not watchtree (with retry %d, sleep %v): %s: %v", retry, tempDelay, d.basePath, err)
				time.Sleep(tempDelay)
				continue
			}

			break
		}

		if err != nil {
			log.Errorf("can't watch %s: %v", d.basePath, err)
			return
		}

		if err != nil {
			log.Fatalf("can not watchtree: %s: %v", d.basePath, err)
		}

	readChanges:
		for {
			select {
			case <-d.stopCh:
				log.Info("discovery has been closed")
				return
			case ps := <-c:
				if ps == nil {
					break readChanges
				}
				var pairs []*KVPair // latest servers
				for _, p := range ps {

					//log.Errorf("Watcher event %s: %s", p.Key, string(p.Value))

					var ID int
					ID, err = strconv.Atoi(p.Key)
					if ID < 0 {
						continue
					}
					//Decode json

					var jsonValue bytes.Buffer
					jsonValue.Write(p.Value)

					var zkServID zkDiscoverServIDVers
					err = json.Unmarshal(jsonValue.Bytes(), &zkServID)
					if err != nil {
						log.Errorf("json.Unmarshal zk path %s: %v", string(jsonValue.Bytes()), err)
					}

					topV, err1 := getTopVersion(zkServID.Vers)
					if err1 != nil {
						log.Infof("getTopVersion error path:%s, err:%v", string(p.Value), err1)
					}

					for _, pv := range zkServID.Vers {
						if pv.Ver == topV {
							pairs = append(pairs, &KVPair{Key:pv.IP, Value:p.Key})
						}
					}

					//pairs = append(pairs, &KVPair{Key: p.Key, Value: string(p.Value)})
				}
				d.pairs = pairs

				for _, ch := range d.chans {
					ch := ch
					go func() {
						defer func() {
							if r := recover(); r != nil {

							}
						}()
						select {
						case ch <- pairs:
						case <-time.After(time.Minute):
							log.Warn("chan is full and new change has been dropped")
						}
					}()
				}
			}
		}

		log.Warn("chan is closed and will rewatch")
	}
}

func (d *MyzkDiscovery) Close() {
	close(d.stopCh)
}
