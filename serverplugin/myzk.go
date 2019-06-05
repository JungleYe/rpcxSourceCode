// +build myzk

package serverplugin

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store/zookeeper"

	"github.com/docker/libkv/store"
	"github.com/rcrowley/go-metrics"
	"github.com/smallnest/rpcx/log"
	"encoding/json"
	"bytes"
)

type zkServIDVers struct {
	ID   int64      `json:"ID"`
	Vers []zkVersIP `json:"Vers"`
}

type zkVersIP struct {
	Ver string `json:"Ver"`
	IP  string `json:"IP"`
}



func init() {
	zookeeper.Register()
}

// ZooKeeperRegisterPlugin implements zookeeper registry.
type MyZkRegisterPlugin struct {
	// service address, for example, tcp@127.0.0.1:8972, quic@127.0.0.1:1234
	ServiceAddress string
	// zookeeper addresses
	ZooKeeperServers []string
	// base path for rpcx server, for example com/example/rpcx
	BasePath string
	Metrics  metrics.Registry
	// Registered services
	ServiceName string

	//Service Format ServerName#ServerID#Version
	Services       []string


	metasLock      sync.RWMutex
	metas          map[string]string
	UpdateInterval time.Duration

	Options *store.Config
	kv      store.Store

	dying chan struct{}
	done  chan struct{}

	//the version of SERVER 
	Version	string
	//the ID of SERVER
	ServerID int

	//Server Name path
	serverNamePath	string

}


// Start starts to connect zookeeper cluster
func (p *MyZkRegisterPlugin) Start() error {

	if p.Version == "" {
		log.Errorf("The Version of Server is NULL")
		return errors.New("The Version of Server is NULL")
	} else {
		ss := strings.Split(p.Version, ".")
		if len(ss) != 4 {
			log.Errorf("The Version Format is NOT CORRECT, Use: x.x.x.x")
			return errors.New("The Version Format is NOT CORRECT, Use: x.x.x.x")
		}
	}

	if p.serverNamePath == "" {
		p.serverNamePath = "L"
	}

	if p.ServerID == 0 {
		log.Errorf("The ID of Server is NULL")
		return errors.New("The ID of Server is NULL")
	}

	if p.done == nil {
		p.done = make(chan struct{})
	}
	if p.dying == nil {
		p.dying = make(chan struct{})
	}

	if p.kv == nil {
		kv, err := libkv.NewStore(store.ZK, p.ZooKeeperServers, p.Options)
		if err != nil {
			log.Errorf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}

	if p.BasePath[0] == '/' {
		p.BasePath = p.BasePath[1:]
	}

	err := p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", p.BasePath, err)
		return err
	}


	/*
	if p.UpdateInterval > 0 {
		ticker := time.NewTicker(p.UpdateInterval)
		go func() {
			defer p.kv.Close()

			// refresh service TTL
			for {
				select {
				case <-p.dying:
					close(p.done)
					return
				case <-ticker.C:
					var data []byte
					if p.Metrics != nil {
						clientMeter := metrics.GetOrRegisterMeter("clientMeter", p.Metrics)
						data = []byte(strconv.FormatInt(clientMeter.Count()/60, 10))
					}
					//set this same metrics for all services at this server
					for _, name := range p.Services {
						nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
						kvPaire, err := p.kv.Get(nodePath)
						if err != nil {
							log.Infof("can't get data of node: %s, because of %v", nodePath, err.Error())

							p.metasLock.RLock()
							meta := p.metas[name]
							p.metasLock.RUnlock()

							err = p.kv.Put(nodePath, []byte(meta), &store.WriteOptions{TTL: p.UpdateInterval * 3})
							if err != nil {
								log.Errorf("cannot re-create zookeeper path %s: %v", nodePath, err)
							}
						} else {
							v, _ := url.ParseQuery(string(kvPaire.Value))
							v.Set("tps", string(data))
							p.kv.Put(nodePath, []byte(v.Encode()), &store.WriteOptions{TTL: p.UpdateInterval * 3})
						}
					}
				}
			}
		}()
	}
	*/

	return nil
}

// Stop unregister all services.
func (p *MyZkRegisterPlugin) Stop() error {
	close(p.dying)
	<-p.done

	if p.kv == nil {
		kv, err := libkv.NewStore(store.ZK, p.ZooKeeperServers, p.Options)
		if err != nil {
			log.Errorf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}

	if p.BasePath[0] == '/' {
		p.BasePath = p.BasePath[1:]
	}

	for _, name := range p.Services {
		strs := strings.Split(name, "#")
		if len(strs) != 2 {
			continue
		}

		err := p.delVerIPAddrJson(strs[0], strs[1])
		if err != nil {
			log.Errorf("cannot delete delVerIPAddrJson zk path %s:%s, %v", strs[0], strs[1], err)
			continue
		}
	}

	return nil
}

// HandleConnAccept handles connections from clients
func (p *MyZkRegisterPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if p.Metrics != nil {
		clientMeter := metrics.GetOrRegisterMeter("clientMeter", p.Metrics)
		clientMeter.Mark(1)
	}
	return conn, true
}

// Register handles registering event.
// this service is registered at



func (p *MyZkRegisterPlugin) addVerIPAddrJson(path string, serverId int, version string, IPAddr string) error {
	var exist bool
	var err error
	exist, err = p.kv.Exists(path)
	if err != nil {
		log.Errorf("Exist err zk path %s: %v", path, err)
		return err
	}
	var jsonIDVerIpAddr []byte

	var needNotify bool
	if exist {
		//Find the ids list of json
		//ps, err1 := p.kv.List(nodePath)
		ps, err1 := p.kv.Get(path)
		if err1 != nil {
			log.Errorf("p.kv.List zk path %s: %v", path, err)
			return err1
		}
		//log.Errorf("ps.len %d", len(ps))
		var jsonValue bytes.Buffer
		jsonValue.Write(ps.Value)

		//Parse json to Struct
		var zkServID zkServIDVers
		err1 = json.Unmarshal(jsonValue.Bytes(), &zkServID)
		if err1 != nil {
			log.Errorf("json.Unmarshal zk path %s: %v", string(jsonValue.Bytes()), err1)
			return err1
		}

		//Replace the exist IPAddr
		var Vers []zkVersIP
		for _, pv := range zkServID.Vers {
			if pv.Ver != version {
				Vers = append(Vers, pv)
			} else {
				if pv.IP == IPAddr {
					//do NOTHING zk has exist nodePath
					goto doNothing
				}
			}
		}
		zkServID.Vers = Vers
		//Add new IPAddr
		verNode := &zkVersIP{Ver: version, IP: IPAddr}
		zkServID.Vers = append(zkServID.Vers, *verNode)

		jsonIDVerIpAddr, err = json.Marshal(zkServID)
		if err != nil {
			log.Errorf("json.Marshal json: %s: %v", string(jsonIDVerIpAddr), err)
			return err
		}
		needNotify = true
	} else {

		zkVers := &zkVersIP{Ver:version, IP:IPAddr}
		zkServID := &zkServIDVers{}
		zkServID.ID = int64(serverId)
		zkServID.Vers = append(zkServID.Vers, *zkVers)
		jsonIDVerIpAddr, err = json.Marshal(zkServID)
		if err != nil {
			log.Errorf("json.Marshal json: %s: %v", string(jsonIDVerIpAddr), err)
			return err
		}
		//Add a new node to zk path will automatic notify, NO need manual do it
		needNotify = false
	}

	err = p.kv.Put(path, jsonIDVerIpAddr, &store.WriteOptions{TTL: p.UpdateInterval * 2})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", path, err)
		return err
	}

	if needNotify {
		err = p.notifyWatcher(p.BasePath, p.ServiceName)
		if err != nil {
			log.Errorf("cannot notifyWatcher zk path %s: %v", path, err)
			return err
		}
	}

doNothing:
	return nil
}

func (p *MyZkRegisterPlugin) delVerIPAddrJson(path string, version string) error {
	var exist bool
	var err error
	exist, err = p.kv.Exists(path)
	if err != nil {
		log.Errorf("Exist err zk path %s: %v", path, err)
		return err
	}

	if exist {

		var jsonIDVerIpAddr []byte
		//Find the ids list of json
		//ps, err1 := p.kv.List(nodePath)
		ps, err1 := p.kv.Get(path)
		if err1 != nil {
			log.Errorf("p.kv.List zk path %s: %v", path, err)
			return err1
		}
		//log.Errorf("ps.len %d", len(ps))
		var jsonValue bytes.Buffer
		jsonValue.Write(ps.Value)

		//Parse json to Struct
		var zkServID zkServIDVers
		err1 = json.Unmarshal(jsonValue.Bytes(), &zkServID)
		if err1 != nil {
			log.Errorf("json.Unmarshal zk path %s: %v", string(jsonValue.Bytes()), err1)
			return err1
		}

		//Replace the exist IPAddr
		var Vers []zkVersIP
		for _, pv := range zkServID.Vers {
			if pv.Ver != version {
				Vers = append(Vers, pv)
			}
		}

		if len(zkServID.Vers) == len(Vers) {
			goto doNothing
		}

		zkServID.Vers = Vers

		jsonIDVerIpAddr, err = json.Marshal(zkServID)
		if err != nil {
			log.Errorf("json.Marshal json: %s: %v", string(jsonIDVerIpAddr), err)
			return err
		}

		err = p.kv.Put(path, jsonIDVerIpAddr, &store.WriteOptions{TTL: p.UpdateInterval * 2})
		if err != nil {
			log.Errorf("cannot create zk path %s: %v", path, err)
			return err
		}
		err = p.notifyWatcher(p.BasePath, p.ServiceName)
		if err != nil {
			log.Errorf("cannot notifyWatcher zk path %s: %v", path, err)
			return err
		}
	}
doNothing:
	return nil
}

/*


BASE/L/ServerName/Json{}


*/

func (p *MyZkRegisterPlugin) Register(name string, rcvr interface{}, metadata string) (err error) {
	if "" == strings.TrimSpace(name) {
		err = errors.New("Register service `name` can't be empty")
		return
	}

	if p.kv == nil {
		zookeeper.Register()
		kv, err := libkv.NewStore(store.ZK, p.ZooKeeperServers, nil)
		if err != nil {
			log.Errorf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}

	if p.BasePath[0] == '/' {
		p.BasePath = p.BasePath[1:]
	}
	p.ServiceName = name

	/*
	err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", p.BasePath, err)
		return err
	}
	*/	
	//Create BASE Path
	err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", p.BasePath, err)
		return err
	}

	/*
		//Create BASE/L Path
		ServerNameFullPath := fmt.Sprintf("%s/%s", p.BasePath, p.ServerNamePath)
		err = p.kv.Put(ServerNameFullPath, p.ServerNamePath, &store.WriteOptions{IsDir: true})
		if err != nil {
			log.Errorf("cannot create zk path %s/%s: %v", p.BasePath, p.ServerNamePath, err)
			return err
		}

		//Create BASE/L/ServerName Path
		nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, p.ServerNamePath, name)
		err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
		if err != nil {
			log.Errorf("cannot create zk path %s: %v", nodePath, err)
			return err
		}
	*/
	//Create BASE/L/ServerName
	nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, p.serverNamePath, name)
	err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", nodePath, err)
		return err
	}

	//the ServerID/Version/IP:PortAddr will be a json
	//{"ID":1,"Vers":[{"Ver":"0.0.0.1","IP":"123.456.78.9"},{"Ver":"0.0.0.2","IP":"123.456.78.1"}]}

	//Create BASE/L/ServerName/ServerID Path
	nodePath = fmt.Sprintf("%s/%s/%s/%d", p.BasePath, p.serverNamePath, name, p.ServerID)

	err = p.addVerIPAddrJson(nodePath, p.ServerID, p.Version, p.ServiceAddress)
	if err != nil {
		log.Errorf("addVerIPAddrJson err zk path %s: %v", nodePath, err)
		return err
	}
/*
	var exist bool
	exist, err = p.kv.Exists(nodePath)
	if err != nil {
		log.Errorf("Exist err zk path %s: %v", nodePath, err)
		return err
	}
	var jsonIDVerIpAddr []byte

	if exist {
		//Find the ids list of json
		//ps, err1 := p.kv.List(nodePath)
		ps, err1 := p.kv.Get(nodePath)
		if err1 != nil {
			log.Errorf("p.kv.List zk path %s: %v", nodePath, err)
			return err1
		}
		//log.Errorf("ps.len %d", len(ps))
		var jsonValue bytes.Buffer
		jsonValue.Write(ps.Value)

		//Parse json to Struct
		var zkServID zkServIDVers
		err1 = json.Unmarshal(jsonValue.Bytes(), &zkServID)
		if err1 != nil {
			log.Errorf("json.Unmarshal zk path %s: %v", string(jsonValue.Bytes()), err1)
			return err1
		}

		//Replace the exist IPAddr
		var Vers []zkVersIP
		for _, pv := range zkServID.Vers {
			if pv.Ver != p.Version {
				Vers = append(Vers, pv)
			}
		}
		zkServID.Vers = Vers
		//Add new IPAddr
		verNode := &zkVersIP{Ver: p.Version, IP: p.ServiceAddress}
		zkServID.Vers = append(zkServID.Vers, *verNode)

		jsonIDVerIpAddr, err = json.Marshal(zkServID)
		if err != nil {
			log.Errorf("json.Marshal json: %s: %v", string(jsonIDVerIpAddr), err)
			return err
		}

	} else {

		zkVers := &zkVersIP{Ver:p.Version, IP:p.ServiceAddress}
		zkServID := &zkServIDVers{}
		zkServID.ID = int64(p.ServerID)
		zkServID.Vers = append(zkServID.Vers, *zkVers)
		jsonIDVerIpAddr, err = json.Marshal(zkServID)
		if err != nil {
			log.Errorf("json.Marshal json: %s: %v", string(jsonIDVerIpAddr), err)
			return err
		}
	}

	err = p.kv.Put(nodePath, jsonIDVerIpAddr, &store.WriteOptions{TTL: p.UpdateInterval * 2})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", nodePath, err)
		return err
	}
*/
	/*
	//Create BASE/L/ServerName/ServerID/Version
	nodePath = fmt.Sprintf("%s/%s/%s/%d/%s", p.BasePath, p.serverNamePath, name, p.ServerID, p.Version)

	//First Del the IP,Address note to make sure the PATH IS UNIQUE!!!
	err = p.kv.DeleteTree(nodePath)
	if err != nil {
		log.Warnf("DeleteTree zk path %s: %v", nodePath, err)
		err = nil
	}

	err = p.kv.Put(nodePath, []byte(p.Version), &store.WriteOptions{TTL: p.UpdateInterval * 2})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", nodePath, err)
		return err
	}

	//Create BASE/L/ServerName/ServerID/Version/IP:PortAddr Path
	nodePath = fmt.Sprintf("%s/%s/%s/%d/%s/%s", p.BasePath, p.serverNamePath, name, p.ServerID, p.Version, p.ServiceAddress)
	err = p.kv.Put(nodePath, []byte(p.ServiceAddress), &store.WriteOptions{TTL: p.UpdateInterval * 2})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", nodePath, err)
		return err
	}
	*/

/*
	//Create BASE/I Path
	ServerIDFullPath := fmt.Sprintf("%s/%s", p.BasePath, p.ServerIDPath)
	err = p.kv.Put(ServerIDFullPath, p.ServerIDPath, &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s/%s: %v", p.BasePath, p.ServerIDPath, err)
		return err
	}

	//Create BASE/I/ServerID Path
	nodePath = fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
	err = p.kv.Put(nodePath, []byte(metadata), &store.WriteOptions{TTL: p.UpdateInterval * 2})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", nodePath, err)
		return err
	}
*/

	Fname := nodePath + "#" + p.Version
	p.Services = append(p.Services, Fname)

	p.metasLock.Lock()
	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	p.metas[Fname] = Fname
	p.metasLock.Unlock()
	return
}

/*
	ID = -1 is a special key for NOTIFY watcher
*/

func (p *MyZkRegisterPlugin) notifyWatcher(base string, name string) error {

	nodePath := fmt.Sprintf("%s/%s/%s/%d", base, "L", name, -1)

	var exist bool
	var err error
	exist, err = p.kv.Exists(nodePath)
	if err != nil {
		log.Errorf("Exist err zk path %s: %v", nodePath, err)
		return err
	}

	if exist {
		err = p.kv.Delete(nodePath)
		if err != nil {
			log.Errorf("Delete err zk path %s: %v", nodePath, err)
			return err
		}
	} else {
		err = p.kv.Put(nodePath, []byte(""), &store.WriteOptions{IsDir: true})
		if err != nil {
			log.Errorf("Delete err zk path %s: %v", nodePath, err)
			return err
		}
	}

	return nil
}

func (p *MyZkRegisterPlugin) Unregister(name string) (err error) {
	if "" == strings.TrimSpace(name) {
		err = errors.New("Register service `name` can't be empty")
		return
	}

	if p.kv == nil {
		zookeeper.Register()
		kv, err := libkv.NewStore(store.ZK, p.ZooKeeperServers, nil)
		if err != nil {
			log.Errorf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}

	if p.BasePath[0] == '/' {
		p.BasePath = p.BasePath[1:]
	}
	err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", p.BasePath, err)
		return err
	}

	nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, p.serverNamePath, name)
	err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", nodePath, err)
		return err
	}

	nodePath = fmt.Sprintf("%s/%s/%s/%d", p.BasePath, p.serverNamePath, name, p.ServerID)

	err = p.delVerIPAddrJson(nodePath, p.Version)
	if err != nil {
		log.Errorf("cannot create consul path %s: %v", nodePath, err)
		return err
	}

	Fname := nodePath + "#" + p.Version
	var services = make([]string, 0, len(p.Services)-1)
	for _, s := range p.Services {
		if s != Fname {
			services = append(services, s)
		}
	}
	p.Services = services

	p.metasLock.Lock()
	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	delete(p.metas, Fname)
	p.metasLock.Unlock()
	return
}
