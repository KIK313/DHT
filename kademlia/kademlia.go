package kademlia

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	k     = 20
	alpha = 3
)

type TP struct {
	O  string
	Tp bool
}
type Pair struct {
	Key string
	Val string
}
type LRU struct {
	bucLock sync.RWMutex
	addr    [k]string
	siz     int
}
type Node struct {
	Ip        string
	Id        *big.Int
	server    *rpc.Server
	listener  net.Listener
	is_online bool
	dataLock  sync.RWMutex
	data      map[string]string
	repubLock sync.RWMutex
	repubtime map[string]int
	bucket    [160]*LRU
}

func get_hash(addr string) *big.Int {
	o := sha1.New()
	o.Write([]byte(addr))
	return (&big.Int{}).SetBytes(o.Sum((nil)))
}
func init() {
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}
func (p *Node) Init(addr string) {
	p.Ip = addr
	p.Id = get_hash(addr)
	p.data = make(map[string]string)
	p.repubtime = make(map[string]int)
	for i := 0; i < 160; i++ {
		var u LRU
		p.bucket[i] = &u
	}
}
func (p *Node) Run() {
	logrus.Infof("%s Run it", p.Ip)
	p.is_online = true
	go p.RunRPCServer()
}
func (p *Node) check_link(id int) {
	ss := ""
	for p.is_online {
		p.bucket[id].bucLock.Lock()
		for i := 0; i < p.bucket[id].siz; i++ {
			err := p.RemoteCall(p.bucket[id].addr[i], "Node.Ping", "", &ss)
			if err == nil {
				break
			}
			for j := i; j < p.bucket[id].siz-1; j++ {
				p.bucket[j] = p.bucket[j+1]
			}
			p.bucket[id].siz--
			i--
		}
		siz := p.bucket[id].siz
		p.bucket[id].bucLock.Unlock()
		if siz < 3 && siz > 0 && id >= 2 {
			if !p.is_online {
				return
			}
			p.bucket[id].bucLock.RLock()
			e := p.bucket[id].addr[0]
			p.bucket[id].bucLock.RUnlock()
			var g [k]string
			p.Lookup_Node(e, &g)
			for i := 0; i < k; i++ {
				if g[i] != "" {
					p.Insert(g[i], &ss)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (p *Node) con_check() {
	go func() {
		for p.is_online {
			p.dataLock.RLock()
			p.repubLock.Lock()
			s := make(map[string]string)
			for key, val := range p.repubtime {
				if val == 0 {
					s[key] = p.data[key]
				} else {
					p.repubtime[key]--
				}
			}
			p.repubLock.Unlock()
			p.dataLock.RUnlock()
			for key, val := range s {
				p.Republish(Pair{key, val}, true)
			}
			p.repubLock.Lock()
			for key := range s {
				p.repubtime[key] = 3
			}
			p.repubLock.Unlock()
			time.Sleep(time.Second)
		}
	}()
	for i := 159; i >= 0; i-- {
		go p.check_link(i)
	}
}
func (p *Node) Create() {
	go p.con_check()
}
func (p *Node) Ping(_ string, _ *string) error {
	return nil
}
func (p *Node) Insert(addr string, _ *string) error {
	if p.Ip == addr {
		return nil
	}
	v := get_hash(addr)
	id := -1
	for i := 159; i >= 0; i-- {
		if v.Bit(i) == p.Id.Bit(i) {
			continue
		}
		id = i
		break
	}
	if id == -1 {
		return nil
	}
	o := p.bucket[id]
	o.bucLock.RLock()
	for i := 0; i < o.siz; i++ {
		if o.addr[i] == addr {
			o.bucLock.RUnlock()
			return nil
		}
	}
	//	fmt.Printf("%d  HHH\n", o.siz)
	o.bucLock.RUnlock()
	o.bucLock.Lock()
	if o.siz < k {
		o.addr[o.siz] = addr
		o.siz++
	} else {
		ss := ""
		err := p.RemoteCall(o.addr[0], "Node.Ping", "", &ss)
		if err == nil {
			tmp := o.addr[0]
			for i := 0; i < k-1; i++ {
				o.addr[i] = o.addr[i+1]
			}
			o.addr[k-1] = tmp
		} else {
			for i := 0; i < k-1; i++ {
				o.addr[i] = o.addr[i+1]
			}
			o.addr[k-1] = addr
		}
	}
	o.bucLock.Unlock()
	return nil
}
func (p *Node) Join(addr string) bool {
	ss := ""
	p.Insert(addr, &ss)
	var g [k]string
	p.RemoteCall(addr, "Node.Lookup_Node", p.Ip, &g)
	fmt.Printf("%s %s\n", p.Ip, addr)
	for i := 0; i < k; i++ {
		if g[i] != "" {
			p.Insert(g[i], &ss)
			p.RemoteCall(g[i], "Node.Insert", p.Ip, &ss)
		}

	}
	return true
}
func (p *Node) Find_Node(o string, ls *[k]string) error {
	v := get_hash(o)
	cnt := k
	id := 0
	ss := ""
	for i := 159; i >= 0 && cnt > 0; i-- {
		if v.Bit(i) == p.Id.Bit(i) {
			continue
		}
		p.bucket[i].bucLock.RLock()
		for j := 0; j < p.bucket[i].siz && cnt > 0; j++ {
			err := p.RemoteCall(p.bucket[i].addr[j], "Node.Insert", o, &ss)
			if err == nil {
				p.RemoteCall(o, "Node.Insert", p.bucket[i].addr[j], &ss)
				(*ls)[id] = p.bucket[i].addr[j]
				id++
				cnt--
			} else {
				p.bucket[i].bucLock.RUnlock()
				p.bucket[i].bucLock.Lock()
				for l := j; l < p.bucket[i].siz-1; l++ {
					p.bucket[i].addr[l] = p.bucket[i].addr[l+1]
				}
				p.bucket[i].siz--
				j--
				p.bucket[i].bucLock.Unlock()
				p.bucket[i].bucLock.RLock()
			}
		}
		p.bucket[i].bucLock.RUnlock()
	}
	if cnt > 0 {
		cnt--
		(*ls)[id] = p.Ip
		id++
		for i := 0; i < 160 && cnt > 0; i++ {
			if v.Bit(i) == p.Id.Bit(i) {
				p.bucket[i].bucLock.RLock()
				for j := 0; j < p.bucket[i].siz && cnt > 0; j++ {
					err := p.RemoteCall(p.bucket[i].addr[j], "Node.Insert", o, &ss)
					if err == nil {
						(*ls)[id] = p.bucket[i].addr[j]
						id++
						cnt--
					} else {
						p.bucket[i].bucLock.RUnlock()
						p.bucket[i].bucLock.Lock()
						for l := j; l < p.bucket[i].siz-1; l++ {
							p.bucket[i].addr[l] = p.bucket[i].addr[l+1]
						}
						p.bucket[i].siz--
						j--
						p.bucket[i].bucLock.Unlock()
						p.bucket[i].bucLock.RLock()
					}
				}
				p.bucket[i].bucLock.RUnlock()
			}
		}
	}
	if o != p.Ip {
		p.Insert(o, &ss)
		p.RemoteCall(o, "Node.Insert", p.Ip, &ss)
	}
	return nil
}
func (p *Node) Find_Val(o TP, ls *[k]string) error {
	if o.Tp {
		p.dataLock.RLock()
		dd, ok := p.data[o.O]
		if ok == true {
			(*ls)[0] = "OK"
			(*ls)[1] = dd
			p.dataLock.RUnlock()
			return nil
		}
		p.dataLock.RUnlock()
	}
	v := get_hash(o.O)
	cnt := k
	id := 0
	ss := ""
	for i := 159; i >= 0 && cnt > 0; i-- {
		if v.Bit(i) == p.Id.Bit(i) {
			continue
		}
		p.bucket[i].bucLock.RLock()
		for j := 0; j < p.bucket[i].siz && cnt > 0; j++ {
			err := p.RemoteCall(p.bucket[i].addr[j], "Node.Ping", "", &ss)
			if err == nil {
				(*ls)[id] = p.bucket[i].addr[j]
				id++
				cnt--
			}
		}
		p.bucket[i].bucLock.RUnlock()
	}
	if cnt > 0 {
		cnt--
		(*ls)[id] = p.Ip
		id++
		for i := 0; i < 160 && cnt > 0; i++ {
			if v.Bit(i) == p.Id.Bit(i) {
				p.bucket[i].bucLock.RLock()
				for j := 0; j < p.bucket[i].siz && cnt > 0; j++ {
					err := p.RemoteCall(p.bucket[i].addr[j], "Node.Ping", "", &ss)
					if err == nil {
						(*ls)[id] = p.bucket[i].addr[j]
						id++
						cnt--
					}
				}
				p.bucket[i].bucLock.RUnlock()
			}
		}
	}
	return nil
}
func (p *Node) Lookup_Node(o string, ls *[k]string) error {
	var g [k]string
	addr := make([]string, 0)
	dis := make([]*big.Int, 0)
	p.Find_Node(o, &g)
	AP := make(map[string]bool)
	for i := 0; i < k; i++ {
		if g[i] != "" {
			addr = append(addr, g[i])
			dis = append(dis, get_dis(g[i], o))
			AP[g[i]] = true
		}
	}
	M := make(map[string]bool)

	for j := 0; j < 25; j++ {
		id := -1
		for i := 0; i < len(addr); i++ {
			_, ok := M[addr[i]]
			if !ok {
				M[addr[i]] = true
				id = i
				break
			}
		}
		if id == -1 {
			break
		}
		var s [k]string
		err := p.RemoteCall(addr[id], "Node.Find_Node", o, &s)
		if err == nil {
			for i := 0; i < k; i++ {
				if s[i] != "" {
					_, ok := AP[s[i]]
					if ok != true {
						AP[s[i]] = true
						addr = append(addr, s[i])
						dis = append(dis, get_dis(s[i], o))
					}
				}
			}
		} else {
			M[addr[id]] = false
		}
	}
	cnt := 0
	ss := ""
	var pos [k]int
	for i := 0; i < len(addr); i++ {
		if cnt == k && dis[pos[k-1]].Cmp(dis[i]) >= 0 {
			continue
		}
		vv, ok := M[addr[i]]
		if ok != true {
			err := p.RemoteCall(addr[i], "Node.Ping", "", &ss)
			if err != nil {
				continue
			}
		} else {
			if vv == false {
				continue
			}
		}
		if cnt < k {
			(*ls)[cnt] = addr[i]
			pos[cnt] = i
			cnt++
		} else {
			(*ls)[k-1] = addr[i]
			pos[k-1] = i
		}
		for j := cnt - 1; j > 0; j-- {
			if dis[pos[j]].Cmp(dis[pos[j-1]]) < 0 {
				tmp := (*ls)[j]
				(*ls)[j] = (*ls)[j-1]
				(*ls)[j-1] = tmp
				pos[j] += pos[j-1]
				pos[j-1] = pos[j] - pos[j-1]
				pos[j] = pos[j] - pos[j-1]
			} else {
				break
			}
		}
	}
	return nil
}
func get_dis(s string, t string) *big.Int {
	r := get_hash(s)
	w := get_hash(t)
	ans := big.NewInt(0)
	for i := 159; i >= 0; i-- {
		if r.Bit(i) != w.Bit(i) {
			ans = ans.SetBit(ans, i, 1)
		}
	}
	return ans
}
func (p *Node) Lookup_Val(o TP, ls *[k]string) error {
	var g [k]string
	addr := make([]string, 0)
	dis := make([]*big.Int, 0)
	p.Find_Val(o, &g)
	if o.Tp {
		if g[0] == "OK" {
			(*ls)[0] = "OK"
			(*ls)[1] = g[1]
			return nil
		}
	}
	AP := make(map[string]bool)
	for i := 0; i < k; i++ {
		if g[i] != "" {
			addr = append(addr, g[i])
			dis = append(dis, get_dis(g[i], o.O))
			AP[g[i]] = true
		}
	}
	M := make(map[string]bool)

	for j := 0; j < 25; j++ {
		id := -1
		for i := 0; i < len(addr); i++ {
			_, ok := M[addr[i]]
			if !ok {
				M[addr[i]] = true
				id = i
				break
			}
		}
		if id == -1 {
			break
		}
		var s [k]string
		err := p.RemoteCall(addr[id], "Node.Find_Val", o, &s)
		if err == nil {
			if s[0] == "OK" {
				(*ls)[0] = "OK"
				(*ls)[1] = s[1]
				return nil
			}
			for i := 0; i < k; i++ {
				if s[i] != "" {
					_, ok := AP[s[i]]
					if ok != true {
						AP[s[i]] = true
						addr = append(addr, s[i])
						dis = append(dis, get_dis(s[i], o.O))
					}
				}
			}
		} else {
			M[addr[id]] = false
		}
	}
	cnt := 0
	ss := ""
	var pos [k]int
	for i := 0; i < len(addr); i++ {
		if cnt == k && dis[pos[k-1]].Cmp(dis[i]) >= 0 {
			continue
		}
		vv, ok := M[addr[i]]
		if ok != true {
			err := p.RemoteCall(addr[i], "Node.Ping", "", &ss)
			if err != nil {
				continue
			}
		} else {
			if vv == false {
				continue
			}
		}
		if cnt < k {
			(*ls)[cnt] = addr[i]
			pos[cnt] = i
			cnt++
		} else {
			(*ls)[k-1] = addr[i]
			pos[k-1] = i
		}
		for j := cnt - 1; j > 0; j-- {
			if dis[pos[j]].Cmp(dis[pos[j-1]]) < 0 {
				tmp := (*ls)[j]
				(*ls)[j] = (*ls)[j-1]
				(*ls)[j-1] = tmp
				pos[j] += pos[j-1]
				pos[j-1] = pos[j] - pos[j-1]
				pos[j] = pos[j] - pos[j-1]
			} else {
				break
			}
		}
	}
	return nil
}
func (p *Node) Store(o Pair, _ *string) error {
	p.dataLock.Lock()
	p.data[o.Key] = o.Val
	p.dataLock.Unlock()
	p.repubLock.Lock()
	p.repubtime[o.Key] = 3
	p.repubLock.Unlock()
	return nil
}
func (p *Node) Republish(o Pair, flag bool) {
	var g [k]string
	p.Lookup_Val(TP{o.Key, false}, &g)
	ss := ""
	for i := 0; i < k; i++ {
		if g[i] == "" {
			continue
		}
		err := p.RemoteCall(g[i], "Node.Store", o, &ss)
		if flag {
			if err == nil {
				p.Insert(g[i], &ss)
				p.RemoteCall(g[i], "Node.Insert", p.Ip, &ss)
			}
		}
	}
}
func (p *Node) Quit() {
	if !p.is_online {
		return
	}

	p.dataLock.RLock()
	s := make(map[string]string)
	for key, val := range p.data {
		s[key] = val
	}
	p.dataLock.RUnlock()
	fmt.Printf("ST quit\n")
	var wt sync.WaitGroup
	wt.Add(len(s))
	for key, val := range s {
		go func(k string, v string) {
			p.Republish(Pair{k, v}, false)
			wt.Done()
		}(key, val)
	}
	wt.Wait()
	fmt.Printf("Quit it\n")
	logrus.Infof("Success Quit")
	p.is_online = false
	p.listener.Close()
}
func (p *Node) ForceQuit() {
	if !p.is_online {
		return
	}
	logrus.Infof("Success ForceQuit")
	p.is_online = false
	p.listener.Close()
}
func (p *Node) Put(key string, value string) bool {
	logrus.Infof("Try to put %s %s", key, value)
	var g [k]string
	p.Lookup_Val(TP{key, false}, &g)
	ss := ""
	for i := 0; i < k; i++ {
		if g[i] == "" {
			continue
		}
		p.RemoteCall(g[i], "Node.Store", Pair{key, value}, &ss)
	}
	return true
}
func (p *Node) GetVal(o string, rep *string) error {
	p.dataLock.RLock()
	*rep = p.data[o]
	p.dataLock.RUnlock()
	return nil
}
func (p *Node) Get(key string) (bool, string) {
	var g [k]string
	err := p.Lookup_Val(TP{key, true}, &g)
	if err != nil {
		logrus.Infof("Lookup_Val Fail when getting %v", err)
		return false, ""
	}
	if g[0] == "OK" {
		return true, g[1]
	}
	ss := ""
	id := 0
	for i := 0; i < k && ss == ""; i++ {
		p.RemoteCall(g[i], "Node.GetVal", key, &ss)
		id = i
	}
	if ss == "" {
		fmt.Printf("GG\n")
	} else {
		for i := 0; i < id; i++ {
			p.RemoteCall(g[i], "Node.Store", Pair{key, ss}, &ss)
		}
	}
	//	fmt.Printf("Find %s Get %s\n", key, ss)
	return true, ss
}
func (p *Node) Delete(key string) bool {
	return true
}
func (p *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	//	logrus.Infof("%s Try to Call %s %s", p.Ip, addr, method)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		//		logrus.Infof("Fail to Dail %v", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		//		logrus.Infof("Fail to Call %v", err)
		return err
	}
	//	logrus.Infof("%s Success RemoteCall %s  %s", p.Ip, addr, method)
	return nil
}
func (p *Node) RunRPCServer() {
	p.server = rpc.NewServer()
	err := p.server.Register(p)
	if err != nil {
		logrus.Infof("Register Fail %v", err)
		return
	}
	logrus.Infof("Register Success")
	p.listener, err = net.Listen("tcp", p.Ip)
	if err != nil {
		logrus.Infof("Listen Fail %v", err)
		return
	}
	for p.is_online {
		conn, err := p.listener.Accept()
		if err != nil {
			logrus.Infof("Accept Fail %v", err)
			return
		}
		go func() {
			p.server.ServeConn(conn)
			conn.Close()
		}()
	}
}
