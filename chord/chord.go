package chord

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

const sl = 6

type Name struct {
	Ip string
	Id *big.Int
}
type Qs struct {
	Pre    string
	Trdata map[string]string
}
type Ppair struct {
	Key string
	Val string
}
type Mmap struct {
	M map[string]string
	B map[string]string
}
type Node struct {
	pos          Name
	data         map[string]string
	dataLock     sync.RWMutex
	copy_data    map[string]string
	codataLock   sync.RWMutex
	Finger_Table [160]Name
	FingerLock   sync.RWMutex
	SucList      [6]Name
	SucLock      sync.RWMutex
	pre          Name
	preLock      sync.RWMutex
	is_online    bool
	onlineLock   sync.RWMutex
	listener     net.Listener
	server       *rpc.Server
	Finger_St    [160]*big.Int
	cur_fix      int
	cur_suc      int
	concheckLock sync.Mutex
}

func init() {
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}
func get_hash(addr string) *big.Int {
	o := sha1.New()
	o.Write([]byte(addr))
	return (&big.Int{}).SetBytes(o.Sum((nil)))
}
func (p *Node) Init(addr string) {
	p.pos.Ip = addr
	p.pos.Id = get_hash(addr)
	p.data = make(map[string]string)
	p.copy_data = make(map[string]string)
	p.cur_fix = 1
	o := big.NewInt(1)
	h := big.NewInt(1)
	for i := 0; i < 160; i++ {
		p.Finger_St[i] = big.NewInt(1)
		p.Finger_St[i].Add(p.pos.Id, o)
		h.Add(o, o)
		o = h
	}
	for i := 0; i < 160; i++ {
		if o.Cmp(p.Finger_St[i]) <= 0 {
			p.Finger_St[i].Sub(p.Finger_St[i], o)
		}
	}
}
func (p *Node) Run() {
	p.is_online = true
	go p.RunRPCServer()
}
func (p *Node) Get_Next(_ string, rp *Name) error {
	p.FingerLock.RLock()
	u := p.Finger_Table[0].Ip
	p.FingerLock.RUnlock()
	if u == "" {
		rp.Ip = ""
		return nil
	}
	ss := ""
	err := p.RemoteCall(u, "Node.Ping", "", &ss)
	if err != nil {
		rp.Ip = ""
		return nil
	}
	rp.Ip = u
	rp.Id = get_hash(rp.Ip)
	return nil
}
func (p *Node) Get_Pre(_ string, rp *Name) error {
	p.preLock.RLock()
	if p.pre.Ip == "" {
		rp.Ip = ""
		p.preLock.RUnlock()
		return nil
	}
	ss := ""
	err := p.RemoteCall(p.pre.Ip, "Node.Ping", "", &ss)
	if err != nil {
		rp.Ip = ""
		p.preLock.RUnlock()
		return nil
	}
	rp.Ip = p.pre.Ip
	rp.Id = get_hash(rp.Ip)
	p.preLock.RUnlock()
	return nil
}
func (p *Node) Notify_Suc(o Name, _ *string) error {
	p.FingerLock.RLock()
	w := p.Finger_Table[0].Ip
	p.FingerLock.RUnlock()
	if w == "" || w == p.pos.Ip {
		p.FingerLock.Lock()
		p.Finger_Table[0].Ip = o.Ip
		p.Finger_Table[0].Id = get_hash(o.Ip)
		p.FingerLock.Unlock()
		p.SucLock.Lock()
		p.SucList[0].Ip = o.Ip
		p.SucList[0].Id = get_hash(o.Ip)
		p.SucLock.Unlock()
		return nil
	}
	if checklr(p.pos.Id, o.Id, get_hash(w)) {
		p.FingerLock.Lock()
		p.Finger_Table[0].Ip = o.Ip
		p.Finger_Table[0].Id = get_hash(o.Ip)
		p.FingerLock.Unlock()
		p.SucLock.Lock()
		p.SucList[0].Ip = o.Ip
		p.SucList[0].Id = get_hash(o.Ip)
		p.SucLock.Unlock()
		return nil
	}
	return nil
}
func (p *Node) Notify_Pre(o Name, _ *string) error {
	p.preLock.RLock()
	if p.pre.Ip == "" {
		p.preLock.RUnlock()
		p.preLock.Lock()
		p.pre.Ip = o.Ip
		p.pre.Id = get_hash(o.Ip)
		p.preLock.Unlock()
		return nil
	}
	if p.pre.Ip == o.Ip {
		p.preLock.RUnlock()
		return nil
	}
	if checklr(p.pre.Id, o.Id, p.pos.Id) {
		p.preLock.RUnlock()
		p.preLock.Lock()
		p.pre.Ip = o.Ip
		p.pre.Id = get_hash(o.Ip)
		p.preLock.Unlock()
	} else {
		p.preLock.RUnlock()
	}
	return nil
}
func (p *Node) Check_pre() {
	p.concheckLock.Lock()
	defer p.concheckLock.Unlock()
	p.preLock.RLock()
	e := p.pre.Ip
	p.preLock.RUnlock()
	if e == "" || e == p.pos.Ip {
		return
	}
	var s Name
	err := p.RemoteCall(e, "Node.Get_Next", "", &s)
	if err != nil {
		p.preLock.Lock()
		p.pre.Ip = ""
		p.preLock.Unlock()
		return
	}
	p.preLock.Lock()
	if s.Ip != "" && checklr(p.pre.Id, s.Id, p.pos.Id) {
		p.pre.Ip = s.Ip
		p.pre.Id = get_hash(s.Ip)
	}
	p.preLock.Unlock()
	p.preLock.RLock()
	e = p.pre.Ip
	p.preLock.RUnlock()
	ss := ""
	p.RemoteCall(e, "Node.Notify_Suc", p.pos, &ss)
}
func (p *Node) Check_suc() {
	p.concheckLock.Lock()
	defer p.concheckLock.Unlock()
	var r bool
	p.SucLock.RLock()
	e := p.SucList[0].Ip
	p.SucLock.RUnlock()
	if e == "" {
		r = true
	} else {
		ss := ""
		err := p.RemoteCall(e, "Node.Ping", "", &ss)
		if err != nil {
			r = true
		}
	}
	if r {
		id := 1
		ss := ""
		p.SucLock.RLock()
		u := p.SucList[id].Ip
		p.SucLock.RUnlock()
		for true {
			var err error
			if u != "" {
				err = p.RemoteCall(u, "Node.Ping", "", &ss)
			}
			if err == nil && u != "" {
				break
			}
			id++
			if id == 6 {
				p.FingerLock.Lock()
				p.Finger_Table[0].Ip = p.pos.Ip
				p.Finger_Table[0].Id = get_hash(p.pos.Ip)
				p.FingerLock.Unlock()
				p.preLock.Lock()
				p.pre.Ip = p.pos.Ip
				p.preLock.Unlock()
				p.SucLock.Lock()
				p.SucList[0].Ip = p.pos.Ip
				p.SucLock.Unlock()
				return
			}
			p.SucLock.RLock()
			u = p.SucList[id].Ip
			p.SucLock.RUnlock()
		}
		if u != p.pos.Ip {
			p.dataLock.RLock()
			r := make(map[string]string)
			for key, val := range p.data {
				r[key] = val
			}
			p.dataLock.RUnlock()
			p.RemoteCall(u, "Node.QuitWork_Suc", r, &ss)
		}
		p.SucLock.Lock()
		p.SucList[0].Ip = u
		p.SucList[0].Id = get_hash(u)
		p.SucLock.Unlock()
		p.FingerLock.Lock()
		p.Finger_Table[0].Ip = u
		p.Finger_Table[0].Id = get_hash(u)
		p.FingerLock.Unlock()
	}
	p.SucLock.RLock()
	e = p.SucList[0].Ip
	p.SucLock.RUnlock()
	if e == p.pos.Ip {
		return
	}
	var s Name
	err := p.RemoteCall(e, "Node.Get_Pre", "", &s)
	if s.Ip != "" && checklr(p.pos.Id, s.Id, get_hash(e)) {
		p.FingerLock.Lock()
		p.Finger_Table[0].Ip = s.Ip
		p.Finger_Table[0].Id = get_hash(s.Ip)
		p.FingerLock.Unlock()
		p.SucLock.Lock()
		p.SucList[0].Ip = s.Ip
		p.SucList[0].Id = get_hash(s.Ip)
		p.SucLock.Unlock()
	}
	p.FingerLock.RLock()
	e = p.Finger_Table[0].Ip
	p.FingerLock.RUnlock()
	var o [6]string
	err = p.RemoteCall(e, "Node.CopySL", "", &o)
	if err == nil {
		p.SucLock.Lock()
		for i := 1; i < 6; i++ {
			p.SucList[i].Ip = o[i-1]
			p.SucList[i].Id = get_hash(o[i-1])
		}
		p.SucLock.Unlock()
		ss := ""
		p.RemoteCall(e, "Node.Notify_Pre", p.pos, &ss)
	}
}
func (p *Node) Fix_finger() {
	p.FingerLock.RLock()
	var id int
	for i := 1; i < 160; i++ {
		if p.Finger_Table[i].Ip == "" {
			id = i
			break
		}
	}
	p.FingerLock.RUnlock()
	if id != 0 {
		var s string
		p.FindDesSuc(p.Finger_St[id], &s)
		p.FingerLock.Lock()
		p.Finger_Table[id].Ip = s
		p.Finger_Table[id].Id = get_hash(s)
		p.FingerLock.Unlock()
	}
	var s string
	p.FindDesSuc(p.Finger_St[p.cur_fix], &s)
	p.FingerLock.Lock()
	p.Finger_Table[p.cur_fix] = Name{s, get_hash(s)}
	p.FingerLock.Unlock()
	p.cur_fix = p.cur_fix + 1
	if p.cur_fix == 160 {
		p.cur_fix = 1
	}
}
func (p *Node) Fix_suc() {
	p.SucLock.RLock()
	e := p.SucList[p.cur_suc].Ip
	p.SucLock.RUnlock()
	var t Name
	err := p.RemoteCall(e, "Node.Get_Next", "", &t)
	if err == nil && t.Ip != "" {
		p.SucLock.Lock()
		p.SucList[p.cur_suc+1].Ip = t.Ip
		p.SucList[p.cur_suc+1].Id = get_hash(t.Ip)
		p.SucLock.Unlock()
		p.cur_suc = (p.cur_suc + 1) % 5
	} else {
		p.cur_suc = 0
	}
}
func (p *Node) Con_Check() {
	go func() {
		for p.is_online {
			p.Check_pre()
			time.Sleep(20 * time.Millisecond)
		}
	}()
	go func() {
		for p.is_online {
			p.Check_suc()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	go func() {
		for p.is_online {
			p.Fix_finger()
			time.Sleep(20 * time.Millisecond)
		}
	}()
	go func() {
		for p.is_online {
			p.Fix_suc()
			time.Sleep(20 * time.Millisecond)
		}
	}()
}
func (p *Node) Create() {
	logrus.Infof("Create: %s", p.pos.Ip)
	for i := 0; i < 160; i++ {
		p.Finger_Table[i] = p.pos
	}
	for i := 0; i < 6; i++ {
		p.SucList[i] = p.pos
	}
	p.pre = p.pos
	go p.Con_Check()
}
func checklR(a, b, c *big.Int) bool {
	sig := a.Cmp(c)
	if sig == 0 {
		return false
	}
	if sig < 0 {
		s1 := a.Cmp(b)
		if s1 >= 0 {
			return false
		}
		s1 = b.Cmp(c)
		if s1 <= 0 {
			return true
		} else {
			return false
		}
	}
	if sig > 0 {
		s1 := a.Cmp(b)
		s2 := b.Cmp(c)
		if s2 > 0 && s1 >= 0 {
			return false
		} else {
			return true
		}
	}
	return true
}
func checklr(a, b, c *big.Int) bool {
	sig := a.Cmp(c)
	if sig == 0 {
		return false
	}
	if sig < 0 {
		s1 := a.Cmp(b)
		if s1 >= 0 {
			return false
		}
		s1 = b.Cmp(c)
		if s1 < 0 {
			return true
		} else {
			return false
		}
	}
	if sig > 0 {
		s1 := a.Cmp(b)
		s2 := b.Cmp(c)
		if s1 >= 0 && s2 >= 0 {
			return false
		} else {
			return true
		}
	}
	return true
}

// FindDesSuc
func (p *Node) FindDesSuc(Id *big.Int, des *string) error {
	p.FingerLock.RLock()
	sc := p.Finger_Table[0].Ip
	p.FingerLock.RUnlock()
	if p.pos.Ip == sc {
		*des = p.pos.Ip
		return nil
	}
	if checklR(p.pos.Id, Id, get_hash(sc)) {
		// p.successor might be unexisted there is something to be fixed
		ss := ""
		err := p.RemoteCall(sc, "Node.Ping", "", &ss)
		if err == nil {
			*des = sc
		} else {
			*des = p.pos.Ip
		}
		return nil
	}
	if p.pos.Id.Cmp(Id) == 0 {
		*des = p.pos.Ip
		return nil
	}
	cnt := 0
	var e [160]int
	for i := 159; i >= 0; i-- {
		p.FingerLock.Lock()
		o := p.Finger_Table[i].Ip
		p.FingerLock.Unlock()
		if o == "" {
			continue
		}
		if checklr(p.pos.Id, get_hash(o), Id) {
			err := p.RemoteCall(o, "Node.FindDesSuc", Id, des)
			if err == nil {
				p.FingerLock.Lock()
				for j := 0; j < cnt; j++ {
					p.Finger_Table[e[j]].Ip = ""
				}
				p.FingerLock.Unlock()
				return nil
			} else {
				e[cnt] = i
				cnt++
			}
		}
	}
	p.FingerLock.Lock()
	for i := 0; i < cnt; i++ {
		p.Finger_Table[e[i]].Ip = ""
	}
	p.FingerLock.Unlock()
	for i := 0; i < 6; i++ {
		p.SucLock.RLock()
		r := p.SucList[i].Ip
		p.SucLock.RUnlock()
		if r == "" {
			continue
		}
		if !checklR(p.pos.Id, Id, get_hash(r)) {
			continue
		}
		ss := ""
		err := p.RemoteCall(r, "Node.Ping", "", &ss)
		if err == nil {
			*des = r
			return nil
		}
	}
	*des = p.pos.Ip
	return nil
}

// FindDesPre  RLock + Lock
func (p *Node) FindDesPre(Id *big.Int, des *string) error {
	p.FingerLock.RLock()
	d := p.Finger_Table[0].Ip
	p.FingerLock.RUnlock()
	if p.pos.Ip == d {
		*des = p.pos.Ip
		return nil
	}
	if checklR(p.pos.Id, Id, get_hash(d)) {
		*des = p.pos.Ip
		return nil
	}
	cnt := 0
	e := make([]int, 160)
	for i := 159; i >= 0; i-- {
		p.FingerLock.RLock()
		o := p.Finger_Table[i].Ip
		p.FingerLock.RUnlock()
		if o == "" {
			continue
		}
		if checklR(p.pos.Id, get_hash(o), Id) {
			err := p.RemoteCall(o, "Node.FindDesPre", Id, des)
			if err == nil {
				p.FingerLock.Lock()
				for j := 0; j < cnt; j++ {
					if e[j] != 0 {
						p.Finger_Table[e[j]].Ip = ""
					}
				}
				p.FingerLock.Unlock()
				return nil
			}
		}
	}
	p.FingerLock.Lock()
	for i := 0; i < cnt; i++ {
		if e[i] != 0 {
			p.Finger_Table[e[i]].Ip = ""
		}
	}
	p.FingerLock.Unlock()
	*des = p.pos.Ip
	return nil
}
func (p *Node) CopySL(_ string, o *[6]string) error {
	p.SucLock.RLock()
	for i := 0; i < 6; i++ {
		(*o)[i] = p.SucList[i].Ip
	}
	p.SucLock.RUnlock()
	return nil
}
func (p *Node) CopyFT(_ string, o *[160]string) error {
	p.FingerLock.RLock()
	for i := 0; i < 160; i++ {
		(*o)[i] = p.Finger_Table[i].Ip
	}
	p.FingerLock.RUnlock()
	return nil
}
func (p *Node) Rmvdata(o map[string]string, _ *string) error {
	p.codataLock.Lock()
	for key := range o {
		delete(p.copy_data, key)
	}
	p.codataLock.Unlock()
	return nil
}
func (p *Node) Movedata(addr string, o *Mmap) error {
	o.B = make(map[string]string)
	o.M = make(map[string]string)
	p.codataLock.RLock()
	for key, val := range p.copy_data {
		((*o).B)[key] = val
	}
	p.codataLock.RUnlock()
	p.codataLock.Lock()
	p.copy_data = make(map[string]string)
	p.codataLock.Unlock()
	Id := get_hash(addr)
	p.dataLock.RLock()
	for key, val := range p.data {
		if !checklr(Id, get_hash(key), p.pos.Id) {
			o.M[key] = val
		}
	}
	p.dataLock.RUnlock()
	p.codataLock.Lock()
	p.dataLock.Lock()
	for key, val := range o.M {
		p.copy_data[key] = val
		delete(p.data, key)
	}
	p.dataLock.Unlock()
	p.codataLock.Unlock()
	p.SucLock.RLock()
	h := p.SucList[0].Ip
	p.SucLock.RUnlock()
	ss := ""
	p.RemoteCall(h, "Node.Rmvdata", o.M, &ss)
	return nil
}
func (p *Node) Join(addr string) bool {
	var suc string
	var pre string
	logrus.Infof("Try to Join  %s %s", p.pos.Ip, addr)
	err := p.RemoteCall(addr, "Node.FindDesSuc", p.pos.Id, &suc)
	logrus.Infof("Join Step1 %s %s", p.pos.Ip, addr)
	if err != nil {
		logrus.Infof("Join Fail when finding Suc : %s %s", p.pos.Ip, addr)
		return false
	}
	err = p.RemoteCall(addr, "Node.FindDesPre", p.pos.Id, &pre)
	logrus.Infof("Join Step2 %s %s", p.pos.Ip, addr)
	if err != nil {
		logrus.Infof("Join Fail when finding Pre: %s %s", p.pos.Ip, addr)
		return false
	}
	// err = p.RemoteCall(suc, "Node.LinkPre", p.pos, "")
	// if err != nil {
	// 	logrus.Infof("Join Fail when linking suc %s %s", suc, p.pos.Ip)
	// 	return false
	// }
	// err = p.RemoteCall(pre, "Node.LinkSuc", p.pos, "")
	// if err != nil {
	// 	logrus.Infof("Join Fail when linking pre %s %s", pre, p.pos.Id)
	// 	return false
	// }
	logrus.Infof("Join Success %s", p.pos.Ip)
	var o [160]string
	p.RemoteCall(suc, "Node.CopyFT", "", &o)
	p.FingerLock.Lock()
	p.Finger_Table[0].Ip = suc
	p.Finger_Table[0].Id = get_hash(suc)
	for i := 1; i < 160; i++ {
		p.Finger_Table[i].Ip = o[i]
		p.Finger_Table[i].Id = get_hash(o[i])
	}
	p.FingerLock.Unlock()
	var sl [6]string
	p.RemoteCall(suc, "Node.CopySL", "", &sl)
	p.SucLock.Lock()
	p.SucList[0].Ip = suc
	p.SucList[0].Id = get_hash(suc)
	for i := 1; i < 6; i++ {
		p.SucList[i].Ip = sl[i-1]
		p.SucList[i].Id = get_hash(sl[i-1])
	}
	p.SucLock.Unlock()
	p.preLock.Lock()
	p.pre = Name{pre, get_hash(pre)}
	p.preLock.Unlock()
	ss := ""
	var gg Mmap
	gg.B = make(map[string]string)
	gg.M = make(map[string]string)
	p.RemoteCall(suc, "Node.Movedata", p.pos.Ip, &gg)
	p.dataLock.Lock()
	for key, value := range gg.M {
		p.data[key] = value
	}
	p.dataLock.Unlock()
	p.codataLock.Lock()
	for key, value := range gg.B {
		p.copy_data[key] = value
	}
	p.codataLock.Unlock()
	p.RemoteCall(suc, "Node.Notify_Pre", p.pos, &ss)
	p.RemoteCall(pre, "Node.Notify_Suc", p.pos, &ss)
	go p.Con_Check()
	return true
}

type Rec struct {
	conn net.Conn
	err  error
}

func (p *Node) RunRPCServer() {
	p.server = rpc.NewServer()
	oo := p.server.Register(p)
	if oo != nil {
		logrus.Infof("Reg Fail %v\n", oo)
		return
	}
	logrus.Infof("Register Success %s\n", p.pos.Ip)
	var err error
	p.listener, err = net.Listen("tcp", p.pos.Ip)
	if err != nil {
		logrus.Infof("Listen Fail: %s %v", p.pos.Ip, err)
		return
	}
	for p.is_online { // Does is_online need a lock ?
		conn, err := p.listener.Accept()
		if err != nil {
			logrus.Infof("Channel Accept Fail: %s %v", p.pos.Ip, err)
			//			conn.Close()
			return
		}
		go func() {
			p.server.ServeConn(conn)
			conn.Close()
		}()
	}
}
func (p *Node) Ping(_ string, _ *string) error {
	return nil
}

func (p *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	logrus.Infof("Tried to Call %s %s", addr, method)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		logrus.Infof("Fail to Dial: %s %s, %v", addr, method, err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Infof("Fail to Call: %s %s %v", addr, method, err)
		return err
	}
	logrus.Infof("Success RC: %s %s", addr, method)
	return nil
}
func (p *Node) QuitWork_Pre(o string, _ *string) error {
	p.FingerLock.Lock()
	p.Finger_Table[0].Ip = o
	p.Finger_Table[0].Id = get_hash(o)
	p.FingerLock.Unlock()
	p.SucLock.Lock()
	p.SucList[0].Ip = o
	p.SucList[0].Id = get_hash(o)
	p.SucLock.Unlock()
	return nil
}
func (p *Node) Trsusu(o map[string]string, _ *string) error {
	p.codataLock.Lock()
	for key, val := range o {
		p.copy_data[key] = val
	}
	p.codataLock.Unlock()
	return nil
}
func (p *Node) QuitWork_Suc(o map[string]string, _ *string) error {
	ss := ""
	p.codataLock.RLock()
	u := make(map[string]string)
	for key, value := range p.copy_data {
		u[key] = value
	}
	p.codataLock.RUnlock()
	p.SucLock.RLock()
	oo := p.SucList[0].Ip
	p.SucLock.RUnlock()
	p.RemoteCall(oo, "Node.Trsusu", u, &ss)
	p.dataLock.Lock()
	for key, val := range u {
		p.data[key] = val
	}
	p.dataLock.Unlock()
	p.codataLock.Lock()
	p.copy_data = make(map[string]string)
	for key, val := range o {
		p.copy_data[key] = val
	}
	p.codataLock.Unlock()
	return nil
}
func (p *Node) Pc(cnt int, r *string) error {
	if cnt > 20 {
		fmt.Printf("END\n\n")
		return nil
	}
	fmt.Printf("%s \n", p.pos.Ip)
	p.SucLock.RLock()
	for i := 0; i < 6; i++ {
		fmt.Printf("%s ", p.SucList[i].Ip)
	}
	o := p.SucList[0]
	p.SucLock.RUnlock()
	fmt.Printf("%s -> %s\n", p.pos.Ip, o.Ip)
	p.RemoteCall(o.Ip, "Node.Pc", cnt+1, r)
	return nil
}
func (p *Node) Lockit(_ string, _ *string) error {
	p.concheckLock.Lock()
	return nil
}
func (p *Node) Linkpre(s string, _ *string) error {
	p.preLock.Lock()
	p.pre.Ip = s
	p.pre.Id = get_hash(s)
	p.preLock.Unlock()
	return nil
}
func (p *Node) UL(_ string, _ *string) error {
	p.concheckLock.Unlock()
	return nil
}
func (p *Node) Alone(_string, _ *string) error {
	p.FingerLock.Lock()
	p.Finger_Table[0].Ip = p.pos.Ip
	p.Finger_Table[0].Id = get_hash(p.pos.Ip)
	p.FingerLock.Unlock()
	p.SucLock.Lock()
	p.SucList[0].Ip = p.pos.Ip
	p.SucList[0].Id = get_hash(p.pos.Ip)
	p.SucLock.Unlock()
	return nil
}
func (p *Node) Quit() {
	if !p.is_online {
		return
	}
	p.concheckLock.Lock()
	ss := ""
	p.SucLock.RLock()
	e := p.SucList[0].Ip
	p.SucLock.RUnlock()
	p.preLock.RLock()
	g := p.pre.Ip
	p.preLock.RUnlock()
	if e == g || e == p.pos.Ip || g == p.pos.Ip {
		if e != p.pos.Ip && g != p.pos.Ip {
			p.RemoteCall(e, "Node.Lockit", "", &ss)
			p.RemoteCall(e, "Node.Alone", "", &ss)
			p.RemoteCall(e, "Node.UL", "", &ss)
		}
		p.is_online = false
		p.listener.Close()
		logrus.Infof("Quit Success HH %s %s %s", e, p.pos.Ip, g)
		return
	}
	var wt sync.WaitGroup
	wt.Add(2)
	go func() {
		defer wt.Done()
		p.RemoteCall(g, "Node.Lockit", "", &ss)
		p.RemoteCall(g, "Node.QuitWork_Pre", e, &ss)
		logrus.Infof("What's Wrong3 %s", p.pos.Ip)
	}()
	go func() {
		defer wt.Done()
		c := make(map[string]string)
		p.codataLock.RLock()
		for key, val := range p.copy_data {
			c[key] = val
		}
		p.codataLock.RUnlock()
		p.RemoteCall(e, "Node.Lockit", "", &ss)
		p.RemoteCall(e, "Node.Linkpre", g, &ss)
		p.RemoteCall(e, "Node.QuitWork_Suc", c, &ss)
	}()
	wt.Wait()
	p.RemoteCall(g, "Node.UL", "", &ss)
	p.RemoteCall(e, "Node.UL", "", &ss)
	logrus.Infof("Quit Success %s", p.pos.Ip)
	p.is_online = false
	p.listener.Close()
}

func (p *Node) ForceQuit() {
	if !p.is_online {
		return
	}
	p.is_online = false
	p.listener.Close()
}
func (p *Node) Putcpdata(o Ppair, _ *string) error {
	p.codataLock.Lock()
	p.copy_data[o.Key] = o.Val
	p.codataLock.Unlock()
	return nil
}
func (p *Node) Putdata(o Ppair, _ *string) error {
	p.dataLock.Lock()
	p.data[o.Key] = o.Val
	p.dataLock.Unlock()
	ss := ""
	id := 0
	for id < 6 {
		p.SucLock.RLock()
		r := p.SucList[id].Ip
		p.SucLock.RUnlock()
		if r == "" {
			id++
		} else {
			err := p.RemoteCall(r, "Node.Ping", "", &ss)
			if err != nil {
				id++
			} else {
				p.RemoteCall(r, "Node.Putcpdata", o, &ss)
				break
			}
		}
	}
	return nil
}
func (p *Node) Put(key string, value string) bool {
	var s string
	logrus.Infof("Try to put")
	err := p.FindDesSuc(get_hash(key), &s)
	if err != nil {
		logrus.Infof("Put Fail when Finding Suc\n")
		return false
	}
	ss := ""
	err = p.RemoteCall(s, "Node.Putdata", Ppair{key, value}, &ss)
	if err != nil {
		logrus.Infof("Put Fail when Tr data\n")
		return false
	}
	return true
}
func (p *Node) Getkey(key string, s *string) error {
	p.dataLock.RLock()
	ans, ok := p.data[key]
	p.dataLock.RUnlock()
	if ok {
		*s = ans
	} else {
		p.codataLock.RLock()
		ans, ok = p.copy_data[key]
		p.codataLock.RUnlock()
		if ok {
			*s = ans
		} else {
			*s = ""
		}
	}
	return nil
}
func (p *Node) Get(key string) (bool, string) {
	var s string
	err := p.FindDesSuc(get_hash(key), &s)
	if err != nil {
		logrus.Infof("Get Fail when Finding Suc\n")
		return false, ""
	}
	err = p.RemoteCall(s, "Node.Getkey", key, &s)
	if err != nil || s == "" {
		logrus.Infof("Get Fail when Geting %v\n", err)
		return false, ""
	}
	return true, s
}
func (p *Node) Delcpkey(key string, _ *string) error {
	p.codataLock.Lock()
	delete(p.copy_data, key)
	p.codataLock.Unlock()
	return nil
}
func (p *Node) Delkey(key string, _ *string) error {
	p.dataLock.Lock()
	delete(p.data, key)
	p.dataLock.Unlock()
	ss := ""
	p.SucLock.RLock()
	z := p.SucList[0].Ip
	p.SucLock.RUnlock()
	err := p.RemoteCall(z, "Node.Delcpkey", key, &ss)
	return err
}
func (p *Node) Delete(key string) bool {
	var s string
	err := p.FindDesSuc(get_hash(key), &s)
	if err != nil {
		logrus.Infof("Delete Fail when Finding Suc\n")
		return false
	}
	ss := ""
	err = p.RemoteCall(s, "Node.Delkey", key, &ss)
	if err != nil {
		logrus.Infof("Delete Fail when deleting\n")
		return false
	}
	return true
}
