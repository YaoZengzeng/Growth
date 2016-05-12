package ipallocator

import (
	"errors"
	"math/big"
	"net"
	"sync"
)

// allocatedMap is thread-unsafe set of allocated IP
type allocatedMap struct {
	p     map[string]struct{}
	last  *big.Int
	begin *big.Int
	end   *big.Int
}

func newAllocatedMap(network *net.IPNet) *allocatedMap {
	firstIP, lastIP := NetworkRange(network)
	begin := big.NewInt(0).Add(ipToBigInt(firstIP), big.NewInt(1))
	end := big.NewInt(0).Sub(ipToBigInt(lastIP), big.NewInt(1))

	return &allocatedMap{
		p:     make(map[string]struct{}),
		begin: begin,
		end:   end,
		last:  big.NewInt(0).Sub(begin, big.NewInt(1)), // so first allocated will be begin
	}
}

// Calculates the first and last IP addresses in an IPNet
func NetworkRange(network *net.IPNet) (net.IP, net.IP) {
	var netIP net.IP
	if network.IP.To4() != nil {
		netIP = network.IP.To4()
	} else if network.IP.To16() != nil {
		netIP = network.IP.To16()
	} else {
		return nil, nil
	}

	lastIP := make([]byte, len(netIP), len(netIP))

	for i := 0; i < len(netIP); i++ {
		lastIP[i] = netIP[i] | ^network.Mask[i]
	}
	return netIP.Mask(network.Mask), net.IP(lastIP)
}

type networkSet map[string]*allocatedMap

var (
	ErrNoAvailableIPs           = errors.New("no available ip addresses on network")
	ErrIPAlreadyAllocated       = errors.New("ip already allocated")
	ErrIPOutOfRange             = errors.New("requested ip is out of range")
	ErrNetworkAlreadyRegistered = errors.New("network already registered")
	ErrBadSubnet                = errors.New("network does not contain specified subnet")
)

type IPAllocator struct {
	allocatedIPs networkSet
	mutex        sync.Mutex
}

func New() *IPAllocator {
	return &IPAllocator{networkSet{}, sync.Mutex{}}
}

// RegisterSubnet registers network in global allocator with bounds
// defined by subnet. If you want to use network range you must call
// this method before first RequestIP, otherwise full network range will be used
func (a *IPAllocator) RegisterSubnet(network *net.IPNet, subnet *net.IPNet) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	key := network.String()
	if _, ok := a.allocatedIPs[key]; ok {
		return ErrNetworkAlreadyRegistered
	}
	n := newAllocatedMap(network)
	beginIP, endIP := NetworkRange(subnet)
	begin := big.NewInt(0).Add(ipToBigInt(beginIP), big.NewInt(1))
	end := big.NewInt(0).Sub(ipToBigInt(endIP), big.NewInt(1))

	// Check that subnet is within network
	if !(begin.Cmp(n.begin) >= 0 && end.Cmp(n.end) <= 0 && begin.Cmp(end) == -1) {
		return ErrBadSubnet
	}
	n.begin.Set(begin)
	n.end.Set(end)
	n.last.Sub(begin, big.NewInt(1))
	a.allocatedIPs[key] = n
	return nil
}

// RequestIP requests an available ip from the given network.  It
// will return the next available ip if the ip provided is nil.  If the
// ip provided is not nil it will validate that the provided ip is available
// for use or return an error
func (a *IPAllocator) RequestIP(network *net.IPNet, ip net.IP) (net.IP, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	key := network.String()
	allocated, ok := a.allocatedIPs[key]
	if !ok {
		allocated = newAllocatedMap(network)
		a.allocatedIPs[key] = allocated
	}

	if ip == nil {
		return allocated.getNextIP()
	}
	return allocated.checkIP(ip)
}

// ReleaseIP adds the provided ip back into the pool of
// available ips to be returned for use.
func (a *IPAllocator) ReleaseIP(network *net.IPNet, ip net.IP) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if allocated, exists := a.allocatedIPs[network.String()]; exists {
		delete(allocated.p, ip.String())
	}
	return nil
}

func (allocated *allocatedMap) checkIP(ip net.IP) (net.IP, error) {
	if _, ok := allocated.p[ip.String()]; ok {
		return nil, ErrIPAlreadyAllocated
	}

	pos := ipToBigInt(ip)
	// Verify that the IP address is within our network range.
	if pos.Cmp(allocated.begin) == -1 || pos.Cmp(allocated.end) == 1 {
		return nil, ErrIPOutOfRange
	}

	// Register the IP.
	allocated.p[ip.String()] = struct{}{}

	return ip, nil
}

// return an available ip if one is currently available.  If not,
// return the next available ip for the network
func (allocated *allocatedMap) getNextIP() (net.IP, error) {
	pos := big.NewInt(0).Set(allocated.last)
	allRange := big.NewInt(0).Sub(allocated.end, allocated.begin)
	for i := big.NewInt(0); i.Cmp(allRange) <= 0; i.Add(i, big.NewInt(1)) {
		pos.Add(pos, big.NewInt(1))
		if pos.Cmp(allocated.end) == 1 {
			pos.Set(allocated.begin)
		}
		if _, ok := allocated.p[bigIntToIP(pos).String()]; ok {
			continue
		}
		allocated.p[bigIntToIP(pos).String()] = struct{}{}
		allocated.last.Set(pos)
		return bigIntToIP(pos), nil
	}
	return nil, ErrNoAvailableIPs
}

// Converts a 4 bytes IP into a 128 bit integer
func ipToBigInt(ip net.IP) *big.Int {
	x := big.NewInt(0)
	if ip4 := ip.To4(); ip4 != nil {
		return x.SetBytes(ip4)
	}
	if ip6 := ip.To16(); ip6 != nil {
		return x.SetBytes(ip6)
	}

	return nil
}

// Converts 128 bit integer into a 4 bytes IP address
func bigIntToIP(v *big.Int) net.IP {
	return net.IP(v.Bytes())
}
