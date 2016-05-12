package qemu

import (
	"encoding/json"
	"github.com/hyperhq/runv/hypervisor"
	"net"
	"testing"
	"time"
)

func TestMessageParse(t *testing.T) {
	rsp := &QmpResponse{}
	msg := []byte(`{"return": {}}`)
	err := json.Unmarshal(msg, rsp)
	if err != nil || rsp.msg.MessageType() != QMP_RESULT {
		t.Error("normal return parsing failed")
	}

	msg_str := []byte(`{"return": "OK\r\n"}`)
	err = json.Unmarshal(msg_str, rsp)
	if err != nil || rsp.msg.MessageType() != QMP_RESULT {
		t.Error("normal return parsing failed")
	}

	msg_event := []byte(`{"timestamp": {"seconds": 1429545058, "microseconds": 283331}, "event": "NIC_RX_FILTER_CHANGED", "data": {"path": "/machine/peripheral-anon/device[1]/virtio-backend"}}`)
	err = json.Unmarshal(msg_event, rsp)
	if err != nil || rsp.msg.MessageType() != QMP_EVENT {
		t.Error("normal return parsing failed")
	}

	msg_error := []byte(`{"error": {"class": "GenericError", "desc": "QMP input object member 'server' is unexpected"}}`)
	err = json.Unmarshal(msg_error, rsp)
	if err != nil || rsp.msg.MessageType() != QMP_ERROR {
		t.Error("normal return parsing failed")
	}
}

func testQmpInitHelper(t *testing.T, ctx *QemuContext) (*net.UnixListener, net.Conn) {
	t.Log("setup ", ctx.qmpSockName)

	ss, err := net.ListenUnix("unix", &net.UnixAddr{Name: ctx.qmpSockName, Net: "unix"})
	if err != nil {
		t.Error("fail to setup connect to qmp socket", err.Error())
	}

	c, err := ss.Accept()
	if err != nil {
		t.Error("cannot accept qmp socket", err.Error())
	}

	t.Log("connected")

	banner := `{"QMP": {"version": {"qemu": {"micro": 0,"minor": 0,"major": 2},"package": ""},"capabilities": []}}`
	t.Log("Writting", banner)

	nr, err := c.Write([]byte(banner))
	if err != nil {
		t.Error("write banner fail ", err.Error())
	}
	t.Log("wrote hello ", nr)

	buf := make([]byte, 1024)
	nr, err = c.Read(buf)
	if err != nil {
		t.Error("fail to get init message")
	}

	t.Log("received message", string(buf[:nr]))

	var msg interface{}
	err = json.Unmarshal(buf[:nr], &msg)
	if err != nil {
		t.Error("can not read init message to json ", string(buf[:nr]))
	}

	hello := msg.(map[string]interface{})
	if hello["execute"].(string) != "qmp_capabilities" {
		t.Error("message wrong", string(buf[:nr]))
	}

	c.Write([]byte(`{ "return": {}}`))

	return ss, c
}

func testQmpEnvironment() (*hypervisor.VmContext, *QemuContext) {
	b := &hypervisor.BootConfig{
		CPU:    1,
		Memory: 128,
		Kernel: hypervisor.DefaultKernel,
		Initrd: hypervisor.DefaultInitrd,
	}

	qemuChan := make(chan hypervisor.VmEvent, 128)
	dr := &QemuDriver{}
	dr.Initialize()
	ctx, _ := hypervisor.InitContext(dr, "vmid", qemuChan, nil, nil, b)

	return ctx, ctx.DCtx.(*QemuContext)
}

func TestQmpHello(t *testing.T) {

	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	s, c := testQmpInitHelper(t, qc)
	defer s.Close()
	defer c.Close()

	c.Write([]byte(`{ "event": "SHUTDOWN", "timestamp": { "seconds": 1265044230, "microseconds": 450486 } }`))

	ev := <-ctx.Hub
	if ev.Event() != hypervisor.EVENT_VM_EXIT {
		t.Error("should got an exit event")
	}

	t.Log("qmp finished")
}

func TestInitFail(t *testing.T) {

	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	t.Log("setup ", qc.qmpSockName)

	ss, err := net.ListenUnix("unix", &net.UnixAddr{Name: qc.qmpSockName, Net: "unix"})
	if err != nil {
		t.Error("fail to setup connect to qmp socket", err.Error())
	}

	c, err := ss.Accept()
	if err != nil {
		t.Error("cannot accept qmp socket", err.Error())
	}
	defer ss.Close()
	defer c.Close()

	t.Log("connected")

	banner := `{"QMP": {"version": {"qemu": {"micro": 0,"minor": 0,"major": 2},"package": ""},"capabilities": []}}`
	t.Log("Writting", banner)

	nr, err := c.Write([]byte(banner))
	if err != nil {
		t.Error("write banner fail ", err.Error())
	}
	t.Log("wrote hello ", nr)

	buf := make([]byte, 1024)
	nr, err = c.Read(buf)
	if err != nil {
		t.Error("fail to get init message")
	}

	t.Log("received message", string(buf[:nr]))

	var msg interface{}
	err = json.Unmarshal(buf[:nr], &msg)
	if err != nil {
		t.Error("can not read init message to json ", string(buf[:nr]))
	}

	hello := msg.(map[string]interface{})
	if hello["execute"].(string) != "qmp_capabilities" {
		t.Error("message wrong", string(buf[:nr]))
	}

	c.Write([]byte(`{ "error": {}}`))

	ev := <-ctx.Hub
	if ev.Event() != hypervisor.ERROR_INIT_FAIL {
		t.Error("should got an event")
	}

	t.Log("qmp init failed")
}

func TestQmpConnTimeout(t *testing.T) {

	ctx, _ := testQmpEnvironment()

	go qmpHandler(ctx)

	time.Sleep(6 * time.Second)

	ev := <-ctx.Hub
	if ev.Event() != hypervisor.ERROR_INIT_FAIL {
		t.Error("should got an fail event")
	}

	t.Log("finished timeout test")
}

func TestQmpInitTimeout(t *testing.T) {
	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	t.Log("connecting to ", qc.qmpSockName)

	ss, err := net.ListenUnix("unix", &net.UnixAddr{Name: qc.qmpSockName, Net: "unix"})
	if err != nil {
		t.Error("fail to setup connect to qmp socket", err.Error())
	}

	c, err := ss.Accept()
	if err != nil {
		t.Error("cannot accept qmp socket", err.Error())
	}
	defer ss.Close()
	defer c.Close()

	t.Log("connected")

	time.Sleep(11 * time.Second)

	ev := <-ctx.Hub
	if ev.Event() != hypervisor.ERROR_INIT_FAIL {
		t.Error("should got an fail event")
	}

	t.Log("finished timeout test")
}

func TestQmpDiskSession(t *testing.T) {

	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	s, c := testQmpInitHelper(t, qc)
	defer s.Close()
	defer c.Close()

	newDiskAddSession(qc, "vol1", "volume", "/dev/dm7", "raw", 5)

	buf := make([]byte, 1024)
	nr, err := c.Read(buf)
	if err != nil {
		t.Error("cannot read command 0 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": "success"}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 1 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	msg := <-ctx.Hub
	if msg.Event() != hypervisor.EVENT_BLOCK_INSERTED {
		t.Error("wrong type of message", msg.Event())
	}

	info := msg.(*hypervisor.BlockdevInsertedEvent)
	t.Log("got block device", info.Name, info.SourceType, info.DeviceName)
}

func TestQmpFailOnce(t *testing.T) {

	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	s, c := testQmpInitHelper(t, qc)
	defer s.Close()
	defer c.Close()

	newDiskAddSession(qc, "vol1", "volume", "/dev/dm7", "raw", 5)

	buf := make([]byte, 1024)
	nr, err := c.Read(buf)
	if err != nil {
		t.Error("cannot read command 0 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{"error": {"class": "GenericError", "desc": "QMP input object member 'server' is unexpected"}}`))
	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read repeated command 1 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 1 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	msg := <-ctx.Hub
	if msg.Event() != hypervisor.EVENT_BLOCK_INSERTED {
		t.Error("wrong type of message", msg.Event())
	}

	info := msg.(*hypervisor.BlockdevInsertedEvent)
	t.Log("got block device", info.Name, info.SourceType, info.DeviceName)
}

func TestQmpKeepFail(t *testing.T) {
	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	s, c := testQmpInitHelper(t, qc)
	defer s.Close()
	defer c.Close()

	newDiskAddSession(qc, "vol1", "volume", "/dev/dm7", "raw", 5)

	buf := make([]byte, 1024)
	nr, err := c.Read(buf)
	if err != nil {
		t.Error("cannot read command 0 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{"error": {"class": "GenericError", "desc": "QMP input object member 'server' is unexpected"}}`))
	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read repeated command 1 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{"error": {"class": "GenericError", "desc": "QMP input object member 'server' is unexpected"}}`))
	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read repeated command 1 again in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{"error": {"class": "GenericError", "desc": "QMP input object member 'server' is unexpected"}}`))

	msg := <-ctx.Hub
	if msg.Event() != hypervisor.ERROR_QMP_FAIL {
		t.Error("wrong type of message", msg.Event())
	}

	info := msg.(*hypervisor.DeviceFailed)
	t.Log("got block device", hypervisor.EventString(info.Session.Event()))
}

func TestQmpNetSession(t *testing.T) {

	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	s, c := testQmpInitHelper(t, qc)
	defer s.Close()
	defer c.Close()

	newNetworkAddSession(qc, 12, "eth0", "mac", 0, 3)

	buf := make([]byte, 1024)
	nr, err := c.Read(buf)
	if err != nil {
		t.Error("cannot read command 0 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 1 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 2 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))
	msg := <-ctx.Hub
	if msg.Event() != hypervisor.EVENT_INTERFACE_INSERTED {
		t.Error("wrong type of message", msg.Event())
	}

	info := msg.(*hypervisor.NetDevInsertedEvent)
	t.Log("got net device", info.Address, info.Index, info.DeviceName)
}

func TestSessionQueue(t *testing.T) {
	ctx, qc := testQmpEnvironment()

	go qmpHandler(ctx)

	s, c := testQmpInitHelper(t, qc)
	defer s.Close()
	defer c.Close()

	newNetworkAddSession(qc, 12, "eth0", "mac", 0, 3)
	newNetworkAddSession(qc, 13, "eth1", "mac", 1, 4)

	buf := make([]byte, 1024)
	nr, err := c.Read(buf)
	if err != nil {
		t.Error("cannot read command 0 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 1 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 2 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	msg := <-ctx.Hub
	if msg.Event() != hypervisor.EVENT_INTERFACE_INSERTED {
		t.Error("wrong type of message", msg.Event())
	}

	info := msg.(*hypervisor.NetDevInsertedEvent)
	t.Log("got block device", info.Address, info.Index, info.DeviceName)
	if info.Address != 0x03 || info.Index != 0 || info.DeviceName != "eth0" {
		t.Error("net dev 0 creation failed")
	}

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 0 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 1 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	nr, err = c.Read(buf)
	if err != nil {
		t.Error("cannot read command 2 in session", err.Error())
	}
	t.Log("received ", string(buf[:nr]))

	c.Write([]byte(`{ "return": {}}`))

	msg = <-ctx.Hub
	if msg.Event() != hypervisor.EVENT_INTERFACE_INSERTED {
		t.Error("wrong type of message", msg.Event())
	}

	info = msg.(*hypervisor.NetDevInsertedEvent)
	t.Log("got network device", info.Address, info.Index, info.DeviceName)
	if info.Address != 0x04 || info.Index != 1 || info.DeviceName != "eth1" {
		t.Error("net dev 1 creation failed")
	}

}
