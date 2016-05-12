package daemon

import (
	"fmt"
	"net"
	"github.com/golang/glog"
	"github.com/hyperhq/runv/hypervisor/types"
	"github.com/hyperhq/runv/hypervisor"
	"github.com/hyperhq/hyper/engine"
)

func (daemon *Daemon) CmdPodMigrate(job *engine.Job) error {
	if len(job.Args) == 0 {
		return fmt.Errorf("Can not execute 'migrate' command without any pod name!")
	}
	podId 	:= job.Args[0]
	ip 		:= job.Args[1]
	port 	:= job.Args[2]
	daemon.PodList.Lock()
	glog.V(2).Infof("lock PodList")
	defer glog.V(2).Infof("unlock PodList")
	defer daemon.PodList.Unlock()
	code, cause, err := daemon.MigratePod(podId, ip, port)
	if err != nil {
		return err
	}

	// Prepare the VM status to client
	v := &engine.Env{}
	v.Set("ID",podId)
	v.SetInt("Code",code)
	v.Set("Cause",cause)
	if _,err := v.WriteTo(job.Stdout); err != nil {
		return err
	}

	return nil
}

func (daemon *Daemon) CmdPodListen(job *engine.Job) error {
	if len(job.Args) == 0 {
		return fmt.Errorf("Can not execute 'listen' command without any pod name!")
	}

	var (
		tag 		string			= ""
		ttys		[]*hypervisor.TtyIO = []*hypervisor.TtyIO{}
		ttyCallback	chan *types.VmResponse
	)

	vmId	:= ""
	ip 		:= job.Args[0]
	port 	:= job.Args[1]
	if len(job.Args) > 2 {
		tag = job.Args[1]
	}
	if tag != "" {
		glog.V(1).Info("Pod Run with client terminal tag: ",tag)
		ttyCallback = make(chan *types.VmResponse, 1)
		ttys = append(ttys,&hypervisor.TtyIO{
				Stdin:			job.Stdin,
				Stdout:			job.Stdout,
				ClientTag:		tag,
				Callback:		ttyCallback,
		})
	}

/*	daemon.PodList.Lock()
	glog.V(2).Infof("lock PodList")
	if _,ok := daemon.PodList.Get(podId); !ok {
		glog.V(2).Infof("unlock PodList")
		daemon.PodList.Unlock()
		return fmt.Errorf("The pod(%s) can not be found, please create it first", podId)
	}*/

	var lazy bool = hypervisor.HDriver.SupportLazyMode() && vmId == ""

	_, _, err := daemon.ListenPod(ip, port, vmId, nil, lazy, false, types.VM_KEEP_NONE, ttys)
	if err != nil {
		glog.Errorf(err.Error())
		return err
	}

/*	if len(ttys) >0 {
	//	glog.V(2).Infof("unlock PodList")
	//	daemon.PodList.Unlock()
		daemon.GetExitCode(podId,tag,ttyCallback)
	}*/


	// Prepare the VM status to client
/*	v := &engine.Env{}
	v.Set("ID",podId)
	v.SetInt("Code",code)
	v.Set("Cause",cause)
	if _,err := v.WriteTo(job.Stdout); err != nil {
		return err
	}*/

	return nil
}

func (daemon *Daemon) ListenPod(ip, port, vmId string, config interface{}, lazy, autoremove bool, keep int, streams []*hypervisor.TtyIO) (int, string, error) {
	glog.V(1).Infof("Listening ip is %s, port is %s", ip, port)
	var (
		err error
		p 	*Pod
	)

	// listen for the mata data
	addr := fmt.Sprintf("%s:%s", ip, port)
	listener, err := net.Listen("tcp", addr)
	defer listener.Close()
	if err != nil {
		glog.Errorf("Create mata data listener error")
	}
	conn,_ := listener.Accept()
	pId := getMata(conn)
	glog.V(1).Infof("Listen Pod get pod Id %s",string(pId))
	conn,_ = listener.Accept()
	pArgs := getMata(conn)
	glog.V(1).Infof("Listen Pod get pod Args %s",string(pArgs))
	conn,_ = listener.Accept()
	cId := getMata(conn)
	glog.V(1).Infof("Listen Pod get containers ids %s",string(cId))
	spec, err := ProcessPodBytes(pArgs, string(pId)) 
	if err != nil {
		glog.Errorf("Process Pod(%s) Args error", string(pId))
	}
	cName 	:= spec.Containers[0].Name
	cImage 	:= spec.Containers[0].Image
	glog.V(1).Infof("Listen Pod get containers name %s", cName)
	glog.V(1).Infof("Listen Pod get containers image %s", cImage)

	podId := string(pId)
	podArgs := string(pArgs)

	if _,ok := daemon.PodList.Get(podId); ok {
		glog.V(1).Infof("The listening get an existing pod")
		return -1,"",fmt.Errorf("The pos has existed(%s)", podId)
	}

	if cInfo,_ := daemon.DockerCli.GetContainerInfo(string(cId)); cInfo == nil {
		if _, _, err := daemon.DockerCli.SendCmdRestore(cName, cImage, string(cId), []string{}, nil); err != nil {
			return -1, "", fmt.Errorf("Restore Container(%s) error", string(cId))
		}
	}

	// put the container ids into db
	key := fmt.Sprintf("pod-container-%s",podId)
	daemon.db.Put([]byte(key), []byte(cId),nil)	


	p,err = daemon.GetPod(podId, podArgs, autoremove)
	if err != nil {
		return -1, "", err
	}

	//vmResponse, err := p.Listen(daemon, vmId, lazy, autoremove, keep, streams)
	_, err = p.Listen(daemon, ip, port, vmId, lazy, autoremove, keep, streams)
	if err != nil {
		return -1, "", err
	}

	//return vmResponse.Code, vmResponse.Cause, nil
	return 0, "", nil
}

func getMata(conn net.Conn) []byte {
	buf := make([]byte, 1024)
	defer conn.Close()
	arg := []byte{}

	for {
		n,err := conn.Read(buf)
		if err != nil {
			return arg
		}
		arg = append(arg, buf[:n]...)
	}
}

func (daemon *Daemon) MigratePod(podId , ip, port string) (int,string,error) {
	var (
		code 	= 0
		cause 	= ""
		err 	error
	)

	glog.V(1).Infof("Prepare to migrate the POD: %s",podId)
	pod, ok := daemon.PodList.Get(podId)
	if !ok {
		glog.Errorf("Can not find pod(%s)",podId)
		return -1, "", fmt.Errorf("Can not find pod(%s)",podId)
	}

	glog.V(1).Infof("Migrate Pod , pod id %s",string(podId))
	podArgs,_ := daemon.GetPodByName(podId)
	glog.V(1).Infof("Migrate Pod , pod Args %s",string(podArgs))
	key := fmt.Sprintf("pod-container-%s", podId)
	cIds, err := daemon.db.Get([]byte(key), nil)
	glog.V(1).Infof("Migrate Pod , containers Id %s",string(cIds))
	if err != nil {
		glog.Errorf("can not find containers of pod(%s)",podId)
	}
	sendMata([]byte(podId), ip, port)
	sendMata(podArgs, ip, port)
	sendMata(cIds, ip, port)

	if pod.vm == nil {
		return types.E_VM_SHUTDOWN, "", nil
	}

	vmId := pod.vm.Id
	vmResponse := pod.vm.MigratePod(pod.status, ip, port)

	if vmResponse != nil {
		glog.V(1).Infof("Migrate Pod succeed, now plan to move the Pod")
		daemon.DeleteVmByPod(podId)
		daemon.RemoveVm(vmId)
		code, cause, err = daemon.CleanPod(podId)
	}


	return code, cause, nil


}

func sendMata(arg []byte, ip, port string) error {
	addr := fmt.Sprintf("%s:%s", ip, port)
	conn, err := net.Dial("tcp",addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	send := 0
	length := len(arg)
	for send<length {
		n,err := conn.Write(arg[send:])
		if err != nil {
			glog.Errorf("send Mata data error", err.Error())
			return err
		}
		send = send + n
	}

	return nil
}

func (p *Pod) Listen(daemon *Daemon, ip, port, vmId string, lazy, autoremove bool, keep int, streams []*hypervisor.TtyIO) (*types.VmResponse, error) {
	var err error = nil 

	if err = p.GetVM(daemon, vmId, lazy, keep); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil && vmId == "" {
			p.KillVM(daemon)
		}
	}()

	if err = p.Prepare(daemon); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			stopLogger(p.status)
		}
	}()

	if err = p.startLogging(daemon); err != nil {
		return nil, err
	}

	if err = p.AttachTtys(streams); err != nil {
		return nil, err
	}

	//vmResponse := p.vm.ListenPod(p.status, p.spec, p.containers, p.volumes)
	p.vm.ListenPod(ip, port, p.status, p.spec, p.containers, p.volumes)
/*	if vmResponse.Data == nil {
		err = fmt.Errorf("VM response data is nil")
		return vmResponse, err
	}*/

	//err = daemon.UpdateVmData(p.vm.Id, vmResponse.Data.([]byte))
	err = daemon.UpdateVmData(p.vm.Id, []byte{})
	if err != nil {
		glog.Error(err.Error())
		return nil, err
	}
	// add or update the Vm info for POD
	err = daemon.UpdateVmByPod(p.id, p.vm.Id)
	if err != nil {
		glog.Error(err.Error())
		return nil, err
	}

	//return vmResponse, nil
	return nil,nil

}
