package qemu

import (
	"os"
	"strings"
	"fmt"

	"github.com/golang/glog"
	"github.com/hyperhq/runv/hypervisor"
	"github.com/hyperhq/runv/lib/utils"
)

func watchDog(qc *QemuContext, hub chan hypervisor.VmEvent) {
	wdt := qc.wdt
	for {
		msg, ok := <-wdt
		if ok {
			switch msg {
			case "quit":
				glog.V(1).Info("quit watch dog.")
				return
			case "kill":
				success := false
				if qc.process != nil {
					glog.V(0).Infof("kill Qemu... %d", qc.process.Pid)
					if err := qc.process.Kill(); err == nil {
						success = true
					}
				} else {
					glog.Warning("no process to be killed")
				}
				hub <- &hypervisor.VmKilledEvent{Success: success}
				return
			}
		} else {
			glog.V(1).Info("chan closed, quit watch dog.")
			break
		}
	}
}

func (qc *QemuContext) watchPid(pid int, hub chan hypervisor.VmEvent) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	qc.process = proc
	go watchDog(qc, hub)

	return nil
}

// launchQemu run qemu and wait it's quit, includes
func launchQemu(qc *QemuContext, ctx *hypervisor.VmContext) {
	qemu := qc.driver.executable
	if qemu == "" {
		ctx.Hub <- &hypervisor.VmStartFailEvent{Message: "can not find qemu executable"}
		return
	}

	args := qc.arguments(ctx)

	if glog.V(1) {
		glog.Info("cmdline arguments: ", strings.Join(args, " "))
	}

	pid, err := utils.ExecInDaemon(qemu, append([]string{"qemu-system-x86_64"}, args...))
	if err != nil {
		//fail to daemonize
		glog.Error("%v", err)
		ctx.Hub <- &hypervisor.VmStartFailEvent{Message: "try to start qemu failed"}
		return
	}

	for _, cb := range qc.callbacks {
		ctx.Hub <- cb
	}

	glog.V(1).Infof("starting daemon with pid: %d", pid)

	err = ctx.DCtx.(*QemuContext).watchPid(int(pid), ctx.Hub)
	if err != nil {
		glog.Error("watch qemu process failed")
		ctx.Hub <- &hypervisor.VmStartFailEvent{Message: "watch qemu process failed"}
		return
	}
}

// listenQemu run qemu for listen and wait it's quit include
func listenQemu(qc *QemuContext, ctx *hypervisor.VmContext, ip, port string) {
	qemu := qc.driver.executable
	if qemu == "" {
		ctx.Hub <- &hypervisor.VmStartFailEvent{Message: "can not find qemu executable"}
		return
	}
	args := qc.arguments(ctx)
	listening := fmt.Sprintf("tcp:%s:%s",ip,port)
	args = append(args,"-incoming",listening)

	if glog.V(1) {
		glog.Info("cmdline arguments: ",strings.Join(args," "))
	}

	pid, err := utils.ExecInDaemon(qemu,append([]string{"qemu-system-x86_64"},args...))
	if err != nil {
		// fail to daemonize
		glog.Error("%v",err)
		ctx.Hub <- &hypervisor.VmStartFailEvent{Message: "try to start qemu for liste failed"}
		return
	}

	for _, cb := range qc.callbacks {
		ctx.Hub <- cb
	}

	glog.V(1).Infof("starting daemon for liste with pid: %d",pid)

	err = ctx.DCtx.(*QemuContext).watchPid(int(pid),ctx.Hub)
	if err != nil {
		glog.Error("watch qemu process failed")
		ctx.Hub <- &hypervisor.VmStartFailEvent{Message: "watch qemu process failed"}
		return
	}

}

func associateQemu(ctx *hypervisor.VmContext) {
	go watchDog(ctx.DCtx.(*QemuContext), ctx.Hub)
}
