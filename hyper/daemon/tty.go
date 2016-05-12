package daemon

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/hyperhq/hyper/engine"
	"strconv"
	"strings"
)

func (daemon *Daemon) CmdTty(job *engine.Job) (err error) {
	if len(job.Args) < 3 {
		return nil
	}
	var (
		podID     = job.Args[0]
		tag       = job.Args[1]
		h         = job.Args[2]
		w         = job.Args[3]
		container string
		vmid      string
	)

	if strings.Contains(podID, "pod-") {
		container = ""
		vmid, err = daemon.GetVmByPodId(podID)
		if err != nil {
			return err
		}
	} else if strings.Contains(podID, "vm-") {
		vmid = podID
	} else {
		container = podID
		podID, err = daemon.GetPodByContainer(container)
		if err != nil {
			return err
		}
		vmid, err = daemon.GetVmByPodId(podID)
		if err != nil {
			return err
		}
	}

	vm, ok := daemon.VmList[vmid]
	if !ok {
		return fmt.Errorf("vm %s doesn't exist!")
	}

	row, err := strconv.Atoi(h)
	if err != nil {
		glog.Warningf("Window row %s incorrect!", h)
	}
	column, err := strconv.Atoi(w)
	if err != nil {
		glog.Warningf("Window column %s incorrect!", h)
	}

	err = vm.Tty(tag, row, column)
	if err != nil {
		return err
	}

	glog.V(1).Infof("Success to resize the tty!")
	return nil
}
