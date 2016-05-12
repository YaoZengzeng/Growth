package daemon

import (
	"fmt"

	"github.com/golang/glog"
	"github.com/hyperhq/hyper/engine"
	"github.com/hyperhq/runv/hypervisor"
	"github.com/hyperhq/runv/hypervisor/types"
)

func (daemon *Daemon) CmdList(job *engine.Job) error {
	var (
		item                  string
		dedicadedPod          bool           = false
		podId                 string         = ""
		dedicadedVM           bool           = false
		vmId                  string         = ""
		auxiliary             bool           = false
		pod                   *Pod           = nil
		vm                    *hypervisor.Vm = nil
		vmJsonResponse                       = []string{}
		podJsonResponse                      = []string{}
		containerJsonResponse                = []string{}
	)
	if len(job.Args) == 0 {
		item = "pod"
	} else {
		item = job.Args[0]
	}
	if item != "pod" && item != "container" && item != "vm" {
		return fmt.Errorf("Can not support %s list!", item)
	}

	if len(job.Args) > 1 && (job.Args[1] != "") {
		dedicadedPod = true
		podId = job.Args[1]
	}

	if len(job.Args) > 2 && (job.Args[2] != "") {
		dedicadedVM = true
		vmId = job.Args[2]
	}

	if len(job.Args) > 3 && (job.Args[3] == "yes" || job.Args[3] == "true") {
		auxiliary = true
	}

	daemon.PodList.RLock()
	glog.Infof("lock read of PodList")
	defer glog.Infof("unlock read of PodList")
	defer daemon.PodList.RUnlock()

	if dedicadedPod {
		var ok bool
		pod, ok = daemon.PodList.Get(podId)
		if !ok || (pod == nil) {
			return fmt.Errorf("Cannot find specified pod %s", podId)
		}
	}

	if dedicadedVM {
		var ok bool
		vm, ok = daemon.VmList[vmId]
		if !ok || (vm == nil) {
			return fmt.Errorf("Cannot find specified vm %s", vmId)
		}
	}

	// Prepare the VM status to client
	v := &engine.Env{}
	v.Set("item", item)
	if item == "vm" {
		if !dedicadedPod && !dedicadedVM {
			for v, info := range daemon.VmList {
				vmJsonResponse = append(vmJsonResponse, v+":"+showVM(info))
			}
		} else if dedicadedPod && !dedicadedVM {
			if v, ok := daemon.VmList[pod.status.Vm]; ok {
				vmJsonResponse = append(vmJsonResponse, pod.status.Vm+":"+showVM(v))
			}
		} else if !dedicadedPod && dedicadedVM {
			vmJsonResponse = append(vmJsonResponse, vmId+":"+showVM(vm))
		} else {
			if pod.status.Vm == vmId {
				vmJsonResponse = append(vmJsonResponse, vmId+":"+showVM(vm))
			}
		}
		v.SetList("vmData", vmJsonResponse)
	}

	if item == "pod" {
		if !dedicadedPod && !dedicadedVM {
			daemon.PodList.Foreach(func(p *Pod) error {
				podJsonResponse = append(podJsonResponse, p.id+":"+showPod(p.status))
				return nil
			})
		} else if dedicadedPod && !dedicadedVM {
			podJsonResponse = append(podJsonResponse, pod.id+":"+showPod(pod.status))
		} else if !dedicadedPod && dedicadedVM {
			daemon.PodList.Foreach(func(p *Pod) error {
				if p.status.Vm == vmId {
					podJsonResponse = append(podJsonResponse, p.id+":"+showPod(p.status))
				}
				return nil
			})
		} else {
			if pod.status.Vm == vmId {
				podJsonResponse = append(podJsonResponse, pod.id+":"+showPod(pod.status))
			}
		}
		v.SetList("podData", podJsonResponse)
	}

	if item == "container" {
		if !dedicadedPod && !dedicadedVM {
			daemon.PodList.Foreach(func(p *Pod) error {
				containerJsonResponse = append(containerJsonResponse, showPodContainers(p.status, auxiliary)...)
				return nil
			})
		} else if dedicadedPod && !dedicadedVM {
			containerJsonResponse = append(containerJsonResponse, showPodContainers(pod.status, auxiliary)...)
		} else if !dedicadedPod && dedicadedVM {
			daemon.PodList.Foreach(func(p *Pod) error {
				if p.status.Vm == vmId {
					containerJsonResponse = append(containerJsonResponse, showPodContainers(p.status, auxiliary)...)
				}
				return nil
			})
		} else {
			if pod.status.Vm == vmId {
				containerJsonResponse = append(containerJsonResponse, showPodContainers(pod.status, auxiliary)...)
			}
		}
		v.SetList("cData", containerJsonResponse)
	}

	if _, err := v.WriteTo(job.Stdout); err != nil {
		return err
	}

	return nil
}

func showVM(v *hypervisor.Vm) string {
	var status string
	switch v.Status {
	case types.S_VM_ASSOCIATED:
		status = "associated"
		break
	case types.S_VM_IDLE:
		status = "idle"
		break
	default:
		status = ""
		break
	}
	p := ""
	if v.Pod != nil {
		p = v.Pod.Id
	}

	return p + ":" + status
}

func showPod(pod *hypervisor.PodStatus) string {
	var status string

	switch pod.Status {
	case types.S_POD_RUNNING:
		status = "running"
	case types.S_POD_CREATED:
		status = "pending"
	case types.S_POD_FAILED:
		status = "failed"
		if pod.Type == "kubernetes" {
			status = "failed(kubernetes)"
		}
	case types.S_POD_SUCCEEDED:
		status = "succeeded"
		if pod.Type == "kubernetes" {
			status = "succeeded(kubernetes)"
		}
	default:
		status = ""
	}

	return pod.Name + ":" + pod.Vm + ":" + status
}

func showPodContainers(pod *hypervisor.PodStatus, aux bool) []string {

	rsp := []string{}
	filterServiceDiscovery := !aux && (pod.Type == "service-discovery")
	proxyName := ServiceDiscoveryContainerName(pod.Name)

	for _, c := range pod.Containers {
		if filterServiceDiscovery && c.Name == proxyName {
			continue
		}
		rsp = append(rsp, showContainer(c))
	}
	return rsp
}

func showContainer(c *hypervisor.Container) string {
	var status string

	switch c.Status {
	case types.S_POD_RUNNING:
		status = "running"
	case types.S_POD_CREATED:
		status = "pending"
	case types.S_POD_FAILED:
		status = "failed"
	case types.S_POD_SUCCEEDED:
		status = "succeeded"
	default:
		status = ""
	}

	return c.Id + ":" + c.Name + ":" + c.PodId + ":" + status
}
