package qemu

import (
	"fmt"
	"strconv"
	"syscall"

	"github.com/golang/glog"
	"github.com/hyperhq/runv/hypervisor"
	"github.com/hyperhq/runv/lib/utils"
)

func qmpQemuQuit(ctx *hypervisor.VmContext, qc *QemuContext) {
	commands := []*QmpCommand{
		{Execute: "quit", Arguments: map[string]interface{}{}},
	}
	qc.qmp <- &QmpSession{commands: commands, respond: defaultRespond(ctx, nil)}
}

func scsiId2Name(id int) string {
	return "sd" + utils.DiskId2Name(id)
}

func defaultRespond(ctx *hypervisor.VmContext, callback hypervisor.VmEvent) func(err error) {
	return func(err error) {
		if err == nil {
			if callback != nil {
				ctx.Hub <- callback
			}
		} else {
			ctx.Hub <- &hypervisor.DeviceFailed{
				Session: callback,
			}
		}
	}
}

func newDiskAddSession(ctx *hypervisor.VmContext, qc *QemuContext, name, sourceType, filename, format string, id int) {
	commands := make([]*QmpCommand, 2)
	commands[0] = &QmpCommand{
		Execute: "human-monitor-command",
		Arguments: map[string]interface{}{
			"command-line": "drive_add dummy file=" +
				filename + ",if=none,id=" + "drive" + strconv.Itoa(id) + ",format=" + format + ",cache=writeback",
		},
	}
	commands[1] = &QmpCommand{
		Execute: "device_add",
		Arguments: map[string]interface{}{
			"driver": "scsi-hd", "bus": "scsi0.0", "scsi-id": strconv.Itoa(id),
			"drive": "drive" + strconv.Itoa(id), "id": "scsi-disk" + strconv.Itoa(id),
		},
	}
	devName := scsiId2Name(id)
	qc.qmp <- &QmpSession{
		commands: commands,
		respond: defaultRespond(ctx, &hypervisor.BlockdevInsertedEvent{
			Name:       name,
			SourceType: sourceType,
			DeviceName: devName,
			ScsiId:     id,
		}),
	}
}

func newDiskDelSession(ctx *hypervisor.VmContext, qc *QemuContext, id int, callback hypervisor.VmEvent) {
	commands := make([]*QmpCommand, 2)
	commands[1] = &QmpCommand{
		Execute: "device_del",
		Arguments: map[string]interface{}{
			"id": "scsi-disk" + strconv.Itoa(id),
		},
	}
	commands[0] = &QmpCommand{
		Execute: "human-monitor-command",
		Arguments: map[string]interface{}{
			"command-line": fmt.Sprintf("drive_del drive%d", id),
		},
	}
	qc.qmp <- &QmpSession{
		commands: commands,
		respond:  defaultRespond(ctx, callback),
	}
}

func newNetworkAddSession(ctx *hypervisor.VmContext, qc *QemuContext, fd uint64, device, mac string, index, addr int) {
	busAddr := fmt.Sprintf("0x%x", addr)
	commands := make([]*QmpCommand, 3)
	scm := syscall.UnixRights(int(fd))
	glog.V(1).Infof("send net to qemu at %d", int(fd))
	commands[0] = &QmpCommand{
		Execute: "getfd",
		Arguments: map[string]interface{}{
			"fdname": "fd" + device,
		},
		Scm: scm,
	}
	commands[1] = &QmpCommand{
		Execute: "netdev_add",
		Arguments: map[string]interface{}{
			"type": "tap", "id": device, "fd": "fd" + device,
		},
	}
	commands[2] = &QmpCommand{
		Execute: "device_add",
		Arguments: map[string]interface{}{
			"driver": "virtio-net-pci",
			"netdev": device,
			"mac":    mac,
			"bus":    "pci.0",
			"addr":   busAddr,
			"id":     device,
		},
	}

	qc.qmp <- &QmpSession{
		commands: commands,
		respond: defaultRespond(ctx, &hypervisor.NetDevInsertedEvent{
			Index:      index,
			DeviceName: device,
			Address:    addr,
		}),
	}
}

func newNetworkDelSession(ctx *hypervisor.VmContext, qc *QemuContext, device string, callback hypervisor.VmEvent) {
	commands := make([]*QmpCommand, 2)
	commands[0] = &QmpCommand{
		Execute: "device_del",
		Arguments: map[string]interface{}{
			"id": device,
		},
	}
	commands[1] = &QmpCommand{
		Execute: "netdev_del",
		Arguments: map[string]interface{}{
			"id": device,
		},
	}

	qc.qmp <- &QmpSession{
		commands: commands,
		respond:  defaultRespond(ctx, callback),
	}
}
