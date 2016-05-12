package hypervisor

import (
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"github.com/hyperhq/runv/hypervisor/pod"
	"github.com/hyperhq/runv/hypervisor/types"
	"sync"
)

type PersistVolumeInfo struct {
	Name       string
	Filename   string
	Format     string
	Fstype     string
	DeviceName string
	ScsiId     int
	Containers []int
	MontPoints []string
}

type PersistNetworkInfo struct {
	Index      int
	PciAddr    int
	DeviceName string
	IpAddr     string
}

type PersistInfo struct {
	Id          string
	DriverInfo  map[string]interface{}
	UserSpec    *pod.UserPod
	VmSpec      *VmPod
	HwStat      *VmHwStatus
	VolumeList  []*PersistVolumeInfo
	NetworkList []*PersistNetworkInfo
}

func (ctx *VmContext) dump() (*PersistInfo, error) {
	dr, err := ctx.DCtx.Dump()
	if err != nil {
		return nil, err
	}

	info := &PersistInfo{
		Id:          ctx.Id,
		DriverInfo:  dr,
		UserSpec:    ctx.userSpec,
		VmSpec:      ctx.vmSpec,
		HwStat:      ctx.dumpHwInfo(),
		VolumeList:  make([]*PersistVolumeInfo, len(ctx.devices.imageMap)+len(ctx.devices.volumeMap)),
		NetworkList: make([]*PersistNetworkInfo, len(ctx.devices.networkMap)),
	}

	vid := 0
	for _, image := range ctx.devices.imageMap {
		info.VolumeList[vid] = image.info.dump()
		info.VolumeList[vid].Containers = []int{image.pos}
		info.VolumeList[vid].MontPoints = []string{"/"}
		vid++
	}

	for _, vol := range ctx.devices.volumeMap {
		info.VolumeList[vid] = vol.info.dump()
		mps := len(vol.pos)
		info.VolumeList[vid].Containers = make([]int, mps)
		info.VolumeList[vid].MontPoints = make([]string, mps)
		i := 0
		for idx, mp := range vol.pos {
			info.VolumeList[vid].Containers[i] = idx
			info.VolumeList[vid].MontPoints[i] = mp
			i++
		}
		vid++
	}

	nid := 0
	for _, nic := range ctx.devices.networkMap {
		info.NetworkList[nid] = &PersistNetworkInfo{
			Index:      nic.Index,
			PciAddr:    nic.PCIAddr,
			DeviceName: nic.DeviceName,
			IpAddr:     nic.IpAddr,
		}
		nid++
	}

	return info, nil
}

func (ctx *VmContext) dumpHwInfo() *VmHwStatus {
	return &VmHwStatus{
		PciAddr:  ctx.pciAddr,
		ScsiId:   ctx.scsiId,
		AttachId: ctx.attachId,
	}
}

func (ctx *VmContext) loadHwStatus(pinfo *PersistInfo) {
	ctx.pciAddr = pinfo.HwStat.PciAddr
	ctx.scsiId = pinfo.HwStat.ScsiId
	ctx.attachId = pinfo.HwStat.AttachId
}

func (blk *BlockDescriptor) dump() *PersistVolumeInfo {
	return &PersistVolumeInfo{
		Name:       blk.Name,
		Filename:   blk.Filename,
		Format:     blk.Format,
		Fstype:     blk.Fstype,
		DeviceName: blk.DeviceName,
		ScsiId:     blk.ScsiId,
	}
}

func (vol *PersistVolumeInfo) blockInfo() *BlockDescriptor {
	return &BlockDescriptor{
		Name:       vol.Name,
		Filename:   vol.Filename,
		Format:     vol.Format,
		Fstype:     vol.Fstype,
		DeviceName: vol.DeviceName,
		ScsiId:     vol.ScsiId,
	}
}

func (cr *VmContainer) roLookup(mpoint string) bool {
	if v := cr.volLookup(mpoint); v != nil {
		return v.ReadOnly
	} else if m := cr.mapLookup(mpoint); m != nil {
		return m.ReadOnly
	}

	return false
}

func (cr *VmContainer) mapLookup(mpoint string) *VmFsmapDescriptor {
	for _, fs := range cr.Fsmap {
		if fs.Path == mpoint {
			return &fs
		}
	}
	return nil
}

func (cr *VmContainer) volLookup(mpoint string) *VmVolumeDescriptor {
	for _, vol := range cr.Volumes {
		if vol.Mount == mpoint {
			return &vol
		}
	}
	return nil
}

func vmDeserialize(s []byte) (*PersistInfo, error) {
	info := &PersistInfo{}
	err := json.Unmarshal(s, info)
	return info, err
}

func (pinfo *PersistInfo) serialize() ([]byte, error) {
	return json.Marshal(pinfo)
}

func (pinfo *PersistInfo) vmContext(hub chan VmEvent, client chan *types.VmResponse,
	wg *sync.WaitGroup) (*VmContext, error) {

	dc, err := HDriver.LoadContext(pinfo.DriverInfo)
	if err != nil {
		glog.Error("cannot load driver context: ", err.Error())
		return nil, err
	}

	ctx, err := InitContext(pinfo.Id, hub, client, dc, &BootConfig{}, types.VM_KEEP_NONE)
	if err != nil {
		return nil, err
	}

	ctx.vmSpec = pinfo.VmSpec
	ctx.userSpec = pinfo.UserSpec
	ctx.wg = wg

	ctx.loadHwStatus(pinfo)

	for idx, container := range ctx.vmSpec.Containers {
		ctx.ptys.ttys[container.Tty] = newAttachments(idx, true)
		if container.Stderr > 0 {
			ctx.ptys.ttys[container.Stderr] = newAttachments(idx, true)
		}
	}

	for _, vol := range pinfo.VolumeList {
		binfo := vol.blockInfo()
		if len(vol.Containers) != len(vol.MontPoints) {
			return nil, errors.New("persistent data corrupt, volume info mismatch")
		}
		if len(vol.MontPoints) == 1 && vol.MontPoints[0] == "/" {
			img := &imageInfo{
				info: binfo,
				pos:  vol.Containers[0],
			}
			ctx.devices.imageMap[vol.Name] = img
		} else {
			v := &volumeInfo{
				info:     binfo,
				pos:      make(map[int]string),
				readOnly: make(map[int]bool),
			}
			for i := 0; i < len(vol.Containers); i++ {
				idx := vol.Containers[i]
				v.pos[idx] = vol.MontPoints[i]
				v.readOnly[idx] = ctx.vmSpec.Containers[idx].roLookup(vol.MontPoints[i])
			}
		}
	}

	for _, nic := range pinfo.NetworkList {
		ctx.devices.networkMap[nic.Index] = &InterfaceCreated{
			Index:      nic.Index,
			PCIAddr:    nic.PciAddr,
			DeviceName: nic.DeviceName,
			IpAddr:     nic.IpAddr,
		}
	}

	return ctx, nil
}
