package daemon

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"time"

	dockertypes "github.com/docker/docker/api/types"
	"github.com/golang/glog"
	"github.com/hyperhq/hyper/storage"
	"github.com/hyperhq/hyper/storage/aufs"
	dm "github.com/hyperhq/hyper/storage/devicemapper"
	"github.com/hyperhq/hyper/storage/overlay"
	"github.com/hyperhq/hyper/storage/vbox"
	"github.com/hyperhq/hyper/storage/rbd"
	"github.com/hyperhq/hyper/utils"
	"github.com/hyperhq/runv/hypervisor"
	"github.com/hyperhq/runv/hypervisor/pod"
)

const (
	DEFAULT_DM_POOL      string = "hyper-volume-pool"
	DEFAULT_DM_POOL_SIZE int    = 20971520 * 512
	DEFAULT_DM_DATA_LOOP string = "/dev/loop6"
	DEFAULT_DM_META_LOOP string = "/dev/loop7"
	DEFAULT_DM_VOL_SIZE  int    = 2 * 1024 * 1024 * 1024
	DEFAULT_VOL_FS              = "ext4"
)

type Storage interface {
	Type() string
	RootPath() string

	Init() error
	CleanUp() error

	PrepareContainer(id, sharedir string) (*hypervisor.ContainerInfo, error)
	InjectFile(src io.Reader, containerId, target, rootDir string, perm, uid, gid int) error
	CreateVolume(daemon *Daemon, podId, shortName string) (*hypervisor.VolumeInfo, error)
	RemoveVolume(podId string, record []byte) error
}

var StorageDrivers map[string]func(*dockertypes.Info) (Storage, error) = map[string]func(*dockertypes.Info) (Storage, error){
	"devicemapper": DMFactory,
	"aufs":         AufsFactory,
	"overlay":      OverlayFsFactory,
	"vbox":         VBoxStorageFactory,
	"rbd":			RbdStorageFactory,
}

func StorageFactory(sysinfo *dockertypes.Info) (Storage, error) {
	if factory, ok := StorageDrivers[sysinfo.Driver]; ok {
		return factory(sysinfo)
	}
	return nil, fmt.Errorf("hyperd can not support docker's backing storage: %s", sysinfo.Driver)
}

func ProbeExistingVolume(v *pod.UserVolume, sharedDir string) (*hypervisor.VolumeInfo, error) {
	if v == nil || v.Source == "" { //do not create volume in this function, it depends on storage driver.
		return nil, fmt.Errorf("can not generate volume info from %v", v)
	}

	var err error = nil
	vol := &hypervisor.VolumeInfo{
		Name: v.Name,
	}

	if v.Driver == "vfs" {
		vol.Fstype = "dir"
		vol.Filepath, err = storage.MountVFSVolume(v.Source, sharedDir)
		if err != nil {
			return nil, err
		}
		glog.V(1).Infof("dir %s is bound to %s", v.Source, vol.Filepath)
	} else {
		vol.Fstype, err = dm.ProbeFsType(v.Source)
		if err != nil {
			vol.Fstype = DEFAULT_VOL_FS //FIXME: for qcow2, the ProbeFsType doesn't work, should be fix later
		}
		vol.Format = v.Driver
		vol.Filepath = v.Source
	}

	return vol, nil
}

type DevMapperStorage struct {
	CtnPoolName string
	VolPoolName string
	DevPrefix   string
	FsType      string
	rootPath    string
	DmPoolData  *dm.DeviceMapper
}

func DMFactory(sysinfo *dockertypes.Info) (Storage, error) {
	driver := &DevMapperStorage{}

	driver.VolPoolName = DEFAULT_DM_POOL

	for _, pair := range sysinfo.DriverStatus {
		if pair[0] == "Pool Name" {
			driver.CtnPoolName = pair[1]
		}
		if pair[0] == "Backing Filesystem" {
			if strings.Contains(pair[1], "ext") {
				driver.FsType = "ext4"
			} else if strings.Contains(pair[1], "xfs") {
				driver.FsType = "xfs"
			} else {
				driver.FsType = "dir"
			}
			break
		}
	}
	driver.DevPrefix = driver.CtnPoolName[:strings.Index(driver.CtnPoolName, "-pool")]
	driver.rootPath = path.Join(utils.HYPER_ROOT, "devicemapper")
	return driver, nil
}

func (dms *DevMapperStorage) Type() string {
	return "devicemapper"
}

func (dms *DevMapperStorage) RootPath() string {
	return dms.rootPath
}

func (dms *DevMapperStorage) Init() error {
	dmPool := dm.DeviceMapper{
		Datafile:         path.Join(utils.HYPER_ROOT, "lib") + "/data",
		Metadatafile:     path.Join(utils.HYPER_ROOT, "lib") + "/metadata",
		DataLoopFile:     DEFAULT_DM_DATA_LOOP,
		MetadataLoopFile: DEFAULT_DM_META_LOOP,
		PoolName:         dms.VolPoolName,
		Size:             DEFAULT_DM_POOL_SIZE,
	}
	dms.DmPoolData = &dmPool
	rand.Seed(time.Now().UnixNano())

	// Prepare the DeviceMapper storage
	return dm.CreatePool(&dmPool)
}

func (dms *DevMapperStorage) CleanUp() error {
	return dm.DMCleanup(dms.DmPoolData)
}

func (dms *DevMapperStorage) PrepareContainer(id, sharedDir string) (*hypervisor.ContainerInfo, error) {
	if err := dm.CreateNewDevice(id, dms.DevPrefix, dms.RootPath()); err != nil {
		return nil, err
	}
	devFullName, err := dm.MountContainerToSharedDir(id, sharedDir, dms.DevPrefix)
	if err != nil {
		glog.Error("got error when mount container to share dir ", err.Error())
		return nil, err
	}
	fstype, err := dm.ProbeFsType(devFullName)
	if err != nil {
		fstype = "ext4"
	}
	return &hypervisor.ContainerInfo{
		Id:     id,
		Rootfs: "/rootfs",
		Image:  devFullName,
		Fstype: fstype,
	}, nil
}

func (dms *DevMapperStorage) InjectFile(src io.Reader, containerId, target, rootDir string, perm, uid, gid int) error {
	return dm.InjectFile(src, containerId, dms.DevPrefix, target, rootDir, perm, uid, gid)
}

func (dms *DevMapperStorage) CreateVolume(daemon *Daemon, podId, shortName string) (*hypervisor.VolumeInfo, error) {
	volName := fmt.Sprintf("%s-%s-%s", dms.VolPoolName, podId, shortName)
	dev_id, _ := daemon.GetVolumeId(podId, volName)
	glog.Infof("DeviceID is %d", dev_id)

	restore := dev_id > 0

	for {
		if !restore {
			dev_id = dms.randDevId()
		}
		dev_id_str := strconv.Itoa(dev_id)

		err := dm.CreateVolume(dms.VolPoolName, volName, dev_id_str, DEFAULT_DM_VOL_SIZE, restore)
		if err != nil && !restore && strings.Contains(err.Error(), "failed: File exists") {
			glog.V(1).Infof("retry for dev_id #%d creating collision: %v", dev_id, err)
			continue
		} else if err != nil {
			glog.V(1).Infof("failed to create dev_id #%d: %v", dev_id, err)
			return nil, err
		}

		glog.V(3).Infof("device (%d) created (restore:%v) for %s: %s", dev_id, restore, podId, volName)
		daemon.SetVolumeId(podId, volName, dev_id_str)
		break
	}

	fstype, err := dm.ProbeFsType("/dev/mapper/" + volName)
	if err != nil {
		fstype = "ext4"
	}

	glog.V(1).Infof("volume %s created with dm as %s", shortName, volName)

	return &hypervisor.VolumeInfo{
		Name:     shortName,
		Filepath: path.Join("/dev/mapper/", volName),
		Fstype:   fstype,
		Format:   "raw",
	}, nil
}

func (dms *DevMapperStorage) RemoveVolume(podId string, record []byte) error {
	fields := strings.Split(string(record), ":")
	dev_id, _ := strconv.Atoi(fields[1])
	if err := dm.DeleteVolume(dms.DmPoolData, dev_id); err != nil {
		glog.Error(err.Error())
		return err
	}
	return nil
}

func (dms *DevMapperStorage) randDevId() int {
	return rand.Intn(1<<24-1) + 1 // 0 reserved for pool device
}

type AufsStorage struct {
	rootPath string
}

func AufsFactory(sysinfo *dockertypes.Info) (Storage, error) {
	driver := &AufsStorage{}
	for _, pair := range sysinfo.DriverStatus {
		if pair[0] == "Root Dir" {
			driver.rootPath = pair[1]
		}
	}
	return driver, nil
}

func (a *AufsStorage) Type() string {
	return "aufs"
}

func (a *AufsStorage) RootPath() string {
	return a.rootPath
}

func (*AufsStorage) Init() error { return nil }

func (*AufsStorage) CleanUp() error { return nil }

func (a *AufsStorage) PrepareContainer(id, sharedDir string) (*hypervisor.ContainerInfo, error) {
	_, err := aufs.MountContainerToSharedDir(id, a.RootPath(), sharedDir, "")
	if err != nil {
		glog.Error("got error when mount container to share dir ", err.Error())
		return nil, err
	}
	devFullName := "/" + id + "/rootfs"
	return &hypervisor.ContainerInfo{
		Id:     id,
		Rootfs: "",
		Image:  devFullName,
		Fstype: "dir",
	}, nil
}

func (a *AufsStorage) InjectFile(src io.Reader, containerId, target, rootDir string, perm, uid, gid int) error {
	return storage.FsInjectFile(src, containerId, target, rootDir, perm, uid, gid)
}

func (a *AufsStorage) CreateVolume(daemon *Daemon, podId, shortName string) (*hypervisor.VolumeInfo, error) {
	volName, err := storage.CreateVFSVolume(podId, shortName)
	if err != nil {
		return nil, err
	}
	return &hypervisor.VolumeInfo{
		Name:     shortName,
		Filepath: volName,
		Fstype:   "dir",
	}, nil
}

func (a *AufsStorage) RemoveVolume(podId string, record []byte) error {
	return nil
}

type OverlayFsStorage struct {
	rootPath string
}

func OverlayFsFactory(_ *dockertypes.Info) (Storage, error) {
	driver := &OverlayFsStorage{
		rootPath: path.Join(utils.HYPER_ROOT, "overlay"),
	}
	return driver, nil
}

func (o *OverlayFsStorage) Type() string {
	return "overlay"
}

func (o *OverlayFsStorage) RootPath() string {
	return o.rootPath
}

func (*OverlayFsStorage) Init() error { return nil }

func (*OverlayFsStorage) CleanUp() error { return nil }

func (o *OverlayFsStorage) PrepareContainer(id, sharedDir string) (*hypervisor.ContainerInfo, error) {
	_, err := overlay.MountContainerToSharedDir(id, o.RootPath(), sharedDir, "")
	if err != nil {
		glog.Error("got error when mount container to share dir ", err.Error())
		return nil, err
	}
	devFullName := "/" + id + "/rootfs"
	return &hypervisor.ContainerInfo{
		Id:     id,
		Rootfs: "",
		Image:  devFullName,
		Fstype: "dir",
	}, nil
}

func (o *OverlayFsStorage) InjectFile(src io.Reader, containerId, target, rootDir string, perm, uid, gid int) error {
	return storage.FsInjectFile(src, containerId, target, rootDir, perm, uid, gid)
}

func (o *OverlayFsStorage) CreateVolume(daemon *Daemon, podId, shortName string) (*hypervisor.VolumeInfo, error) {
	volName, err := storage.CreateVFSVolume(podId, shortName)
	if err != nil {
		return nil, err
	}
	return &hypervisor.VolumeInfo{
		Name:     shortName,
		Filepath: volName,
		Fstype:   "dir",
	}, nil
}

func (o *OverlayFsStorage) RemoveVolume(podId string, record []byte) error {
	return nil
}

type VBoxStorage struct {
	rootPath string
}

func VBoxStorageFactory(_ *dockertypes.Info) (Storage, error) {
	driver := &VBoxStorage{
		rootPath: path.Join(utils.HYPER_ROOT, "vbox"),
	}
	return driver, nil
}

func (v *VBoxStorage) Type() string {
	return "vbox"
}

func (v *VBoxStorage) RootPath() string {
	return v.rootPath
}

func (*VBoxStorage) Init() error { return nil }

func (*VBoxStorage) CleanUp() error { return nil }

func (v *VBoxStorage) PrepareContainer(id, sharedDir string) (*hypervisor.ContainerInfo, error) {
	devFullName, err := vbox.MountContainerToSharedDir(id, v.RootPath(), "")
	if err != nil {
		glog.Error("got error when mount container to share dir ", err.Error())
		return nil, err
	}
	return &hypervisor.ContainerInfo{
		Id:     id,
		Rootfs: "/rootfs",
		Image:  devFullName,
		Fstype: "ext4",
	}, nil
}

func (v *VBoxStorage) InjectFile(src io.Reader, containerId, target, rootDir string, perm, uid, gid int) error {
	return errors.New("vbox storage driver does not support file insert yet")
}

func (v *VBoxStorage) CreateVolume(daemon *Daemon, podId, shortName string) (*hypervisor.VolumeInfo, error) {
	volName, err := storage.CreateVFSVolume(podId, shortName)
	if err != nil {
		return nil, err
	}
	return &hypervisor.VolumeInfo{
		Name:     shortName,
		Filepath: volName,
		Fstype:   "dir",
	}, nil
}

func (v *VBoxStorage) RemoveVolume(podId string, record []byte) error {
	return nil
}


type RbdStorage struct {
	rootPath 	string
	RbdPrefix	string
	RbdPool		string
}

func RbdStorageFactory(_*dockertypes.Info) (Storage, error) {
	driver := &RbdStorage {
		rootPath:	path.Join(utils.HYPER_ROOT,"rbd"),
		RbdPrefix:	"docker_image",
		RbdPool:	"rbd",
	}
	return driver,nil
}

func (r *RbdStorage) Type() string {
	return "rbd"
}

func (r *RbdStorage) RootPath() string {
	return r.rootPath
}

func (r *RbdStorage) Init() error {return nil}

func (r *RbdStorage) CleanUp() error {return nil}

func (r *RbdStorage) PrepareContainer(id, sharedDir string) (*hypervisor.ContainerInfo,error) {
	if err := rbd.MapImageToRbdDevice(id,r.RbdPrefix,r.RbdPool); err != nil {
		glog.Errorf("map the image to rbd device failed: ",err.Error)
		return nil,err
	}

	devFullName,err := rbd.MountContainerToSharedDir(id,r.RootPath(),r.RbdPrefix)
	if err != nil {
		glog.Errorf("got error when mount container to share dir",err.Error())
		return nil,err
	}
	return &hypervisor.ContainerInfo {
		Id:			id,
		Rootfs:		"/rootfs",
		Image:		devFullName,
		Fstype:		"ext4",
	},nil
}

func (r *RbdStorage) InjectFile(src io.Reader,containerId,target,rootDir string,perm,uid,gid int) error {
	//return errors.New("rbd storage driver does not support file insert yet")
	return nil
}

func (r *RbdStorage) CreateVolume(daemon *Daemon,podId,shortName string) (*hypervisor.VolumeInfo,error) {
	return nil,errors.New("rbd storage driver does not support volume yet")
}

func (r *RbdStorage) RemoveVolume(podId string,record []byte) error {
	return nil
}