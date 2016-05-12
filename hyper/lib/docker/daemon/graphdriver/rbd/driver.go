package rbd

import (
	"os"
	"io/ioutil"
	"path"
	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/pkg/idtools"
	"github.com/docker/docker/pkg/mount"
	"github.com/hyperhq/hyper/lib/docker/daemon/graphdriver"
)

func init() {
	graphdriver.Register("rbd",Init)
}

type Driver struct {
	home	string
	*RbdSet
	uidMaps []idtools.IDMap
	gidMaps []idtools.IDMap
}

func Init(home string,options []string, uidMaps, gidMaps []idtools.IDMap) (graphdriver.Driver,error) {
	rbdSet,err := NewRbdSet(home,true,options,uidMaps,gidMaps)
	if err != nil {
		return nil,err
	}
	if err := os.MkdirAll(home,0700); err != nil && !os.IsExist(err) {
		logrus.Errorf("Rbd create home dir %s failed: %v",home,err)
		return nil,err
	}

	// MakePrivate ensures a mounted filesystem has the PRIVATE mount option enabled
/*	if err := mount.MakePrivate(home); err != nil {
		return nil,err
	}*/
	d := &Driver {
		home:		home,
		RbdSet:		rbdSet,
		uidMaps:	uidMaps,
		gidMaps:	gidMaps,
	}
	return graphdriver.NewNaiveDiffDriver(d,uidMaps,gidMaps),nil
}

func (d *Driver) String() string {
	return "rbd"
}

func (d *Driver) Status() [][2]string {
	status := [][2]string{

	}
	return status
}

// GetMetadata returns a map of information about the device.
func (d *Driver) GetMetadata(id string) (map[string]string,error) {
	//m,err := d.RbdSet.exportDeviceMetadata(id)
/*
	if err != nil {
		return nil,err
	}*/

	metadata := make(map[string]string)
	//......
	return metadata,nil
}

func (d *Driver) Cleanup() error {
	err := d.RbdSet.Shutdown()
	if err2 := mount.Unmount(d.home); err2 == nil {
		err = err2
	}

	return err
}

func (d *Driver) Create(id, parent, mountLabel string) error {
	if err := d.RbdSet.AddDevice(id,parent); err != nil {
		return err 
	}

	return nil
}

func (d *Driver) Remove(id string) error {
	if !d.RbdSet.HasDevice(id) {
		return nil
	}

	if err := d.RbdSet.DeleteDevice(id); err != nil {
		return err
	}

	mp := path.Join(d.home,"mnt",id)
	if err := os.RemoveAll(mp); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// Get mounts a device with given id into the root filesystem
func (d *Driver) Get(id,mountLabel string) (string,error) {
	mp := path.Join(d.home,"mnt",id)

	uid,gid,err := idtools.GetRootUIDGID(d.uidMaps,d.gidMaps)
	if err != nil {
		return "",err
	}
	// Create the target directories if they don't exist
	if err := idtools.MkdirAllAs(path.Join(d.home,"mnt"),0755,uid,gid); err != nil && !os.IsExist(err) {
		return "",err
	}
	if err := idtools.MkdirAs(mp,0755,uid,gid); err != nil && !os.IsExist(err) {
		return "",err
	}

	// Mount the device
	if err := d.RbdSet.MountDevice(id,mp,mountLabel); err != nil {
		return "",err
	}

	rootFs := path.Join(mp,"rootfs")
	if err := idtools.MkdirAllAs(rootFs,0755,uid,gid); err != nil && os.IsExist(err) {
		d.RbdSet.UnmountDevice(id)
		return "",err
	}

	idFile := path.Join(mp,id)
	if _,err := os.Stat(idFile); err != nil && os.IsNotExist(err) {
		// Create an "id" file with the container/image id in it to help reconstruct this in case 
		// of later problems
		if err := ioutil.WriteFile(idFile,[]byte(id),0600); err != nil {
			d.RbdSet.UnmountDevice(id)
			return "",err
		}
	}

	return rootFs,nil
}


// Put Unmount a device and removes it
func (d *Driver) Put(id string) error {
	err := d.RbdSet.UnmountDevice(id)

	if err != nil {
		logrus.Errorf("Error unmounting device %s: %s",id,err)
	}
	//return err
	return nil
}

//Exists checks to see if the device exists.
func (d *Driver) Exists(id string) bool {
	return d.RbdSet.HasDevice(id)
	return false
}

func (d *Driver) Setup() error {
	return nil
}