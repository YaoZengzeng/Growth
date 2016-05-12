// +build linux

package rbd

import (
	"fmt"
	"os/exec"
	"encoding/json"
)

type RbdMappingInfo struct {
	Pool 		string		`json:"pool"`
	Name 		string		`json:"name"`
	Snapshot	string		`json:"snap"`
	Device 		string		`json:"device"`
}

// For rbd, we do not need to mount the container to sharedDir.
// All of we need to provide the block device name of container.
func MountContainerToSharedDir(containerId, sharedDir, devPrefix string) (string, error) {
	devFullName := fmt.Sprintf("/dev/rbd/rbd/%s_%s",devPrefix,containerId)
	return devFullName,nil
}

func MapImageToRbdDevice(containerId, devPrefix, poolName string) error {
	imageName := fmt.Sprintf("%s_%s",devPrefix,containerId)

	_,err := exec.Command("rbd","--pool",poolName,"map",imageName).Output()
	if err != nil {
		return err
	}

	v,_ := imageIsMapped(imageName,poolName)
	if v == true {
		return nil
	} else {
		return fmt.Errorf("Unable map image %s",imageName)
	}
}

func imageIsMapped(imageName, poolName string) (bool, error) {
	out,err := exec.Command("rbd","showmapped","--format","json").Output()
	if err != nil {
		fmt.Errorf("Rbd run rbd showmapped failed: %v",err)
		return false,err
	}

	mappingInfos := map[string]*RbdMappingInfo{}
	json.Unmarshal(out,&mappingInfos)

	for _,info := range mappingInfos {
		if info.Pool == poolName && info.Name == imageName {
			return true,nil
		}
	}

	return false,nil
}	