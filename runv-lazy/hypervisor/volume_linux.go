package hypervisor

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/hyperhq/runv/hypervisor/pod"
)

func CreateContainer(userPod *pod.UserPod, sharedDir string, hub chan VmEvent) (string, error) {
	return "", nil
}

func UmountOverlayContainer(shareDir, image string, index int, hub chan VmEvent) {
	mount := filepath.Join(shareDir, image)
	success := true
	for i := 0; i < 10; i++ {
		time.Sleep(3 * time.Second / 1000)
		err := syscall.Unmount(mount, 0)
		if err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "device or resource busy") {
				success = true
				break
			}
			glog.Warningf("Cannot umount overlay %s: %s", mount, err.Error())
			success = false
		} else {
			success = true
			break
		}
	}
	hub <- &ContainerUnmounted{Index: index, Success: success}
}

func aufsUnmount(target string) error {
	glog.V(1).Infof("Ready to unmount the target : %s", target)
	if _, err := os.Stat(target); err != nil && os.IsNotExist(err) {
		return nil
	}
	cmdString := fmt.Sprintf("auplink %s flush", target)
	cmd := exec.Command("/bin/sh", "-c", cmdString)
	if err := cmd.Run(); err != nil {
		glog.Warningf("Couldn't run auplink command : %s\n", err.Error())
	}
	if err := syscall.Unmount(target, 0); err != nil {
		return err
	}
	return nil
}

func UmountAufsContainer(shareDir, image string, index int, hub chan VmEvent) {
	mount := filepath.Join(shareDir, image)
	success := true
	for i := 0; i < 10; i++ {
		time.Sleep(3 * time.Second / 1000)

		err := aufsUnmount(mount)
		if err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "device or resource busy") {
				success = true
				break
			}
			glog.Warningf("Cannot umount aufs %s: %s", mount, err.Error())
			success = false
		} else {
			success = true
			break
		}
	}
	hub <- &ContainerUnmounted{Index: index, Success: success}
}

func UmountVolume(shareDir, volPath string, name string, hub chan VmEvent) {
	mount := filepath.Join(shareDir, volPath)
	success := true
	err := syscall.Unmount(mount, 0)
	if err != nil {
		glog.Warningf("Cannot umount volume %s: %s", mount, err.Error())
		err = syscall.Unmount(mount, syscall.MNT_DETACH)
		if err != nil {
			glog.Warningf("Cannot lazy umount volume %s: %s", mount, err.Error())
			success = false
		} else {
			success = true
		}
	}
	if success == true {
		os.Remove(mount)
	}

	// After umount that device, we need to delete it
	hub <- &VolumeUnmounted{Name: name, Success: success}
}

func UmountDMDevice(deviceFullPath, name string, hub chan VmEvent) {
	args := fmt.Sprintf("dmsetup remove -f %s", deviceFullPath)
	cmd := exec.Command("/bin/sh", "-c", args)
	success := true
	if output, err := cmd.CombinedOutput(); err != nil {
		glog.Warningf("Cannot umount device %s: %s, %s", deviceFullPath, err.Error(), output)
		// retry
		cmd := exec.Command("/bin/sh", "-c", args)
		if err := cmd.Run(); err != nil {
			success = false
		}
	} else {
		// Command was successful
		success = true
	}

	// After umount that device, we need to delete it
	hub <- &BlockdevRemovedEvent{Name: name, Success: success}
}

func supportAufs() bool {
	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return false
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if strings.Contains(s.Text(), "aufs") {
			return true
		}
	}
	return false
}

func supportOverlay() bool {
	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return false
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if strings.Contains(s.Text(), "overlay") {
			return true
		}
	}
	return false
}
