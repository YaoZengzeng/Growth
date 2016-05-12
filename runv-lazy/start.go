package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/hyperhq/runv/lib/utils"
	"github.com/kardianos/osext"
	"github.com/opencontainers/runtime-spec/specs-go"
)

type startConfig struct {
	Name       string
	BundlePath string
	Root       string
	Driver     string
	Kernel     string
	Initrd     string
	Vbox       string

	*specs.Spec `json:"config"`
}

func loadStartConfig(context *cli.Context) (*startConfig, error) {
	config := &startConfig{
		Name:       context.Args().First(),
		Root:       context.GlobalString("root"),
		Driver:     context.GlobalString("driver"),
		Kernel:     context.GlobalString("kernel"),
		Initrd:     context.GlobalString("initrd"),
		Vbox:       context.GlobalString("vbox"),
		BundlePath: context.String("bundle"),
	}
	var err error

	if config.Name == "" {
		return nil, fmt.Errorf("Please specify container ID")
	}

	// If config.BundlePath is "", this code sets it to the current work directory
	if !filepath.IsAbs(config.BundlePath) {
		config.BundlePath, err = filepath.Abs(config.BundlePath)
		if err != nil {
			return nil, fmt.Errorf("Cannot get abs path for bundle path: %s\n", err.Error())
		}
	}

	ocffile := filepath.Join(config.BundlePath, specConfig)
	config.Spec, err = loadSpec(ocffile)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func firstExistingFile(candidates []string) string {
	for _, file := range candidates {
		if _, err := os.Stat(file); err == nil {
			return file
		}
	}
	return ""
}

var startCommand = cli.Command{
	Name:  "start",
	Usage: "create and run a container",
	ArgsUsage: `<container-id>

Where "<container-id>" is your name for the instance of the container that you
are starting. The name you provide for the container instance must be unique on
your host.`,
	Description: `The start command creates an instance of a container for a bundle. The bundle
is a directory with a specification file named "` + specConfig + `" and a root
filesystem.

The specification file includes an args parameter. The args parameter is used
to specify command(s) that get run when the container is started. To change the
command(s) that get executed on start, edit the args parameter of the spec. See
"runv spec --help" for more explanation.`,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "bundle, b",
			Usage: "path to the root of the bundle directory",
		},
	},
	Action: func(context *cli.Context) {
		config, err := loadStartConfig(context)
		if err != nil {
			fmt.Printf("load config failed %v\n", err)
			os.Exit(-1)
		}
		if os.Geteuid() != 0 {
			fmt.Printf("runv should be run as root\n")
			os.Exit(-1)
		}
		_, err = os.Stat(filepath.Join(config.Root, config.Name))
		if err == nil {
			fmt.Printf("Container %s exists\n", config.Name)
			os.Exit(-1)
		}

		var sharedContainer string
		for _, ns := range config.Spec.Linux.Namespaces {
			if ns.Path != "" {
				if strings.Contains(ns.Path, "/") {
					fmt.Printf("Runv doesn't support path to namespace file, it supports containers name as shared namespaces only\n")
					os.Exit(-1)
				}
				if ns.Type == "mount" {
					// TODO support it!
					fmt.Printf("Runv doesn't support shared mount namespace currently\n")
					os.Exit(-1)
				}
				sharedContainer = ns.Path
				_, err = os.Stat(filepath.Join(config.Root, sharedContainer, stateJson))
				if err != nil {
					fmt.Printf("The container %s is not existing or not ready\n", sharedContainer)
					os.Exit(-1)
				}
				_, err = os.Stat(filepath.Join(config.Root, sharedContainer, "runv.sock"))
				if err != nil {
					fmt.Printf("The container %s is not ready\n", sharedContainer)
					os.Exit(-1)
				}
			}
		}

		// only set the default Kernel/Initrd/Vbox when it is the first container(sharedContainer == "")
		if config.Kernel == "" && sharedContainer == "" && config.Driver != "vbox" {
			config.Kernel = firstExistingFile([]string{
				filepath.Join(config.BundlePath, config.Spec.Root.Path, "boot/vmlinuz"),
				filepath.Join(config.BundlePath, "boot/vmlinuz"),
				filepath.Join(config.BundlePath, "vmlinuz"),
				"/var/lib/hyper/kernel",
			})
		}
		if config.Initrd == "" && sharedContainer == "" && config.Driver != "vbox" {
			config.Initrd = firstExistingFile([]string{
				filepath.Join(config.BundlePath, config.Spec.Root.Path, "boot/initrd.img"),
				filepath.Join(config.BundlePath, "boot/initrd.img"),
				filepath.Join(config.BundlePath, "initrd.img"),
				"/var/lib/hyper/hyper-initrd.img",
			})
		}
		if config.Vbox == "" && sharedContainer == "" && config.Driver == "vbox" {
			config.Vbox = firstExistingFile([]string{
				filepath.Join(config.BundlePath, "vbox.iso"),
				"/opt/hyper/static/iso/hyper-vbox-boot.iso",
			})
		}

		// convert the paths to abs
		if config.Kernel != "" && !filepath.IsAbs(config.Kernel) {
			config.Kernel, err = filepath.Abs(config.Kernel)
			if err != nil {
				fmt.Printf("Cannot get abs path for kernel: %s\n", err.Error())
				os.Exit(-1)
			}
		}
		if config.Initrd != "" && !filepath.IsAbs(config.Initrd) {
			config.Initrd, err = filepath.Abs(config.Initrd)
			if err != nil {
				fmt.Printf("Cannot get abs path for initrd: %s\n", err.Error())
				os.Exit(-1)
			}
		}
		if config.Vbox != "" && !filepath.IsAbs(config.Vbox) {
			config.Vbox, err = filepath.Abs(config.Vbox)
			if err != nil {
				fmt.Printf("Cannot get abs path for vbox: %s\n", err.Error())
				os.Exit(-1)
			}
		}

		if sharedContainer != "" {
			initCmd := &initContainerCmd{Name: config.Name, Root: config.Root}
			conn, err := runvRequest(config.Root, sharedContainer, RUNV_INITCONTAINER, initCmd)
			if err != nil {
				os.Exit(-1)
			}
			conn.Close()
		} else {
			path, err := osext.Executable()
			if err != nil {
				fmt.Printf("cannot find self executable path for %s: %v\n", os.Args[0], err)
				os.Exit(-1)
			}

			os.MkdirAll(context.String("log_dir"), 0755)
			args := []string{
				"runv-ns-daemon",
				"--root", config.Root,
				"--id", config.Name,
			}
			if context.GlobalBool("debug") {
				args = append(args, "-v", "3", "--log_dir", context.GlobalString("log_dir"))
			}
			_, err = utils.ExecInDaemon(path, args)
			if err != nil {
				fmt.Printf("failed to launch runv daemon, error:%v\n", err)
				os.Exit(-1)
			}
		}

		status, err := startContainer(config)
		if err != nil {
			fmt.Printf("start container failed: %v", err)
		}
		os.Exit(status)
	},
}

type initContainerCmd struct {
	Name string
	Root string
}

// Shared namespaces multiple containers suppurt
// The runv supports pod-style shared namespaces currently.
// (More fine grain shared namespaces style (docker/runc style) is under implementation)
//
// Pod-style shared namespaces:
// * if two containers share at least one type of namespace, they share all kinds of namespaces except the mount namespace
// * mount namespace can't be shared, each container has its own mount namespace
//
// Implementation detail:
// * Shared namespaces is configured in Spec.Linux.Namespaces, the namespace Path should be existing container name.
// * In runv, shared namespaces multiple containers are located in the same VM which is managed by a runv-daemon.
// * Any running container can exit in any arbitrary order, the runv-daemon and the VM are existed until the last container of the VM is existed

func startContainer(config *startConfig) (int, error) {
	conn, err := runvRequest(config.Root, config.Name, RUNV_STARTCONTAINER, config)
	if err != nil {
		return -1, err
	}

	return containerTtySplice(config.Root, config.Name, conn, true)
}
