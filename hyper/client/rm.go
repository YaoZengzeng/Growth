package client

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/hyperhq/hyper/engine"
	"github.com/hyperhq/runv/hypervisor/types"

	gflag "github.com/jessevdk/go-flags"
)

func (cli *HyperClient) HyperCmdRm(args ...string) error {
	var parser = gflag.NewParser(nil, gflag.Default)
	parser.Usage = "rm POD [POD...]\n\nRemove one or more pods"
	args, err := parser.Parse()
	if err != nil {
		if !strings.Contains(err.Error(), "Usage") {
			return err
		} else {
			return nil
		}
	}
	if len(args) < 2 {
		return fmt.Errorf("\"rm\" requires a minimum of 1 argument, please provide POD ID.\n")
	}
	pods := args[1:]
	for _, id := range pods {
		err := cli.RmPod(id)
		if err == nil {
			fmt.Fprintf(cli.out, "Pod(%s) is successful to be deleted!\n", id)
		} else {
			fmt.Fprintf(cli.out, "%v\n", err)
		}
	}
	return nil
}

func (cli *HyperClient) RmPod(id string) error {
	v := url.Values{}
	v.Set("podId", id)
	body, _, err := readBody(cli.call("DELETE", "/pod?"+v.Encode(), nil, nil))
	if err != nil {
		return fmt.Errorf("Error to remove pod(%s), %s", id, err.Error())
	}
	out := engine.NewOutput()
	remoteInfo, err := out.AddEnv()
	if err != nil {
		return fmt.Errorf("Error to remove pod(%s), %s", id, err.Error())
	}

	if _, err := out.Write(body); err != nil {
		return fmt.Errorf("Error to remove pod(%s), %s", id, err.Error())
	}
	out.Close()
	errCode := remoteInfo.GetInt("Code")
	if !(errCode == types.E_OK || errCode == types.E_VM_SHUTDOWN) {
		return fmt.Errorf("Error to remove pod(%s), %s", id, remoteInfo.Get("Cause"))
	}
	return nil
}
