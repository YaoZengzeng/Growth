package client

import (
	"strings"
	"fmt"
	"net/url"

	"github.com/hyperhq/hyper/engine"

	gflag "github.com/jessevdk/go-flags"
)

func (cli *HyperClient) HyperCmdMigrate(args ...string) error {
	var opts struct {
		Ip 		string	`short:"i" long:"ip" default:"127.0.0.1" value-name:"127.0.0.1" description:"destination vm's ip"`
		Port 	string	`short:"p" long:"port" default:"12345" value-name:"12345" description:"destination vm's port"`
	}
	var parser = gflag.NewParser(&opts,gflag.Default)
	parser.Usage = "migrate [-i 127.0.0.1 -p 12345]| POD_ID \n\nMigrate a 'running' pod"
	args,err := parser.Parse()
	if err != nil {
		if !strings.Contains(err.Error(),"Usage") {
			return err
		} else {
			return nil
		}
	}

	if len(args) == 1 {
		return fmt.Errorf("\"migrate\" requires a minimum of 1 argument, please provide POD ID.\n")
	}

	podId 	:= args[1]
	ip 		:= opts.Ip
	port 	:= opts.Port


	_,_,err = cli.MigratePod(podId, ip, port)
	if err != nil {
		return err
	}

	fmt.Printf("The POD is: %s! migrate command executed successfully\n",podId)
	return nil

}

func (cli *HyperClient) MigratePod(podId string, ip string, port string) (int, string, error) {
	v := url.Values{}
	v.Set("podId",podId)
	v.Set("ip",ip)
	v.Set("port",port)
	
	body,_,err := readBody(cli.call("POST","/pod/migrate?"+v.Encode(),nil,nil))
	if err != nil {
		if strings.Contains(err.Error(),"leveldb: not found") {
			return -1,"",fmt.Errorf("Can not find that POD ID to migrate, please check your POD ID!")
		}
		return -1, "", err
	}

	out := engine.NewOutput()
	remoteInfo,err := out.AddEnv()
	if err != nil {
		return -1, "", err
	}

	if _, err := out.Write(body); err != nil {
		return -1, "", err
	}
	out.Close()
	// This 'ID' stands for pod ID
	// This 'Code' ...
	// This 'Cause' ..
	if remoteInfo.Exists("ID") {
		// TODO ...
	}

	return remoteInfo.GetInt("Code"), remoteInfo.Get("Cause"),nil

}

func (cli *HyperClient) HyperCmdListen(args ...string) error {
	var opts struct {
		Ip 		string	`short:"i" long:"ip" default:"0.0.0.0" value-name:"0" description:"destination vm's ip"`
		Port 	string	`short:"p" long:"port" default:"12345" value-name:"12345" description:"destination vm's port"`
	}
	var parser = gflag.NewParser(&opts,gflag.Default)
	parser.Usage = "migrate [-i 0 -p 12345]| POD_ID \n\nMigrate a 'running' pod"
	args,err := parser.Parse()
	if err != nil {
		if !strings.Contains(err.Error(),"Usage") {
			return err
		} else {
			return nil
		}
	}

	ip 		:= opts.Ip
	port 	:= opts.Port

	_,_,err = cli.ListenPod(ip, port)
	if err != nil {
		return err
	}

	fmt.Printf("Now listening successfully!\n")
	return nil

}

func (cli *HyperClient) ListenPod(ip, port string) (int, string, error) {
	v := url.Values{}
	v.Set("ip",ip)
	v.Set("port",port)
	body,_,err := readBody(cli.call("POST","/pod/listen?"+v.Encode(),nil,nil))
	if err != nil {
		if strings.Contains(err.Error(),"leveldb: not found") {
			return -1,"",fmt.Errorf("Can not find that POD ID to migrate, please check your POD ID!")
		}
		return -1, "", err
	}
	out := engine.NewOutput()
	remoteInfo,err := out.AddEnv()
	if err != nil {
		return -1, "", err
	}

	if _, err := out.Write(body); err != nil {
		return -1, "", err
	}
	out.Close()
	// This 'ID' stands for pod ID
	// This 'Code' ...
	// This 'Cause' ..
	if remoteInfo.Exists("ID") {
		// TODO ...
	}

	return remoteInfo.GetInt("Code"), remoteInfo.Get("Cause"),nil

}