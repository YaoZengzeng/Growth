package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/hyperhq/runv/hypervisor"
	"github.com/hyperhq/runv/lib/term"
)

type tty struct {
	root      string
	container string
	tag       string
	termFd    uintptr
	terminal  bool
}

type ttyWinSize struct {
	Tag    string
	Height int
	Width  int
}

// stdin/stdout <-> conn
func containerTtySplice(root, container string, conn net.Conn, isContainer bool) (int, error) {
	tag, err := runvGetTag(conn)
	if err != nil {
		return -1, err
	}
	fmt.Printf("tag=%s\n", tag)

	outFd, isTerminalOut := term.GetFdInfo(os.Stdout)
	newTty(root, container, tag, outFd, isTerminalOut).monitorTtySize()
	_, err = term.TtySplice(conn)
	if err != nil {
		return -1, err
	}

	cmd := &ttyTagCmd{Root: root, Container: "", Tag: tag}
	if isContainer {
		cmd.Container = container
	}
	conn, err = runvRequest(root, container, RUNV_EXITSTATUS, cmd)
	if err != nil {
		fmt.Printf("runvRequest failed: %v\n", err)
		return -1, err
	}
	defer conn.Close()

	msg, err := hypervisor.ReadVmMessage(conn.(*net.UnixConn))
	if err != nil {
		fmt.Printf("read runv server data failed: %v\n", err)
		return -1, err
	}

	if msg.Code != RUNV_EXITSTATUS {
		return -1, fmt.Errorf("unexpected respond code")
	}

	return int(msg.Message[0]), nil
}

func newTty(root, container, tag string, termFd uintptr, terminal bool) *tty {
	return &tty{
		root:      root,
		container: container,
		tag:       tag,
		termFd:    termFd,
		terminal:  terminal,
	}
}

func (tty *tty) resizeTty() {
	if !tty.terminal {
		return
	}

	height, width := getTtySize(tty.termFd)
	ttyCmd := &ttyWinSize{Tag: tty.tag, Height: height, Width: width}
	conn, err := runvRequest(tty.root, tty.container, RUNV_WINSIZE, ttyCmd)
	if err != nil {
		fmt.Printf("Failed to reset winsize")
		return
	}
	conn.Close()
}

func (tty *tty) monitorTtySize() {
	tty.resizeTty()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGWINCH)
	go func() {
		for range sigchan {
			tty.resizeTty()
		}
	}()
}

func getTtySize(termFd uintptr) (int, int) {
	ws, err := term.GetWinsize(termFd)
	if err != nil {
		fmt.Printf("Error getting size: %s", err.Error())
		if ws == nil {
			return 0, 0
		}
	}
	return int(ws.Height), int(ws.Width)
}
