/* Accept commands from the dxfuse_tools program. The only command
* right now is sync, but this is the place to implement additional
* ones to come in the future.
*/
package dxfuse

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	// A port number for accepting commands
	CmdPort = 7205
)

type CmdServer struct {
	options  Options
	sybx    *SyncDbDx
}

type CmdClient struct {
}

type Args struct {
	// currently unused
	a int
}

func NewCmdServer(options Options, sybx *SyncDbDx) *CmdServer {
	cmdServer := &CmdServer{
		options: options,
		sybx : sybx,
	}
	return cmdServer
}

// write a log message, and add a header
func (csrv *CmdServer) log(a string, args ...interface{}) {
	LogMsg("CmdServer", a, args...)
}

func (csrv *CmdServer) Init() {
	rpc.Register(*csrv)
	rpc.HandleHTTP()

	// setup a server on the local host.
	l, err := net.Listen("tcp", ":" + string(CmdPort))
	if err != nil {
		log.Panicf("listen error:%s", err.Error())
	}
	go http.Serve(l, nil)
}

func (csrv *CmdServer) Sync(args *Args, reply *bool) error {
	csrv.sybx.CmdSync()

	*reply = true
	return nil
}

func NewCmdClient() *CmdClient {
	return &CmdClient{}
}

func (client *CmdClient) Sync() {
	rpcClient, err := rpc.DialHTTP("tcp", "127.0.0.1" + ":" + string(CmdPort))
	if err != nil {
		fmt.Printf("could not connect to the dxfuse server:", err.Error())
		os.Exit(1)
	}
	defer rpcClient.Close()

	// Synchronous call
	args := Args{a : 3}
	var reply bool
	err = rpcClient.Call("CmdServer.Sync", &args, &reply)
	if err != nil {
		fmt.Printf("sync error:", err.Error())
		os.Exit(1)
	}
}
