/* Accept commands from the dxfuse_tools program. The only command
* right now is sync, but this is the place to implement additional
* ones to come in the future.
*/
package dxfuse

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

const (
	// A port number for accepting commands
	CmdPort = 7205
)

type CmdServer struct {
	options  Options
	sybx    *SyncDbDx
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

func InitCmdServer(csrv *CmdServer) {
	addy, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", CmdPort))
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}

	rpc.Register(csrv)
	go rpc.Accept(inbound)

	csrv.log("started command server, accepting external commands")
}

// Note: all export functions from this module have to have this format.
// Nothing else will work with the RPC package.
func (csrv *CmdServer) GetLine(arg string, reply *bool) error {
	csrv.log("Received line %s", arg)
	switch arg {
	case "sync":
		csrv.sybx.CmdSync()
	default:
		csrv.log("Unknown command")
	}

	*reply = true
	return nil
}
