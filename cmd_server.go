/* Accept commands from the dxfuse_tools program. The only command
* right now is sync, but this is the place to implement additional
* ones to come in the future.
 */
package dxfuse

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"

	"golang.org/x/sync/semaphore"
)

const (
	// A port number for accepting commands
	CmdPort = 7205
)

type CmdServer struct {
	options Options
	sybx    *SyncDbDx
	inbound *net.TCPListener
}

// A separate structure used for exporting through RPC
type CmdServerBox struct {
	cmdSrv *CmdServer
}

func NewCmdServer(options Options, sybx *SyncDbDx) *CmdServer {
	cmdServer := &CmdServer{
		options: options,
		sybx:    sybx,
		inbound: nil,
	}
	return cmdServer
}

// write a log message, and add a header
func (cmdSrv *CmdServer) log(a string, args ...interface{}) {
	LogMsg("CmdServer", a, args...)
}

func (cmdSrv *CmdServer) Init() {
	addy, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", CmdPort))
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}
	cmdSrv.inbound = inbound

	cmdSrvBox := &CmdServerBox{
		cmdSrv: cmdSrv,
	}
	rpc.Register(cmdSrvBox)
	go rpc.Accept(inbound)

	cmdSrv.log("started command server, accepting external commands")
}

func (cmdSrv *CmdServer) Close() {
	cmdSrv.inbound.Close()
}

// Only allow one sync operation at a time
var sem = semaphore.NewWeighted(1)

// Note: all export functions from this module have to have this format.
// Nothing else will work with the RPC package.
func (box *CmdServerBox) GetLine(arg string, reply *bool) error {
	cmdSrv := box.cmdSrv
	cmdSrv.log("Received line %s", arg)
	switch arg {
	case "sync":
		// Error out if another sync operation has been run by the cmd client
		// https://stackoverflow.com/questions/45208536/good-way-to-return-on-locked-mutex-in-go
		if !sem.TryAcquire(1) {
			cmdSrv.log("Rejecting sync operation as another is already runnning")
			return errors.New("another sync operation is already running")
		}
		defer sem.Release(1)
		cmdSrv.sybx.CmdSync()
	default:
		cmdSrv.log("Unknown command")
	}

	*reply = true
	return nil
}
