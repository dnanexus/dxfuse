package dxfuse

import (
	"fmt"
	"net/rpc"
	"os"
)

type CmdClient struct {
}

// Sending commands with a client
//
func NewCmdClient() *CmdClient {
	return &CmdClient{}
}

func (client *CmdClient) Sync() {
	rpcClient, err := rpc.Dial("tcp", fmt.Sprintf(":%d", CmdPort))
	if err != nil {
		fmt.Printf("could not connect to the dxfuse server: %s", err.Error())
		os.Exit(1)
	}
	defer rpcClient.Close()

	// Synchronous call
	var reply bool
	err = rpcClient.Call("CmdServerBox.GetLine", "sync", &reply)
	if err != nil {
		fmt.Printf("sync error: %s\n", err.Error())
		os.Exit(1)
	}
}
