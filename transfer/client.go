package transfer

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/smallnest/rpcx"
)

const (
	BLOCK_SIZE = 512 * 1024
)

// Client struct
type Client struct {
	Addr       string
	rpcxClient *rpcx.Client
}

// NewClient func
func NewClient(addr string) *Client {
	return &Client{Addr: addr}
}

// Dial func
func (c *Client) Dial() error {
	s := &rpcx.DirectClientSelector{Network: "tcp", Address: c.Addr, DialTimeout: 10 * time.Second}
	client := rpcx.NewClient(s)
	//client, err := rpc.DialHTTP("tcp", c.Addr)
	// if err != nil {
	// 	return err
	// }
	c.rpcxClient = client

	return nil
}

// Close func
func (c *Client) Close() error {
	return c.rpcxClient.Close()
}

// UploadFinish func
func (c *Client) UploadFinish(JsonFile, stormFile string) error {
	var res Response
	if err := c.rpcxClient.Call(context.Background(), "FileSession.UploadFinish", UploadFiles{StormFile: stormFile, JsonFile: JsonFile}, &res); err != nil {
		return err
	}
	return nil
}

// TopologyName func
func (c *Client) TopologyName(topology string) error {
	var res Response
	if err := c.rpcxClient.Call(context.Background(), "FileSession.TopologyName", FileRequest{Filename: topology}, &res); err != nil {
		return err
	}
	return nil
}

// ServerOpen func
func (c *Client) ServerOpen(filename string) (SessionId, error) {
	var res Response
	if err := c.rpcxClient.Call(context.Background(), "FileSession.Open", FileRequest{Filename: filename}, &res); err != nil {
		return 0, err
	}

	return res.Id, nil
}

// ServerStat func
func (c *Client) ServerStat(filename string) (*StatResponse, error) {
	var res StatResponse
	if err := c.rpcxClient.Call(context.Background(), "FileSession.Stat", FileRequest{Filename: filename}, &res); err != nil {
		fmt.Println("enter err", err)
		return nil, err
	}
	return &res, nil
}

// Stat func
func (c *Client) Stat(filename string) (*StatResponse, error) {

	var res StatResponse
	//	path := filepath.Join(r.server.ReadDirectory, req.Filename)
	fi, err := os.Stat(filename)
	if os.IsNotExist(err) {
		fmt.Println("--------stat err", err)
		return nil, err
	}

	if fi.IsDir() {
		res.Type = "Directory"
	} else {
		res.Type = "File"
		res.Size = fi.Size()
	}
	res.LastModified = fi.ModTime()

	return &res, nil
}

// GetBlock func
func (c *Client) GetBlock(sessionID SessionId, blockID int) ([]byte, error) {
	return c.ServerReadAt(sessionID, int64(blockID)*BLOCK_SIZE, BLOCK_SIZE)
}

// ServerReadAt func
func (c *Client) ServerReadAt(sessionId SessionId, offset int64, size int) ([]byte, error) {
	res := &ReadResponse{Data: make([]byte, size)}
	err := c.rpcxClient.Call(context.Background(), "FileSession.ReadAt", ReadRequest{Id: sessionId, Size: size, Offset: offset}, &res)

	if res.EOF {
		err = io.EOF
	}

	if size != res.Size {
		return res.Data[:res.Size], err
	}

	return res.Data, nil
}

// ServerWriteAt func
func (c *Client) ServerWriteAt(req ReadRequest) error {
	var res Response
	if err := c.rpcxClient.Call(context.Background(), "FileSession.WriteAt", req, &res); err != nil {
		return err
	}
	return nil
}

// ServerCreate func
func (c *Client) ServerCreate(filename string) (SessionId, error) {
	var res Response
	if err := c.rpcxClient.Call(context.Background(), "FileSession.Create", FileRequest{Filename: filename}, &res); err != nil {
		return 0, err
	}

	return res.Id, nil
}

// Read func
func (c *Client) Read(sessionId SessionId, buf []byte) (int, error) {
	res := &ReadResponse{Data: buf}
	if err := c.rpcxClient.Call(context.Background(), "FileSession.Read", ReadRequest{Id: sessionId, Size: cap(buf)}, &res); err != nil {
		return 0, err
	}

	return res.Size, nil
}

// CloseSession func
func (c *Client) CloseSession(sessionId SessionId) error {
	res := &Response{}
	if err := c.rpcxClient.Call(context.Background(), "FileSession.Close", Request{Id: sessionId}, &res); err != nil {
		return err
	}

	return nil
}

// Download func
func (c *Client) Download(filename, saveFile string) error {
	return c.DownloadAt(filename, saveFile, 0)
}

// Upload func
func (c *Client) Upload(filename string, serverFile string) error {
	stat, err := c.Stat(filename)
	if err != nil {
		fmt.Println("stat error", err)
		return err
	}

	if stat.IsDir() {
		return fmt.Errorf("%s is directory", filename)
	}

	blocks := int(stat.Size / BLOCK_SIZE)
	if stat.Size%BLOCK_SIZE != 0 {
		blocks++
	}

	sessionID, err := c.ServerCreate(serverFile)
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("%s file open error", filename)
	}
	for i := 0; i < blocks; i++ {
		data := make([]byte, BLOCK_SIZE)
		n, err := file.ReadAt(data, int64(i)*BLOCK_SIZE)

		if err != nil && err != io.EOF {
			fmt.Println("error is:", err)
			return err
		}
		req := new(ReadRequest)
		req.Offset = int64(i) * BLOCK_SIZE
		req.Size = n
		req.Data = data[:n]
		req.Id = sessionID
		if err == io.EOF {
			req.EOF = true
		}

		e := c.ServerWriteAt(*req)
		if e != nil {
			return e
		}
	}
	return nil
}

// DownloadAt func
func (c *Client) DownloadAt(filename, saveFile string, blockID int) error {
	fmt.Println(filename, saveFile)
	fmt.Println(c)
	stat, err := c.ServerStat(filename)
	if err != nil {
		fmt.Println("error", filename, err)
		return err
	}
	fmt.Println("enter", stat)
	if stat.IsDir() {
		return fmt.Errorf("%s is directory", filename)
	}
	blocks := int(stat.Size / BLOCK_SIZE)
	if stat.Size%BLOCK_SIZE != 0 {
		blocks++
	}
	file, err := os.OpenFile(saveFile, os.O_CREATE|os.O_WRONLY, 0744)
	if err != nil {
		fmt.Println("enter", err)
		return err
	}
	defer file.Close()
	sessionID, err := c.ServerOpen(filename)
	if err != nil {
		fmt.Println(err)
		return err
	}

	for i := blockID; i < blocks; i++ {
		buf, rerr := c.GetBlock(sessionID, i)
		if rerr != nil && rerr != io.EOF {
			return rerr
		}
		if _, werr := file.WriteAt(buf, int64(i)*BLOCK_SIZE); werr != nil {
			return werr
		}

		if i%((blocks-blockID)/100+1) == 0 {
			log.Printf("Downloading %s [%d/%d] blocks", filename, i-blockID+1, blocks-blockID)
		}

		if rerr == io.EOF {
			break
		}
	}

	c.CloseSession(sessionID)

	return nil
}
