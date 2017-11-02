package transfer

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/gstormlee/gstorm/nimbus/distribute"
)

// Request struct
type Request struct {
	Id SessionId
}

type UploadFiles struct {
	JsonFile  string
	StormFile string
	name      string
}

// FileRequest struct
type FileRequest struct {
	Filename string
}

// GetRequest struct
type GetRequest struct {
	Id      SessionId
	BlockId int
}

// GetResponse struct
type GetResponse struct {
	Id      SessionId
	BlockId int
	Size    int64
	Data    []byte
}

// ReadRequest struct
type ReadRequest struct {
	Id     SessionId
	Offset int64
	Size   int
	Data   []byte
	EOF    bool
}

// ReadResponse struct
type ReadResponse struct {
	Size int
	Data []byte
	EOF  bool
}

// StatResponse struct
type StatResponse struct {
	Type         string
	Size         int64
	LastModified time.Time
}

// IsDir func
func (r *StatResponse) IsDir() bool {
	return r.Type == "Directory"
}

// Response struct
type Response struct {
	Id     SessionId
	Result bool
}

// FileSession struct
type FileSession struct {
	server  *Server
	session *Session
}

func (f *FileSession) UploadFinish(upload UploadFiles, res *Response) error {
	JsonFile := upload.JsonFile
	stormFile := upload.StormFile
	Json := path.Join(f.server.TopologyDir, JsonFile)
	storm := path.Join(f.server.TopologyDir, stormFile)
	distribute.ReadJSON(Json, storm)
	return nil
}

func (f *FileSession) TopologyName(req FileRequest, res *Response) error {
	path := filepath.Join(f.server.ReadDirectory, req.Filename)
	fmt.Println("dir", path)
	err := os.MkdirAll(path, 0777)
	if err == nil {
		f.server.TopologyDir = path
		return nil
	}
	return err
}

// Open func
func (f *FileSession) Open(req FileRequest, res *Response) error {
	//path := filepath.Join(f.server.TopologyDir, req.Filename)
	file, err := os.Open(req.Filename)
	if err != nil {
		return err
	}

	res.Id = f.session.Add(file)
	res.Result = true

	//log.Printf("Open %s, sessionId=%d", req.Filename, res.Id)

	return nil
}

// create func
func (f *FileSession) create(req FileRequest, res *Response) error {
	file, err := os.OpenFile(req.Filename, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	res.Id = f.session.Add(file)
	res.Result = true
	return nil
}

// Close func
func (f *FileSession) Close(req Request, res *Response) error {
	f.session.Delete(req.Id)
	res.Result = true

	log.Printf("Close sessionId=%d", req.Id)

	return nil
}

// Stat func
func (f *FileSession) Stat(req FileRequest, res *StatResponse) error {
	fi, err := os.Stat(req.Filename)
	if os.IsNotExist(err) {

		return err
	}

	if fi.IsDir() {
		res.Type = "Directory"
	} else {
		res.Type = "File"
		res.Size = fi.Size()
	}
	res.LastModified = fi.ModTime()

	log.Printf("Stat %s, %#v", req.Filename, res)

	return nil
}

// WriteAt func
func (f *FileSession) WriteAt(req ReadRequest, res *ReadResponse) error {
	file := f.session.Get(req.Id)
	if file == nil {
		return errors.New("You must call open first.")
	}

	n, err := file.WriteAt(req.Data, req.Offset)
	fmt.Println(n, err)
	if err != nil {
		fmt.Println(err)
		return err
	}

	if n != req.Size {
		log.Printf("write error")
	}
	return nil
}

// ReadAt func
func (f *FileSession) ReadAt(req ReadRequest, res *ReadResponse) error {
	file := f.session.Get(req.Id)
	if file == nil {
		return errors.New("You must call open first.")
	}

	res.Data = make([]byte, req.Size)
	n, err := file.ReadAt(res.Data, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}

	if err == io.EOF {
		res.EOF = true
	}

	res.Size = n
	res.Data = res.Data[:n]

	return nil
}

// Read func
func (f *FileSession) Read(req ReadRequest, res *ReadResponse) error {
	file := f.session.Get(req.Id)
	if file == nil {
		return errors.New("You must call open first.")
	}

	res.Data = make([]byte, req.Size)
	n, err := file.Read(res.Data)
	if err != nil && err != io.EOF {
		return err
	}

	if err == io.EOF {
		res.EOF = true
	}

	res.Size = n
	res.Data = res.Data[:res.Size]

	return nil
}

func (f *FileSession) Create(req FileRequest, res *Response) error {
	path := filepath.Join(f.server.TopologyDir, req.Filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("enter", err)
		return err
	}
	res.Id = f.session.Add(file)
	res.Result = true

	return nil
}
