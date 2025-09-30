package dxfuse

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dnanexus/dxda"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock types
type MockMetadataDb struct {
	MetadataDbInterface
	mock.Mock
}

func (m *MockMetadataDb) Init() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockMetadataDb) BeginTxn() (*sql.Tx, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sql.Tx), args.Error(1)
}

func (m *MockMetadataDb) Commit(oph *OpHandle) error {
	args := m.Called(oph)
	return args.Error(0)
}

func (m *MockMetadataDb) Rollback(oph *OpHandle) error {
	args := m.Called(oph)
	return args.Error(0)
}

func (m *MockMetadataDb) PopulateRoot(ctx context.Context, oph *OpHandle, manifest Manifest) error {
	args := m.Called(ctx, oph, manifest)
	return args.Error(0)
}

func (m *MockMetadataDb) Shutdown() {
	m.Called()
}

func (m *MockMetadataDb) LookupDirByInode(ctx context.Context, oph *OpHandle, inode int64) (Dir, bool, error) {
	args := m.Called(ctx, oph, inode)
	return args.Get(0).(Dir), args.Bool(1), args.Error(2)
}

func (m *MockMetadataDb) LookupInDir(ctx context.Context, oph *OpHandle, dir *Dir, name string) (Node, bool, error) {
	args := m.Called(ctx, oph, dir, name)
	if args.Get(0) == nil {
		return nil, args.Bool(1), args.Error(2)
	}
	return args.Get(0).(Node), args.Bool(1), args.Error(2)
}

func (m *MockMetadataDb) LookupByInode(ctx context.Context, oph *OpHandle, inode int64) (Node, bool, error) {
	args := m.Called(ctx, oph, inode)
	return args.Get(0).(Node), args.Bool(1), args.Error(2)
}

func (m *MockMetadataDb) CreateDir(ctx context.Context, oph *OpHandle, projId, projFolder string, ctime, mtime int64, mode os.FileMode, fullPath string) (int64, error) {
	args := m.Called(ctx, oph, projId, projFolder, ctime, mtime, mode, fullPath)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockMetadataDb) CreateFile(ctx context.Context, oph *OpHandle, parentDir *Dir, name string, mode os.FileMode, fileId string) (File, error) {
	args := m.Called(ctx, oph, parentDir, name, mode, fileId)
	return args.Get(0).(File), args.Error(1)
}

func (m *MockMetadataDb) ReadDirAll(ctx context.Context, oph *OpHandle, dir *Dir) (map[string]File, map[string]Dir, error) {
	args := m.Called(ctx, oph, dir)
	return args.Get(0).(map[string]File), args.Get(1).(map[string]Dir), args.Error(2)
}

func (m *MockMetadataDb) UpdateFileAttrs(ctx context.Context, oph *OpHandle, inode int64, size int64, mtime time.Time, mode *os.FileMode) error {
	args := m.Called(ctx, oph, inode, size, mtime, mode)
	return args.Error(0)
}

func (m *MockMetadataDb) UpdateInodeFileState(ctx context.Context, oph *OpHandle, inode int64, state string, uploading bool) error {
	args := m.Called(ctx, oph, inode, state, uploading)
	return args.Error(0)
}

func (m *MockMetadataDb) UpdateInodeFileId(ctx context.Context, oph *OpHandle, inode int64, fileId string) error {
	args := m.Called(ctx, oph, inode, fileId)
	return args.Error(0)
}

func (m *MockMetadataDb) RemoveFile(ctx context.Context, oph *OpHandle, inode int64) error {
	args := m.Called(ctx, oph, inode)
	return args.Error(0)
}

func (m *MockMetadataDb) RemoveEmptyDir(ctx context.Context, oph *OpHandle, inode int64) error {
	args := m.Called(ctx, oph, inode)
	return args.Error(0)
}

func (m *MockMetadataDb) UnlinkInode(ctx context.Context, oph *OpHandle, inode int64) error {
	args := m.Called(ctx, oph, inode)
	return args.Error(0)
}

func (m *MockMetadataDb) MoveFile(ctx context.Context, oph *OpHandle, inode int64, newParent Dir, newName string) error {
	args := m.Called(ctx, oph, inode, newParent, newName)
	return args.Error(0)
}

func (m *MockMetadataDb) MoveDir(ctx context.Context, oph *OpHandle, oldParent, newParent, dir Dir, newName string) error {
	args := m.Called(ctx, oph, oldParent, newParent, dir, newName)
	return args.Error(0)
}

func (m *MockMetadataDb) GetParentDirByInode(ctx context.Context, oph *OpHandle, inode int64) (Dir, error) {
	args := m.Called(ctx, oph, inode)
	return args.Get(0).(Dir), args.Error(1)
}

func (m *MockMetadataDb) UpdateFileTagsAndProperties(ctx context.Context, oph *OpHandle, file File) error {
	args := m.Called(ctx, oph, file)
	return args.Error(0)
}

type MockPrefetchGlobalState struct {
	PrefetchGlobalStateInterface
	mock.Mock
}

func (m *MockPrefetchGlobalState) Shutdown() {
	m.Called()
}

func (m *MockPrefetchGlobalState) CreateStreamEntry(hid fuseops.HandleID, file File, url DxDownloadURL) {
	m.Called(hid, file, url)
}

func (m *MockPrefetchGlobalState) RemoveStreamEntry(hid fuseops.HandleID) {
	m.Called(hid)
}

func (m *MockPrefetchGlobalState) CacheLookup(hid fuseops.HandleID, offset, endOfs int64, dst []byte) int {
	args := m.Called(hid, offset, endOfs, dst)
	return args.Int(0)
}

type MockFileUploader struct {
	FileUploaderInterface
	mock.Mock
}

func (m *MockFileUploader) Shutdown() {
	m.Called()
}

func (m *MockFileUploader) AllocateWriteBuffer(partId int, firstBuffer bool) []byte {
	args := m.Called(partId, firstBuffer)
	return args.Get(0).([]byte)
}

type MockDxOps struct {
	DxOpsInterface
	mock.Mock
}

func (m *MockDxOps) DxRemoveObjects(ctx context.Context, httpClient *http.Client, projId string, objects []string) error {
	args := m.Called(ctx, httpClient, projId, objects)
	return args.Error(0)
}

func (m *MockDxOps) DxFileNew(ctx context.Context, httpClient *http.Client, nonce, projId, fileName, projFolder string) (string, error) {
	args := m.Called(ctx, httpClient, nonce, projId, fileName, projFolder)
	return args.String(0), args.Error(1)
}

func (m *MockDxOps) DxFolderNew(ctx context.Context, httpClient *http.Client, projId, folderPath string) error {
	args := m.Called(ctx, httpClient, projId, folderPath)
	return args.Error(0)
}

func (m *MockDxOps) DxFolderRemove(ctx context.Context, httpClient *http.Client, projId, folderPath string) error {
	args := m.Called(ctx, httpClient, projId, folderPath)
	return args.Error(0)
}

func (m *MockDxOps) DxRename(ctx context.Context, httpClient *http.Client, projId, fileId, newName string) error {
	args := m.Called(ctx, httpClient, projId, fileId, newName)
	return args.Error(0)
}

func (m *MockDxOps) DxMove(ctx context.Context, httpClient *http.Client, projId string, objects, folders []string, destination string) error {
	args := m.Called(ctx, httpClient, projId, objects, folders, destination)
	return args.Error(0)
}

func (m *MockDxOps) DxRenameFolder(ctx context.Context, httpClient *http.Client, projId, folderPath, newName string) error {
	args := m.Called(ctx, httpClient, projId, folderPath, newName)
	return args.Error(0)
}

func (m *MockDxOps) DxFileCloseAndWait(ctx context.Context, httpClient *http.Client, projId, fileId string) error {
	args := m.Called(ctx, httpClient, projId, fileId)
	return args.Error(0)
}

func (m *MockDxOps) DxFileUploadPart(ctx context.Context, httpClient *http.Client, fileId string, partId int, data []byte) error {
	args := m.Called(ctx, httpClient, fileId, partId, data)
	return args.Error(0)
}

func (m *MockDxOps) DxAddTags(ctx context.Context, httpClient *http.Client, projId, fileId string, tags []string) error {
	args := m.Called(ctx, httpClient, projId, fileId, tags)
	return args.Error(0)
}

func (m *MockDxOps) DxRemoveTags(ctx context.Context, httpClient *http.Client, projId, fileId string, tags []string) error {
	args := m.Called(ctx, httpClient, projId, fileId, tags)
	return args.Error(0)
}

func (m *MockDxOps) DxSetProperties(ctx context.Context, httpClient *http.Client, projId, fileId string, props map[string]*string) error {
	args := m.Called(ctx, httpClient, projId, fileId, props)
	return args.Error(0)
}

func (m *MockDxOps) DxAPI(ctx context.Context, httpClient *http.Client, retries int, dxEnv *dxda.DXEnvironment, endpoint string, payload string) ([]byte, error) {
	args := m.Called(ctx, httpClient, retries, dxEnv, endpoint, payload)
	return args.Get(0).([]byte), args.Error(1)
}

// Helper functions

func createMockFilesys(mode string) *Filesys {
	httpClientPool := make(chan *http.Client, 1)
	httpClientPool <- &http.Client{}

	return &Filesys{
		dxEnv: dxda.DXEnvironment{},
		options: Options{
			Mode:         mode,
			Verbose:      false,
			VerboseLevel: 0,
			Uid:          1000,
			Gid:          1000,
		},
		mutex:          &sync.Mutex{},
		httpClientPool: httpClientPool,
		fhCounter:      1,
		fhTable:        make(map[fuseops.HandleID]*FileHandle),
		dhCounter:      1,
		dhTable:        make(map[fuseops.HandleID]*DirHandle),
		tmpFileCounter: 0,
		shutdownCalled: false,
		projId2Desc: map[string]DxProjectDescription{
			"project-123": {
				Id:    "project-123",
				Level: PERM_CONTRIBUTE,
			},
		},
	}
}

func createMockSqlTx() *sql.Tx {
	// Return nil since our mocks don't actually use the transaction
	return nil
}

func createTestFile() File {
	return File{
		Inode:         1001,
		Name:          "test.txt",
		State:         "closed",
		ArchivalState: "live",
		Id:            "file-123",
		ProjId:        "project-123",
		Size:          1024,
		Kind:          FK_Regular,
		Mode:          0644,
		Tags:          []string{"tag1", "tag2"},
		Properties:    map[string]string{"prop1": "value1"},
	}
}

func createTestDir() Dir {
	return Dir{
		Inode:      1002,
		Dname:      "testdir",
		ProjId:     "project-123",
		ProjFolder: "/testdir",
		FullPath:   "/project-123/testdir",
		faux:       false,
	}
}

// Test functions
func TestNewDxfuse(t *testing.T) {
	dxEnv := dxda.DXEnvironment{}
	manifest := Manifest{
		Directories: []ManifestDir{
			{ProjId: "project-123", Folder: "/"},
		},
		Files: []ManifestFile{
			{ProjId: "project-123", Fname: "test.txt", Parent: "/", FileId: "file-123"},
		},
	}
	options := Options{
		Mode:        ReadOnly,
		StateFolder: "/tmp/test",
	}

	// This would require significant setup to actually work
	// In a real test environment, you'd need proper database setup
	_, err := NewDxfuse(dxEnv, manifest, options)
	// We expect this to fail due to database setup, but it tests the function signature
	assert.Error(t, err)
}

func TestFilesys_LookUpInode(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		setup     func(*MockMetadataDb)
		parentId  fuseops.InodeID
		childName string
		expectErr error
	}{
		{
			name: "successful lookup in readonly mode",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(createTestDir(), true, nil)
				mdb.On("LookupInDir", mock.Anything, mock.Anything, mock.Anything, "test.txt").
					Return(createTestFile(), true, nil)
			},
			parentId:  1,
			childName: "test.txt",
			expectErr: nil,
		},
		{
			name: "parent directory not found",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)
				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(Dir{}, false, nil)
			},
			parentId:  1,
			childName: "test.txt",
			expectErr: fuse.ENOENT,
		},
		{
			name: "child file not found",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(createTestDir(), true, nil)
				mdb.On("LookupInDir", mock.Anything, mock.Anything, mock.Anything, "nonexistent.txt").
					Return(nil, false, nil)
			},
			parentId:  1,
			childName: "nonexistent.txt",
			expectErr: fuse.ENOENT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := createMockFilesys(tt.mode)
			mockMdb := &MockMetadataDb{}
			fsys.mdb = mockMdb

			tt.setup(mockMdb)

			op := &fuseops.LookUpInodeOp{
				Parent: tt.parentId,
				Name:   tt.childName,
			}

			err := fsys.LookUpInode(context.Background(), op)

			if tt.expectErr != nil {
				assert.Equal(t, tt.expectErr, err)
			} else {
				assert.NoError(t, err)
				assert.NotEqual(t, fuseops.InodeID(0), op.Entry.Child)
			}

			mockMdb.AssertExpectations(t)
		})
	}
}

func TestFilesys_CreateFile(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		setup     func(*MockMetadataDb, *MockDxOps)
		fileName  string
		expectErr error
	}{
		{
			name: "create file in readwrite mode",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)
				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(createTestDir(), true, nil)
				mdb.On("LookupInDir", mock.Anything, mock.Anything, mock.Anything, "new.txt").
					Return(nil, false, nil)
				ops.On("DxFileNew", mock.Anything, mock.Anything, mock.Anything, "project-123", "new.txt", "/testdir").
					Return("file-456", nil)
				mdb.On("CreateFile", mock.Anything, mock.Anything, mock.Anything, "new.txt", mock.Anything, "file-456").
					Return(File{Inode: 1003, Name: "new.txt", Id: "file-456"}, nil)
			},
			fileName:  "new.txt",
			expectErr: nil,
		},
		{
			name: "create file in readonly mode should fail",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)
				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(createTestDir(), true, nil)
				mdb.On("LookupInDir", mock.Anything, mock.Anything, mock.Anything, "new.txt").
					Return(nil, false, nil)
			},
			fileName:  "new.txt",
			expectErr: syscall.EPERM,
		},
		{
			name: "file already exists",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(createTestDir(), true, nil)
				mdb.On("LookupInDir", mock.Anything, mock.Anything, mock.Anything, "existing.txt").
					Return(createTestFile(), true, nil)
			},
			fileName:  "existing.txt",
			expectErr: fuse.EEXIST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := createMockFilesys(tt.mode)
			mockMdb := &MockMetadataDb{}
			mockOps := &MockDxOps{}
			fsys.mdb = mockMdb
			fsys.ops = mockOps

			tt.setup(mockMdb, mockOps)

			op := &fuseops.CreateFileOp{
				Parent:    1,
				Name:      tt.fileName,
				Mode:      0644,
				OpContext: fuseops.OpContext{Pid: 12345},
			}

			err := fsys.CreateFile(context.Background(), op)

			if tt.expectErr != nil {
				assert.Equal(t, tt.expectErr, err)
			} else {
				assert.NoError(t, err)
				assert.NotEqual(t, fuseops.HandleID(0), op.Handle)
			}

			mockMdb.AssertExpectations(t)
			mockOps.AssertExpectations(t)
		})
	}
}

func TestFilesys_OpenFile(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		setup     func(*MockMetadataDb, *MockDxOps, *MockPrefetchGlobalState)
		openFlags uint32
		expectErr error
		expectAM  int
	}{
		{
			name: "open for read in readonly mode",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps, pgs *MockPrefetchGlobalState) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)
				file := createTestFile()
				ops.On("DxAPI", mock.Anything, mock.Anything, mock.Anything, mock.Anything, "file-123/download", mock.Anything).
					Return([]byte(`{"url": "http://example.com/download"}`), nil)
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
				pgs.On("CreateStreamEntry", mock.Anything, mock.Anything, mock.Anything)
			},
			openFlags: uint32(syscall.O_RDONLY),
			expectErr: nil,
			expectAM:  AM_RO_Remote,
		},
		{
			name: "open for write with truncate in allow overwrite mode",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps, pgs *MockPrefetchGlobalState) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
				mdb.On("GetParentDirByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(createTestDir(), nil)
				ops.On("DxRemoveObjects", mock.Anything, mock.Anything, "project-123", []string{"file-123"}).
					Return(nil)
				ops.On("DxFileNew", mock.Anything, mock.Anything, mock.Anything, "project-123", "test.txt", "/testdir").
					Return("file-456", nil)
				mdb.On("UpdateInodeFileId", mock.Anything, mock.Anything, int64(1001), "file-456").
					Return(nil)
				mdb.On("UpdateInodeFileState", mock.Anything, mock.Anything, int64(1001), "open", true).
					Return(nil)
				mdb.On("UpdateFileAttrs", mock.Anything, mock.Anything, int64(1001), int64(0), mock.Anything, mock.Anything).
					Return(nil)
			},
			openFlags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			expectErr: nil,
			expectAM:  AM_AO_Remote,
		},
		{
			name: "open for write without truncate should be readonly",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps, pgs *MockPrefetchGlobalState) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)
				ops.On("DxAPI", mock.Anything, mock.Anything, mock.Anything, mock.Anything, "file-123/download", mock.Anything).
					Return([]byte(`{"url": "http://example.com/download"}`), nil)
				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
				pgs.On("CreateStreamEntry", mock.Anything, mock.Anything, mock.Anything)
			},
			openFlags: uint32(syscall.O_WRONLY),
			expectErr: nil,
			expectAM:  AM_RO_Remote,
		},
		{
			name: "open for write in readonly mode should fail",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps, pgs *MockPrefetchGlobalState) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)
				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
			},
			openFlags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			expectErr: syscall.EACCES,
		},
		{
			name: "open non-closed file should fail",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps, pgs *MockPrefetchGlobalState) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)
				file := createTestFile()
				file.State = "open"
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
			},
			openFlags: uint32(syscall.O_RDONLY),
			expectErr: syscall.EACCES,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := createMockFilesys(tt.mode)
			mockMdb := &MockMetadataDb{}
			mockOps := &MockDxOps{}
			mockPgs := &MockPrefetchGlobalState{}
			fsys.mdb = mockMdb
			fsys.ops = mockOps
			fsys.pgs = mockPgs

			tt.setup(mockMdb, mockOps, mockPgs)

			op := &fuseops.OpenFileOp{
				Inode:     1001,
				OpContext: fuseops.OpContext{Pid: 12345},
			}

			// Use reflection to set the OpenFlags field with the correct type
			openFlagsValue := reflect.ValueOf(tt.openFlags).Convert(reflect.TypeOf(op.OpenFlags))
			reflect.ValueOf(op).Elem().FieldByName("OpenFlags").Set(openFlagsValue)

			err := fsys.OpenFile(context.Background(), op)

			if tt.expectErr != nil {
				assert.Equal(t, tt.expectErr, err)
			} else {
				assert.NoError(t, err)
				assert.NotEqual(t, fuseops.HandleID(0), op.Handle)

				// Check the file handle access mode
				fh, exists := fsys.fhTable[op.Handle]
				assert.True(t, exists)
				assert.Equal(t, tt.expectAM, fh.accessMode)
			}

			mockMdb.AssertExpectations(t)
			mockOps.AssertExpectations(t)
			mockPgs.AssertExpectations(t)
		})
	}
}

func TestFilesys_SetInodeAttributes(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		setup     func(*MockMetadataDb)
		size      *uint64
		expectErr error
	}{
		{
			name: "set mode and mtime in allow overwrite mode",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
				mdb.On("UpdateFileAttrs", mock.Anything, mock.Anything, int64(1001), int64(1024), mock.Anything, mock.Anything).
					Return(nil)
			},
			size:      nil,
			expectErr: nil,
		},
		{
			name: "ftruncate should fail",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
			},
			size:      func() *uint64 { s := uint64(512); return &s }(),
			expectErr: syscall.ENOTSUP,
		},
		{
			name: "set attributes on directory should fail",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				dir := createTestDir()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1002)).
					Return(dir, true, nil)
			},
			size:      nil,
			expectErr: syscall.EPERM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := createMockFilesys(tt.mode)
			mockMdb := &MockMetadataDb{}
			fsys.mdb = mockMdb

			tt.setup(mockMdb)

			mode := os.FileMode(0755)
			mtime := time.Now()
			op := &fuseops.SetInodeAttributesOp{
				Inode: 1001,
				Mode:  &mode,
				Mtime: &mtime,
				Size:  tt.size,
			}

			if tt.name == "set attributes on directory should fail" {
				op.Inode = 1002
			}

			err := fsys.SetInodeAttributes(context.Background(), op)

			assert.Equal(t, tt.expectErr, err)

			mockMdb.AssertExpectations(t)
		})
	}
}

func TestFilesys_MkDir(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		setup     func(*MockMetadataDb, *MockDxOps)
		expectErr error
	}{
		{
			name: "create directory in readwrite mode",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(createTestDir(), true, nil)
				mdb.On("LookupInDir", mock.Anything, mock.Anything, mock.Anything, "newdir").
					Return(nil, false, nil)
				ops.On("DxFolderNew", mock.Anything, mock.Anything, "project-123", "/testdir/newdir").
					Return(nil)
				mdb.On("CreateDir", mock.Anything, mock.Anything, "project-123", "/testdir/newdir", mock.Anything, mock.Anything, mock.Anything, "/project-123/testdir/newdir").
					Return(int64(1003), nil)
			},
			expectErr: nil,
		},
		{
			name: "create directory in readonly mode should fail",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				mdb.On("LookupDirByInode", mock.Anything, mock.Anything, int64(1)).
					Return(createTestDir(), true, nil)
				mdb.On("LookupInDir", mock.Anything, mock.Anything, mock.Anything, "newdir").
					Return(nil, false, nil)
			},
			expectErr: syscall.EPERM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := createMockFilesys(tt.mode)
			mockMdb := &MockMetadataDb{}
			mockOps := &MockDxOps{}
			fsys.mdb = mockMdb
			fsys.ops = mockOps

			tt.setup(mockMdb, mockOps)

			op := &fuseops.MkDirOp{
				Parent: 1,
				Name:   "newdir",
				Mode:   0755,
			}

			err := fsys.MkDir(context.Background(), op)

			if tt.expectErr != nil {
				assert.Equal(t, tt.expectErr, err)
			} else {
				assert.NoError(t, err)
			}

			mockMdb.AssertExpectations(t)
			mockOps.AssertExpectations(t)
		})
	}
}

func TestFilesys_CheckProjectPermissions(t *testing.T) {
	tests := []struct {
		name         string
		mode         string
		projLevel    int
		requiredPerm int
		expected     bool
	}{
		{
			name:         "readonly mode with view permission",
			mode:         ReadOnly,
			projLevel:    PERM_VIEW,
			requiredPerm: PERM_VIEW,
			expected:     true,
		},
		{
			name:         "readonly mode with contribute permission should fail",
			mode:         ReadOnly,
			projLevel:    PERM_CONTRIBUTE,
			requiredPerm: PERM_CONTRIBUTE,
			expected:     false,
		},
		{
			name:         "readwrite mode with sufficient permissions",
			mode:         AllowOverwrite,
			projLevel:    PERM_CONTRIBUTE,
			requiredPerm: PERM_CONTRIBUTE,
			expected:     true,
		},
		{
			name:         "readwrite mode with insufficient permissions",
			mode:         AllowOverwrite,
			projLevel:    PERM_VIEW,
			requiredPerm: PERM_CONTRIBUTE,
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := createMockFilesys(tt.mode)
			fsys.projId2Desc["project-123"] = DxProjectDescription{
				Id:    "project-123",
				Level: tt.projLevel,
			}

			result := fsys.checkProjectPermissions("project-123", tt.requiredPerm)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilesys_Shutdown(t *testing.T) {
	fsys := createMockFilesys(ReadOnly)

	mockMdb := &MockMetadataDb{}
	mockPgs := &MockPrefetchGlobalState{}
	mockUploader := &MockFileUploader{}

	fsys.mdb = mockMdb
	fsys.pgs = mockPgs
	fsys.uploader = mockUploader

	mockMdb.On("Shutdown").Return()
	mockPgs.On("Shutdown").Return()
	mockUploader.On("Shutdown").Return()

	// First shutdown call
	fsys.Shutdown()
	assert.True(t, fsys.shutdownCalled)

	// Second shutdown call should not call mocks again
	fsys.Shutdown()

	mockMdb.AssertExpectations(t)
	mockPgs.AssertExpectations(t)
	mockUploader.AssertExpectations(t)
}

func TestFilesys_TranslateError(t *testing.T) {
	fsys := createMockFilesys(ReadOnly)

	tests := []struct {
		name     string
		input    error
		expected error
	}{
		{
			name: "DxError InvalidInput",
			input: &dxda.DxError{
				EType:   "InvalidInput",
				Message: "Invalid input provided",
			},
			expected: fuse.EINVAL,
		},
		{
			name: "DxError PermissionDenied",
			input: &dxda.DxError{
				EType:   "PermissionDenied",
				Message: "Permission denied",
			},
			expected: syscall.EPERM,
		},
		{
			name: "DxError ResourceNotFound",
			input: &dxda.DxError{
				EType:   "ResourceNotFound",
				Message: "Resource not found",
			},
			expected: fuse.ENOENT,
		},
		{
			name: "DxError Unauthorized",
			input: &dxda.DxError{
				EType:   "Unauthorized",
				Message: "Unauthorized access",
			},
			expected: syscall.EPERM,
		},
		{
			name: "DxError Unknown",
			input: &dxda.DxError{
				EType:   "UnknownError",
				Message: "Unknown error",
			},
			expected: fuse.EIO,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fsys.translateError(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Additional tests for XAttr functionality
func TestFilesys_SetXattr(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		setup     func(*MockMetadataDb, *MockDxOps)
		attrName  string
		attrValue []byte
		flags     uint32
		expectErr error
	}{
		{
			name: "set tag in readwrite mode",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
				ops.On("DxAddTags", mock.Anything, mock.Anything, "project-123", "file-123", []string{"newtag"}).
					Return(nil)
				mdb.On("UpdateFileTagsAndProperties", mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
			},
			attrName:  "tag.newtag",
			attrValue: []byte(""),
			flags:     0,
			expectErr: nil,
		},
		{
			name: "set property in readwrite mode",
			mode: AllowOverwrite,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
				ops.On("DxSetProperties", mock.Anything, mock.Anything, "project-123", "file-123", mock.Anything).
					Return(nil)
				mdb.On("UpdateFileTagsAndProperties", mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
			},
			attrName:  "prop.newprop",
			attrValue: []byte("newvalue"),
			flags:     0,
			expectErr: nil,
		},
		{
			name: "set xattr in readonly mode should fail",
			mode: ReadOnly,
			setup: func(mdb *MockMetadataDb, ops *MockDxOps) {
				mdb.On("BeginTxn").Return(createMockSqlTx(), nil)
				mdb.On("Commit", mock.Anything).Return(nil)

				file := createTestFile()
				mdb.On("LookupByInode", mock.Anything, mock.Anything, int64(1001)).
					Return(file, true, nil)
			},
			attrName:  "tag.newtag",
			attrValue: []byte(""),
			flags:     0,
			expectErr: syscall.EPERM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := createMockFilesys(tt.mode)
			mockMdb := &MockMetadataDb{}
			mockOps := &MockDxOps{}
			fsys.mdb = mockMdb
			fsys.ops = mockOps

			tt.setup(mockMdb, mockOps)

			op := &fuseops.SetXattrOp{
				Inode: 1001,
				Name:  tt.attrName,
				Value: tt.attrValue,
				Flags: tt.flags,
			}

			err := fsys.SetXattr(context.Background(), op)

			assert.Equal(t, tt.expectErr, err)

			mockMdb.AssertExpectations(t)
			mockOps.AssertExpectations(t)
		})
	}
}

// Test for directory operations
func TestFilesys_ReadDir(t *testing.T) {
	fsys := createMockFilesys(ReadOnly)

	// Create a directory handle
	testDir := createTestDir()
	dh := &DirHandle{
		d: testDir,
		entries: []fuseutil.Dirent{
			{Offset: 1, Inode: 1001, Name: "file1.txt", Type: fuseutil.DT_File},
			{Offset: 2, Inode: 1002, Name: "subdir", Type: fuseutil.DT_Directory},
		},
	}

	handleID := fsys.insertIntoDirHandleTable(dh)

	op := &fuseops.ReadDirOp{
		Handle: handleID,
		Offset: 0,
		Dst:    make([]byte, 1024),
	}

	err := fsys.ReadDir(context.Background(), op)

	assert.NoError(t, err)
	assert.True(t, op.BytesRead > 0)
}
