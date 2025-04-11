package main

import (
    "context"
    "log"
    "net"
    "sync"
    "time"
    pb "dfs-project/internal/grpc"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
)

type FileRecord struct {
    FileName    string
    DataKeeper  string // IP:Port of data keeper
    FilePath    string
    IsAlive     bool
    ReplicaList []string // List of Data Keepers with replicas
}

type server struct {
    pb.UnimplementedMasterTrackerServer
    mu             sync.RWMutex
    fileTable      map[string]*FileRecord
    dataKeepers    map[string]*DataKeeperInfo
    nextUploadPort int // For round-robin assignment
}

type DataKeeperInfo struct {
    Address    string // IP:Port
    IsAlive    bool
    LastSeen   time.Time
    DataPort   string // Port for file transfer over TCP
    ReplicaCount int  // Number of files replicated to this node
}

func NewServer() *server {
    return &server{
        fileTable:      make(map[string]*FileRecord),
        dataKeepers:    make(map[string]*DataKeeperInfo),
        nextUploadPort: 0,
    }
}

// RegisterDataKeeper registers a new data keeper or updates an existing one
func (s *server) RegisterDataKeeper(ctx context.Context, req *pb.RegisterDataKeeperRequest) (*pb.RegisterDataKeeperResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    address := req.Address
    dataPort := req.DataPort
    
    s.dataKeepers[address] = &DataKeeperInfo{
        Address:    address,
        IsAlive:    true,
        LastSeen:   time.Now(),
        DataPort:   dataPort,
        ReplicaCount: 0,
    }
    
    log.Printf("Data Keeper registered: %s with data port %s", address, dataPort)
    return &pb.RegisterDataKeeperResponse{Success: true}, nil
}

// Heartbeat updates the alive status of a data keeper
func (s *server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    address := req.Address
    
    if keeper, exists := s.dataKeepers[address]; exists {
        keeper.IsAlive = true
        keeper.LastSeen = time.Now()
        return &pb.HeartbeatResponse{Success: true}, nil
    }
    
    return &pb.HeartbeatResponse{Success: false}, nil
}

// GetUploadDataKeeper returns a data keeper for file upload
func (s *server) GetUploadDataKeeper(ctx context.Context, req *pb.GetUploadDataKeeperRequest) (*pb.GetUploadDataKeeperResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    // Find an alive data keeper with least files (simple load balancing)
    var selectedKeeper *DataKeeperInfo
    minReplicas := 999999
    
    for _, keeper := range s.dataKeepers {
        if keeper.IsAlive && keeper.ReplicaCount < minReplicas {
            selectedKeeper = keeper
            minReplicas = keeper.ReplicaCount
        }
    }
    
    if selectedKeeper == nil {
        return nil, grpc.Errorf(codes.Unavailable, "No available data keepers")
    }
    
    return &pb.GetUploadDataKeeperResponse{
        Address: selectedKeeper.Address,
        DataPort: selectedKeeper.DataPort,
    }, nil
}

// FileUploaded is called by Data Keeper when a file has been uploaded successfully
func (s *server) FileUploaded(ctx context.Context, req *pb.FileUploadedRequest) (*pb.FileUploadedResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    filename := req.Filename
    dataKeeper := req.DataKeeper
    filePath := req.FilePath
    
    // Update file table
    s.fileTable[filename] = &FileRecord{
        FileName:    filename,
        DataKeeper:  dataKeeper,
        FilePath:    filePath,
        IsAlive:     true,
        ReplicaList: []string{},
    }
    
    // Update replica count for this data keeper
    if keeper, exists := s.dataKeepers[dataKeeper]; exists {
        keeper.ReplicaCount++
    }
    
    // Select two other nodes for replication
    selectedReplicas := s.selectReplicaNodes(dataKeeper, 2)
    
    // Trigger replication to those nodes
    for _, replicaAddress := range selectedReplicas {
        replica := s.dataKeepers[replicaAddress]
        s.fileTable[filename].ReplicaList = append(s.fileTable[filename].ReplicaList, replicaAddress)
        
        // In a real implementation, you would initiate the replication here
        // For now, just log it
        log.Printf("Replicating file %s from %s to %s", filename, dataKeeper, replicaAddress)
        
        // Update replica count for the replica node
        replica.ReplicaCount++
    }
    
    return &pb.FileUploadedResponse{Success: true}, nil
}

// selectReplicaNodes selects numReplicas data keepers for replication, excluding the source
func (s *server) selectReplicaNodes(sourceKeeper string, numReplicas int) []string {
    replicas := []string{}
    
    for addr, keeper := range s.dataKeepers {
        if addr != sourceKeeper && keeper.IsAlive && len(replicas) < numReplicas {
            replicas = append(replicas, addr)
        }
    }
    
    return replicas
}

// UploadFile implementation from the original code
func (s *server) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
    log.Printf("Received upload request for file: %s", req.GetFilename())
    
    // Get a data keeper for upload
    uploadKeeperResp, err := s.GetUploadDataKeeper(ctx, &pb.GetUploadDataKeeperRequest{})
    if err != nil {
        return &pb.UploadFileResponse{Status: "Failed: No available data keepers"}, nil
    }
    
    // Inform client about the data keeper to use
    return &pb.UploadFileResponse{
        Status: "Ready for upload",
        DataKeeperAddress: uploadKeeperResp.Address,
        DataKeeperPort: uploadKeeperResp.DataPort,
    }, nil
}


func (s *server) DownloadFile(ctx context.Context, req *pb.DownloadFileRequest) (*pb.DownloadFileResponse, error) {
    filename := req.GetFilename()
    log.Printf("Download request for file: %s", filename)

    s.mu.RLock()
    fileRecord, exists := s.fileTable[filename]
    s.mu.RUnlock()

    if !exists {
        return nil, grpc.Errorf(codes.NotFound, "File not found")
    }

    // Build a list: include primary data keeper and any replica that is alive.
    downloadNodes := []string{}
    primaryKeeper := fileRecord.DataKeeper
    if keeper, ok := s.dataKeepers[primaryKeeper]; ok && keeper.IsAlive {
        // Split the host part from the primaryKeeper address (e.g., "localhost:50052" -> "localhost")
        host, _, err := net.SplitHostPort(primaryKeeper)
        if err != nil {
            host = primaryKeeper
        }
        downloadNodes = append(downloadNodes, net.JoinHostPort(host, keeper.DataPort))
    }
    for _, replicaAddr := range fileRecord.ReplicaList {
        if keeper, ok := s.dataKeepers[replicaAddr]; ok && keeper.IsAlive {
            host, _, err := net.SplitHostPort(replicaAddr)
            if err != nil {
                host = replicaAddr
            }
            downloadNodes = append(downloadNodes, net.JoinHostPort(host, keeper.DataPort))
        }
    }
    if len(downloadNodes) == 0 {
        return nil, grpc.Errorf(codes.Unavailable, "No available data keepers for file download")
    }
    return &pb.DownloadFileResponse{
        DataKeeperAddresses: downloadNodes,
    }, nil
}



// In masterTracker/main.go

// func (s *server) DownloadFile(ctx context.Context, req *pb.DownloadFileRequest) (*pb.DownloadFileResponse, error) {
//     filename := req.GetFilename()
//     log.Printf("Download request for file: %s", filename)

//     s.mu.RLock()
//     fileRecord, exists := s.fileTable[filename]
//     s.mu.RUnlock()

//     if !exists {
//         return nil, grpc.Errorf(codes.NotFound, "File not found")
//     }

//     // Build a list: include primary data keeper and any replica that is alive.
//     downloadNodes := []string{}
//     primaryKeeper := fileRecord.DataKeeper
//     if keeper, ok := s.dataKeepers[primaryKeeper]; ok && keeper.IsAlive {
//         downloadNodes = append(downloadNodes, net.JoinHostPort(primaryKeeper, keeper.DataPort))
//     }
//     for _, replicaAddr := range fileRecord.ReplicaList {
//         if keeper, ok := s.dataKeepers[replicaAddr]; ok && keeper.IsAlive {
//             downloadNodes = append(downloadNodes, net.JoinHostPort(replicaAddr, keeper.DataPort))
//         }
//     }
//     if len(downloadNodes) == 0 {
//         return nil, grpc.Errorf(codes.Unavailable, "No available data keepers for file download")
//     }
//     return &pb.DownloadFileResponse{
//         DataKeeperAddresses: downloadNodes,
//     }, nil
// }






// // DownloadFile and GetFileList implementations from the original code
// func (s *server) DownloadFile(ctx context.Context, req *pb.DownloadFileRequest) (*pb.DownloadFileResponse, error) {
//     filename := req.GetFilename()
//     log.Printf("Download request for file: %s", filename)
    
//     s.mu.RLock()
//     fileRecord, exists := s.fileTable[filename]
//     s.mu.RUnlock()
    
//     if !exists {
//         return nil, grpc.Errorf(codes.NotFound, "File not found")
//     }
    
//     // In a real implementation, you would return information about where to download the file
//     // For simplicity, we're returning dummy data
//     return &pb.DownloadFileResponse{
//         FileData: []byte("sample file content"),
//         DataKeeperAddress: fileRecord.DataKeeper,
//     }, nil
// }

func (s *server) GetFileList(ctx context.Context, req *pb.GetFileListRequest) (*pb.GetFileListResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    filenames := make([]string, 0, len(s.fileTable))
    for filename := range s.fileTable {
        filenames = append(filenames, filename)
    }
    
    return &pb.GetFileListResponse{Filenames: filenames}, nil
}

// Periodically check data keeper health
func (s *server) checkDataKeeperHealth() {
    for {
        time.Sleep(2 * time.Second)
        s.mu.Lock()
        
        for addr, keeper := range s.dataKeepers {
            // If not seen in the last 3 seconds, mark as dead
            if time.Since(keeper.LastSeen) > 3*time.Second {
                log.Printf("Data Keeper %s is not responding, marking as dead", addr)
                keeper.IsAlive = false
                
                // Update file records that were on this data keeper
                for _, fileRecord := range s.fileTable {
                    if fileRecord.DataKeeper == addr {
                        fileRecord.IsAlive = false
                    }
                }
            }
        }
        
        s.mu.Unlock()
    }
}

func main() {
    listen, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    s := NewServer()
    grpcServer := grpc.NewServer()
    pb.RegisterMasterTrackerServer(grpcServer, s)
    
    // Start the health checker in a goroutine
    go s.checkDataKeeperHealth()

    log.Println("Master Tracker server started on port 50051...")
    if err := grpcServer.Serve(listen); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}