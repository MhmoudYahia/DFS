package main

import (
    "context"
    "flag"
    "fmt"
    "io"
    "log"
    "net"
    "os"
    "path/filepath"
    "time"

    pb "dfs-project/internal/grpc"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

var (
    port         = flag.String("port", "50052", "The gRPC server port")
    dataPort     = flag.String("dataport", "9000", "The TCP port for file uploads")
    downloadPort = flag.String("downloadport", "9100", "The TCP port for file downloads")
    masterAddr   = flag.String("master", "localhost:50051", "The master tracker address")
    storageDir   = flag.String("dir", "./storage", "Directory to store files")
)

type server struct {
    pb.UnimplementedDataKeeperServer
    address    string
    dataPort   string
    downloadPort string
    storageDir string
}

// FileTransfer transfers file data between nodes.
func (s *server) FileTransfer(ctx context.Context, req *pb.FileTransferRequest) (*pb.FileTransferResponse, error) {
    sourceIP := req.GetSourceIp()
    destIP := req.GetDestinationIp()
    fileData := req.GetFileData()

    log.Printf("[Replication] File replication initiated from %s to %s, size: %d bytes", sourceIP, destIP, len(fileData))

    // For a replica, store the file using a naming convention.
    filePath := filepath.Join(s.storageDir, fmt.Sprintf("replica_from_%s", sourceIP))
    if err := os.WriteFile(filePath, fileData, 0644); err != nil {
        log.Printf("Failed to save replicated file: %v", err)
        return &pb.FileTransferResponse{Status: "Failed"}, nil
    }

    return &pb.FileTransferResponse{Status: "Transfer completed"}, nil
}

// GetFile reads a file from the storage directory and returns its content.
// This is used by the master to retrieve the file data from a source DataKeeper.
func (s *server) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
    filename := req.GetFilename()
    filePath := filepath.Join(s.storageDir, filename)
    fileData, err := os.ReadFile(filePath)
    if err != nil {
        log.Printf("GetFile: failed to read file %s: %v", filename, err)
        return nil, err
    }
    return &pb.GetFileResponse{FileData: fileData}, nil
}

// ReplicateFile writes the replicated file to disk.
func (s *server) ReplicateFile(ctx context.Context, req *pb.ReplicateFileRequest) (*pb.FileTransferResponse, error) {
    filename := req.GetFilename()
    fileData := req.GetFileData()
    filePath := filepath.Join(s.storageDir, filename)
    if err := os.WriteFile(filePath, fileData, 0644); err != nil {
        log.Printf("ReplicateFile: failed to write replica file %s: %v", filename, err)
        return &pb.FileTransferResponse{Status: "Failed"}, nil
    }
    log.Printf("Replicated file %s successfully", filename)
    return &pb.FileTransferResponse{Status: "Transfer completed"}, nil
}

// handleFileUpload handles TCP connections for file uploads.
func (s *server) handleFileUpload(listener net.Listener) {
    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Printf("Error accepting connection: %v", err)
            continue
        }
        go s.processFileUpload(conn)
    }
}

// processFileUpload handles an individual file upload connection.
func (s *server) processFileUpload(conn net.Conn) {
    defer conn.Close()

    // Read the 2-byte length prefix to determine filename length.
    lenBuf := make([]byte, 2)
    if _, err := io.ReadFull(conn, lenBuf); err != nil {
        log.Printf("Error reading filename length: %v", err)
        return
    }

    filenameLen := int(lenBuf[0]) | int(lenBuf[1])<<8

    // Read the actual filename.
    filenameBuf := make([]byte, filenameLen)
    if _, err := io.ReadFull(conn, filenameBuf); err != nil {
        log.Printf("Error reading filename: %v", err)
        return
    }

    filename := string(filenameBuf)
    filePath := filepath.Join(s.storageDir, filename)

    log.Printf("Receiving file: %s", filename)

    file, err := os.Create(filePath)
    if err != nil {
        log.Printf("Failed to create file: %v", err)
        return
    }
    defer file.Close()

    // Copy the rest of the data from the connection to the file.
    n, err := io.Copy(file, conn)
    if err != nil {
        log.Printf("Error saving file: %v", err)
        return
    }

    log.Printf("Received file %s, %d bytes", filename, n)

    // Notify the master tracker about the upload.
    s.notifyMasterAboutUpload(filename, filePath)
}

// notifyMasterAboutUpload notifies the master tracker of a successful file upload.
func (s *server) notifyMasterAboutUpload(filename, filePath string) {
    conn, err := grpc.Dial(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Printf("Failed to connect to master: %v", err)
        return
    }
    defer conn.Close()

    client := pb.NewMasterTrackerClient(conn)

    resp, err := client.FileUploaded(context.Background(), &pb.FileUploadedRequest{
        Filename:   filename,
        DataKeeper: s.address,
        FilePath:   filePath,
    })

    if err != nil || !resp.Success {
        log.Printf("Failed to notify master about upload: %v", err)
    } else {
        log.Printf("Master notified about file upload: %s", filename)
    }
}

// processFileDownload handles an individual file download request.
func (s *server) processFileDownload(conn net.Conn) {
    defer conn.Close()

    // Read the 2-byte length prefix to determine filename length.
    lenBuf := make([]byte, 2)
    if _, err := io.ReadFull(conn, lenBuf); err != nil {
        log.Printf("Error reading filename length for download: %v", err)
        return
    }

    filenameLen := int(lenBuf[0]) | int(lenBuf[1])<<8
    filenameBuf := make([]byte, filenameLen)
    if _, err := io.ReadFull(conn, filenameBuf); err != nil {
        log.Printf("Error reading filename for download: %v", err)
        return
    }

    filename := string(filenameBuf)
    filePath := filepath.Join(s.storageDir, filename)

    // Open the file.
    file, err := os.Open(filePath)
    if err != nil {
        log.Printf("Failed to open file %s for download: %v", filename, err)
        return
    }
    defer file.Close()

    // Send the file content over the connection.
    n, err := io.Copy(conn, file)
    if err != nil {
        log.Printf("Error sending file %s: %v", filename, err)
    }
    log.Printf("Sent file %s, %d bytes", filename, n)
}

// handleFileDownload handles TCP connections for file downloads.
func (s *server) handleFileDownload(listener net.Listener) {
    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Printf("Error accepting download connection: %v", err)
            continue
        }
        go s.processFileDownload(conn)
    }
}

// sendHeartbeats periodically sends heartbeats to the master tracker.
func (s *server) sendHeartbeats() {
    conn, err := grpc.Dial(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatalf("Failed to connect to master for heartbeats: %v", err)
    }
    defer conn.Close()

    client := pb.NewMasterTrackerClient(conn)

    for {
        resp, err := client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
            Address: s.address,
        })

        if err != nil || !resp.Success {
            log.Printf("Heartbeat failed: %v", err)
        }

        time.Sleep(1 * time.Second)
    }
}

// registerWithMaster registers this DataKeeper with the master tracker.
func (s *server) registerWithMaster() error {
    conn, err := grpc.Dial(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return fmt.Errorf("failed to connect to master: %v", err)
    }
    defer conn.Close()

    client := pb.NewMasterTrackerClient(conn)

    resp, err := client.RegisterDataKeeper(context.Background(), &pb.RegisterDataKeeperRequest{
        Address:      s.address,
        DataPort:     s.dataPort,
        DownloadPort: *downloadPort,
    })

    if err != nil || !resp.Success {
        return fmt.Errorf("failed to register with master: %v", err)
    }

    log.Printf("Successfully registered with master tracker")
    return nil
}

func main() {
    flag.Parse()

    // Create the storage directory if it doesn't exist.
    if err := os.MkdirAll(*storageDir, 0755); err != nil {
        log.Fatalf("Failed to create storage directory: %v", err)
    }

    // Start the gRPC server.
    grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
    if err != nil {
        log.Fatalf("Failed to listen on port %s: %v", *port, err)
    }

    // Start the TCP listener for file uploads.
    dataListener, err := net.Listen("tcp", fmt.Sprintf(":%s", *dataPort))
    if err != nil {
        log.Fatalf("Failed to listen on data port %s: %v", *dataPort, err)
    }

    // Start the TCP listener for file downloads.
    downloadListener, err := net.Listen("tcp", fmt.Sprintf(":%s", *downloadPort))
    if err != nil {
        log.Fatalf("Failed to listen on download port %s: %v", *downloadPort, err)
    }

	 // Get the externally accessible IP address (or use hostname)
    // You can use one of these approaches:
    // 1. Use a specific command line argument for external address
    externalAddr := flag.String("external", "", "External IP or hostname for this data keeper")
    flag.Parse()
    
    myAddress := *externalAddr
    if myAddress == "" {
        // Default to hostname if not specified
        hostname, err := os.Hostname()
        if err != nil {
            log.Printf("Warning: Unable to get hostname, falling back to localhost: %v", err)
            myAddress = "localhost"
        } else {
            myAddress = hostname
        }
    }

    s := &server{
        address:      fmt.Sprintf("%s:%s", myAddress, *port),
        dataPort:     *dataPort,
        downloadPort: *downloadPort,
        storageDir:   *storageDir,
    }

    // Register with the master tracker.
    if err := s.registerWithMaster(); err != nil {
        log.Fatalf("Failed to register with master: %v", err)
    }

    // Start sending heartbeats.
    go s.sendHeartbeats()

    // Start handling file uploads via TCP.
    go s.handleFileUpload(dataListener)

    // Start handling file downloads via TCP.
    go s.handleFileDownload(downloadListener)

    // Start the gRPC server.
    grpcServer := grpc.NewServer()
    pb.RegisterDataKeeperServer(grpcServer, s)

    log.Printf("Data Keeper server started on port %s with upload port %s and download port %s...", *port, *dataPort, *downloadPort)
    if err := grpcServer.Serve(grpcListener); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
