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

    // Update the import path to match your project structure
    pb "dfs-project/internal/grpc"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

var (
    port       = flag.String("port", "50052", "The gRPC server port")
    dataPort   = flag.String("dataport", "9000", "The TCP port for file transfers")
    masterAddr = flag.String("master", "localhost:50051", "The master tracker address")
    storageDir = flag.String("dir", "./storage", "Directory to store files")
)

type server struct {
    pb.UnimplementedDataKeeperServer
    address    string
    dataPort   string
    storageDir string
}

func (s *server) FileTransfer(ctx context.Context, req *pb.FileTransferRequest) (*pb.FileTransferResponse, error) {
    sourceIP := req.GetSourceIp()
    destIP := req.GetDestinationIp()
    fileData := req.GetFileData()

    log.Printf("Transferring file from %s to %s, size: %d bytes", sourceIP, destIP, len(fileData))

    // In a real implementation, you would store the file on disk
    filePath := filepath.Join(s.storageDir, fmt.Sprintf("replica_from_%s", sourceIP))
    if err := os.WriteFile(filePath, fileData, 0644); err != nil {
        log.Printf("Failed to save replicated file: %v", err)
        return &pb.FileTransferResponse{Status: "Failed"}, nil
    }

    return &pb.FileTransferResponse{Status: "Transfer completed"}, nil
}

// handleFileUpload handles TCP connections for file uploads
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

// processFileUpload handles an individual file upload connection
func (s *server) processFileUpload(conn net.Conn) {
    defer conn.Close()

    // First read the filename from the connection
    // Read a 2-byte length prefix first
    lenBuf := make([]byte, 2)
    if _, err := io.ReadFull(conn, lenBuf); err != nil {
        log.Printf("Error reading filename length: %v", err)
        return
    }
    
    filenameLen := int(lenBuf[0]) | int(lenBuf[1])<<8
    
    // Now read the actual filename
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

    // Copy the rest of the data from connection to file
    n, err := io.Copy(file, conn)
    if err != nil {
        log.Printf("Error saving file: %v", err)
        return
    }

    log.Printf("Received file %s, %d bytes", filename, n)

    // Notify master tracker about the upload
    s.notifyMasterAboutUpload(filename, filePath)
}

// notifyMasterAboutUpload notifies the master tracker about a successful upload
func (s *server) notifyMasterAboutUpload(filename, filePath string) {
    // Connect to master tracker
    // Replace WithInsecure with WithTransportCredentials(insecure.NewCredentials())
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

// sendHeartbeats periodically sends heartbeats to the master tracker
func (s *server) sendHeartbeats() {
    // Connect to master tracker
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

        // Sleep for 1 second before next heartbeat
        time.Sleep(1 * time.Second)
    }
}

// registerWithMaster registers this data keeper with the master tracker
func (s *server) registerWithMaster() error {
    // Connect to master tracker
    conn, err := grpc.Dial(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return fmt.Errorf("failed to connect to master: %v", err)
    }
    defer conn.Close()

    client := pb.NewMasterTrackerClient(conn)

    // Register this data keeper
    resp, err := client.RegisterDataKeeper(context.Background(), &pb.RegisterDataKeeperRequest{
        Address:  s.address,
        DataPort: s.dataPort,
    })

    if err != nil || !resp.Success {
        return fmt.Errorf("failed to register with master: %v", err)
    }

    log.Printf("Successfully registered with master tracker")
    return nil
}

func main() {
    flag.Parse()

    // Create storage directory if it doesn't exist
    if err := os.MkdirAll(*storageDir, 0755); err != nil {
        log.Fatalf("Failed to create storage directory: %v", err)
    }

    // Start gRPC server
    grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
    if err != nil {
        log.Fatalf("Failed to listen on port %s: %v", *port, err)
    }

    // Start data server for file transfers
    dataListener, err := net.Listen("tcp", fmt.Sprintf(":%s", *dataPort))
    if err != nil {
        log.Fatalf("Failed to listen on data port %s: %v", *dataPort, err)
    }

    s := &server{
        address:    fmt.Sprintf("localhost:%s", *port),
        dataPort:   *dataPort,
        storageDir: *storageDir,
    }

    // Register with master
    if err := s.registerWithMaster(); err != nil {
        log.Fatalf("Failed to register with master: %v", err)
    }

    // Start sending heartbeats
    go s.sendHeartbeats()

    // Start handling file uploads
    go s.handleFileUpload(dataListener)

    // Start gRPC server
    grpcServer := grpc.NewServer()
    pb.RegisterDataKeeperServer(grpcServer, s)

    log.Printf("Data Keeper server started on port %s with data port %s...", *port, *dataPort)
    if err := grpcServer.Serve(grpcListener); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}