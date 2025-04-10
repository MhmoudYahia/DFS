package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"path/filepath"

	pb "dfs-project/internal/grpc"

	"google.golang.org/grpc"
)

var (
	masterAddr   = flag.String("master", "localhost:50051", "The master tracker address")
	uploadFile   = flag.String("upload", "", "File to upload")
	downloadFile = flag.String("download", "", "File to download")
	listFiles    = flag.Bool("list", false, "List available files")
)

func uploadFileToSystem(client pb.MasterTrackerClient, filePath string) error {
	// Read file
	fileData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	// Get filename from path
	filename := filepath.Base(filePath)

	// Step 1: Contact master tracker for upload destination
	uploadResp, err := client.UploadFile(context.Background(), &pb.UploadFileRequest{
		Filename: filename,
	})

	if err != nil {
		return fmt.Errorf("failed to get upload destination: %v", err)
	}

	if uploadResp.Status != "Ready for upload" {
		return fmt.Errorf("master not ready for upload: %s", uploadResp.Status)
	}

	// Step 2: Connect to the data keeper and transfer the file
	dataKeeperAddr := uploadResp.DataKeeperAddress
	dataKeeperPort := uploadResp.DataKeeperPort
	log.Printf("Uploading file to Data Keeper at %s", dataKeeperAddr)
	log.Printf("Data Keeper Port: %s", dataKeeperPort)

	// Extract hostname from DataKeeperAddress

	// Combine host with data port for file transfers
	dataTransferAddr := "localhost:" + dataKeeperPort
	log.Printf("Uploading file to Data Keeper at %s (data port: %s)", dataKeeperAddr, dataKeeperPort)

	// Connect to data keeper using TCP
	conn, err := net.Dial("tcp", dataTransferAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to Data Keeper: %v", err)
	}
	defer conn.Close()

	// Send file data
	// First send the filename with a 2-byte length prefix
	filenameBuf := []byte(filename)
	lenBuf := []byte{byte(len(filenameBuf)), byte(len(filenameBuf) >> 8)}

	// Write the length prefix
	_, err = conn.Write(lenBuf)
	if err != nil {
		return fmt.Errorf("failed to send filename length: %v", err)
	}

	// Write the filename
	_, err = conn.Write(filenameBuf)
	if err != nil {
		return fmt.Errorf("failed to send filename: %v", err)
	}

	// Then send the file data
	_, err = conn.Write(fileData)
	if err != nil {

		return fmt.Errorf("failed to send file: %v", err)
	}

	log.Printf("File %s uploaded successfully (%d bytes)", filename, len(fileData))

	// The data keeper will notify the master, and the master will handle replication
	return nil
}

func downloadFileFromSystem(client pb.MasterTrackerClient, filename string) error {
    // Step 1: Contact master tracker for download location
    resp, err := client.DownloadFile(context.Background(), &pb.DownloadFileRequest{
        Filename: filename,
    })

    if err != nil {
        return fmt.Errorf("failed to get download information: %v", err)
    }

    // Step 2: Connect to the data keeper and download the file
    dataKeeperAddr := resp.DataKeeperAddress
    
    // Parse the address to extract host and port
    host, port, err := net.SplitHostPort(dataKeeperAddr)
    if err != nil {
        // If there's no port in the address, use as is
        host = dataKeeperAddr
        port = "9000" // Default data port if not specified
    }
    
    // Get the data port of the keeper (in a complete implementation, this would be in the response)
    // IMPORTANT: You would need to update the DownloadFileResponse proto to include DataKeeperPort like UploadFileResponse
    dataTransferAddr := net.JoinHostPort(host, port)
    
    log.Printf("Downloading file from Data Keeper at %s", dataTransferAddr)

    // Connect to data keeper using TCP
    conn, err := net.Dial("tcp", dataTransferAddr)
    if err != nil {
        return fmt.Errorf("failed to connect to Data Keeper: %v", err)
    }
    defer conn.Close()

    // Send the filename with a 2-byte length prefix
    filenameBuf := []byte(filename)
    lenBuf := []byte{byte(len(filenameBuf)), byte(len(filenameBuf) >> 8)}

    // Write the length prefix
    _, err = conn.Write(lenBuf)
    if err != nil {
        return fmt.Errorf("failed to send filename length: %v", err)
    }

    // Write the filename
    _, err = conn.Write(filenameBuf)
    if err != nil {
        return fmt.Errorf("failed to send filename: %v", err)
    }
    
    // Read the file data from the connection
    // First create a temporary buffer to store the data
    fileData, err := ioutil.ReadAll(conn)
    if err != nil {
        return fmt.Errorf("failed to read file data: %v", err)
    }
    
    // Save the file locally
    err = ioutil.WriteFile(filename, fileData, 0644)
    if err != nil {
        return fmt.Errorf("failed to save downloaded file: %v", err)
    }

    log.Printf("File %s downloaded successfully (%d bytes)", filename, len(fileData))
    return nil
}

func listAvailableFiles(client pb.MasterTrackerClient) error {
	resp, err := client.GetFileList(context.Background(), &pb.GetFileListRequest{})
	if err != nil {
		return fmt.Errorf("failed to get file list: %v", err)
	}

	fmt.Println("Available files:")
	for _, filename := range resp.Filenames {
		fmt.Printf("- %s\n", filename)
	}

	return nil
}

func main() {
	flag.Parse()

	// Connect to the Master Tracker gRPC server
	conn, err := grpc.Dial(*masterAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)

	// Handle commands
	if *listFiles {
		if err := listAvailableFiles(client); err != nil {
			log.Fatalf("Error listing files: %v", err)
		}
	} else if *uploadFile != "" {
		if err := uploadFileToSystem(client, *uploadFile); err != nil {
			log.Fatalf("Error uploading file: %v", err)
		}
	} else if *downloadFile != "" {
		if err := downloadFileFromSystem(client, *downloadFile); err != nil {
			log.Fatalf("Error downloading file: %v", err)
		}
	} else {
		flag.Usage()
	}
}
