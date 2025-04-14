package main

import (
	"context"
	pb "dfs-project/internal/grpc"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
)

var (
	masterAddr   = flag.String("master", "localhost:50051", "The master tracker address")
	uploadFile   = flag.String("upload", "", "File to upload")
	downloadFile = flag.String("download", "", "File to download")
	listFiles    = flag.Bool("list", false, "List available files")
)

// uploadFileToSystem handles the file upload.
func uploadFileToSystem(client pb.MasterTrackerClient, filePath string) error {
	// Read file data from disk.
	fileData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	filename := filepath.Base(filePath)

	// Ask the master for an upload destination.
	uploadResp, err := client.UploadFile(context.Background(), &pb.UploadFileRequest{
		Filename: filename,
	})
	if err != nil {
		return fmt.Errorf("failed to get upload destination: %v", err)
	}

	if uploadResp.Status != "Ready for upload" {
		return fmt.Errorf("master not ready for upload: %s", uploadResp.Status)
	}

	dataKeeperAddr := uploadResp.DataKeeperAddress
	dataKeeperPort := uploadResp.DataKeeperPort
	log.Printf("Uploading file to Data Keeper at %s (upload port: %s)", dataKeeperAddr, dataKeeperPort)

	// Extract host from the data keeper address (which might already include a port)
	host, _, err := net.SplitHostPort(dataKeeperAddr)

	if err != nil {
		// If there's no port, use the address as-is
		host = dataKeeperAddr
	}
	// Use the upload port for transferring file data via TCP.
	dataTransferAddr := net.JoinHostPort(host, dataKeeperPort)
	conn, err := net.Dial("tcp", dataTransferAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to Data Keeper: %v", err)
	}
	defer conn.Close()

	// Send the filename (with a 2-byte length prefix) then the file contents.
	filenameBuf := []byte(filename)
	lenBuf := []byte{byte(len(filenameBuf)), byte(len(filenameBuf) >> 8)}

	if _, err = conn.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to send filename length: %v", err)
	}
	if _, err = conn.Write(filenameBuf); err != nil {
		return fmt.Errorf("failed to send filename: %v", err)
	}
	if _, err = conn.Write(fileData); err != nil {
		return fmt.Errorf("failed to send file: %v", err)
	}

	log.Printf("File %s uploaded successfully (%d bytes)", filename, len(fileData))
	return nil
}

// downloadFileFromSystem handles file download via TCP.
func downloadFileFromSystem(client pb.MasterTrackerClient, filename string) error {
	resp, err := client.DownloadFile(context.Background(), &pb.DownloadFileRequest{
		Filename: filename,
	})
	if err != nil {
		return fmt.Errorf("failed to get download information: %v", err)
	}

	if len(resp.DataKeeperAddresses) == 0 {
		return fmt.Errorf("no download addresses provided by master")
	}

	// Connect to the first available download endpoint.
	dataTransferAddr := resp.DataKeeperAddresses[0]
	log.Printf("Downloading file from Data Keeper at %s", dataTransferAddr)

	conn, err := net.Dial("tcp", dataTransferAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to Data Keeper: %v", err)
	}
	defer conn.Close()

	// Send the filename (with the same 2-byte length prefix protocol).
	filenameBuf := []byte(filename)
	lenBuf := []byte{byte(len(filenameBuf)), byte(len(filenameBuf) >> 8)}
	if _, err = conn.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to send filename length: %v", err)
	}
	if _, err = conn.Write(filenameBuf); err != nil {
		return fmt.Errorf("failed to send filename: %v", err)
	}

	// Read the file data returned.
	fileData, err := ioutil.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("failed to read file data: %v", err)
	}

	// Create "Downloaded" folder if it doesn't exist.
	downloadFolder := "Downloaded"
	if err := os.MkdirAll(downloadFolder, 0755); err != nil {
		return fmt.Errorf("failed to create download folder: %v", err)
	}

	// Save the file within the "Downloaded" folder.
	outPath := filepath.Join(downloadFolder, filename)
	if err = ioutil.WriteFile(outPath, fileData, 0644); err != nil {
		return fmt.Errorf("failed to save downloaded file: %v", err)
	}

	log.Printf("File %s downloaded successfully to %s (%d bytes)", filename, outPath, len(fileData))
	return nil
}

// listAvailableFiles prints the list of available files.
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

	conn, err := grpc.Dial(*masterAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to Master Tracker: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)

	// Execute the selected command.
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
