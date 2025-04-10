package filetransfer

import (
	"log"
	"net"
)

func UploadFile(filename string, fileData []byte, targetIP string, targetPort string) error {
	// Simulate TCP connection to the DataKeeper
	conn, err := net.Dial("tcp", net.JoinHostPort(targetIP, targetPort))
	if err != nil {
		log.Printf("Error connecting to Data Keeper: %v", err)
		return err
	}
	defer conn.Close()

	// Send file data
	_, err = conn.Write(fileData)
	if err != nil {
		log.Printf("Error sending file: %v", err)
		return err
	}

	log.Printf("File %s uploaded successfully to %s:%s", filename, targetIP, targetPort)
	return nil
}
