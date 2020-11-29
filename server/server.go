package main

import (
	"fmt"
	"io"
	"log"
	"net"

	data "github.com/jamoreno22/lab2_dist/datanode_1/pkg/proto"
	"google.golang.org/grpc"
)

type dataServer struct {
	data.UnimplementedDataNodeServer
}

var path = "Log"

// books variable when books are saved
var books = []data.Book{}

func main() {

	// create a listener on TCP port 7777
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 7777))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// create a server instance
	ds := dataServer{}                               // create a gRPC server object
	grpcDataServer := grpc.NewServer()               // attach the Ping service to the server
	data.RegisterDataNodeServer(grpcDataServer, &ds) // start the server

	log.Println("Server running ...")
	if err := grpcDataServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}

// - - - - - - - - - - - - - DataNode Server functions - - - - - - - - - - - -

// DistributeChunks server side
func (d *dataServer) DistributeChunks(dcs data.DataNode_DistributeChunksServer) error {
	log.Printf("Stream DistributeChunks")

	sP := []data.Proposal{}

	for {
		prop, err := dcs.Recv()
		if err == io.EOF {
			log.Printf("EOF ------------")
			return (dcs.SendAndClose(&data.Message{Text: "Oh no... EOF"}))
		}
		if err != nil {
			return err
		}

		sP = append(sP, *prop)
		return nil
	}
}

// UploadBook server side

func (d *dataServer) UploadBook(ubs data.DataNode_UploadBookServer) error {
	log.Printf("Stream UploadBook")

	book := data.Book{}
	indice := 0
	for {
		chunk, err := ubs.Recv()
		if err == io.EOF {
			books = append(books, book)
			log.Printf("EOF... books lenght = %d", len(books))
			return (ubs.SendAndClose(&data.Message{Text: "EOF"}))
		}
		if err != nil {
			return err
		}
		book.Chunks = append(book.Chunks, chunk)
		indice = indice + 1

	}
}
