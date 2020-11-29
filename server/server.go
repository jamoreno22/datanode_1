package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"time"

	data "github.com/jamoreno22/lab2_dist/datanode_1/pkg/proto"
	"google.golang.org/grpc"
)

type dataNodeServer struct {
	data.UnimplementedDataNodeServer
}

// books variable when books are saved
var books = []data.Book{}

func main() {

	// create a listener on TCP port 7777
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 7777))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// create a server instance
	ds := dataNodeServer{}                               // create a gRPC server object
	grpcDataNodeServer := grpc.NewServer()               // attach the Ping service to the server
	data.RegisterDataNodeServer(grpcDataNodeServer, &ds) // start the server

	log.Println("DataNode Server running ...")
	if err := grpcDataNodeServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}

// - - - - - - - - - - - - - DataNode Server functions - - - - - - - - - - - -

// DistributeChunks server side
func (d *dataNodeServer) DistributeChunks(dcs data.DataNode_DistributeChunksServer) error {
	log.Printf("Stream DistributeChunks")

	sP := []data.Proposal{}

	for {
		prop, err := dcs.Recv()
		if err == io.EOF {
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

func (d *dataNodeServer) UploadBook(ubs data.DataNode_UploadBookServer) error {
	log.Printf("Stream UploadBook")
	book := data.Book{}
	indice := 0
	for {
		chunk, err := ubs.Recv()
		if err == io.EOF {
			books = append(books, book)
			log.Printf("EOF... books lenght = %d", len(books))
			prop := generateProposals(books, []string{"10.10.28.17:9000", "10.10.28.18:9000", "10.10.28.19:9000"})

			//if distribuido

			b, i := checkProposal(prop)
			if !b {
				prop = generateProposals(books, i)
			}

			//if distribuido

			return (ubs.SendAndClose(&data.Message{Text: "EOF"}))
		}
		if err != nil {
			return err
		}
		book.Chunks = append(book.Chunks, chunk)
		indice = indice + 1

	}
}

func generateProposals(book data.Book, Ips []string) []data.Proposal {
	var props []data.Proposal
	for _, chunk := range book {
		randomIP := Ips[rand.Intn(len(Ips))]
		props = append(props, data.Proposal{Ip: randomIP, Chunk: chunk})
	}
	return props
}

func checkProposal(props []data.Proposal) (bool, []string) {
	var ips = []string{"10.10.28.17:9000", "10.10.28.18:9000", "10.10.28.19:9000"}
	var gIps []string

	for _, ip := range ips {
		if pingDataNode(ip) {
			gIps = append(gIps, ip)
		}
	}

	for _, prop := range props {
		if !stringInSlice(prop.Ip, gIps) {
			return false, gIps
		}
	}
	return true, gIps
}

func pingDataNode(ip string) bool {
	timeOut := time.Duration(10 * time.Second)
	_, err := net.DialTimeout("tcp", ip, timeOut)
	if err != nil {
		return false
	}
	return true
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
