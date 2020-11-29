package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"

	data "github.com/jamoreno22/lab2_dist/datanode_1/pkg/proto"
	"google.golang.org/grpc"
)

var bookName string

func main() {
	var conn *grpc.ClientConn

	conn, err := grpc.Dial(":7777", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}

	defer conn.Close()

	//dc := gral.NewDataNodeClient(conn)

	fmt.Println("Seleccione qué desea hacer:")
	fmt.Println("0 : Cargar un libro")
	fmt.Println("1 : Descargar un libro")

	reader := bufio.NewReader(os.Stdin)
	char, _, err := reader.ReadRune()

	if err != nil {
		fmt.Println(err)
	}

	switch char {
	//Upload
	case '0':
		fmt.Println("Carga")
		fmt.Println("Seleccione distribución:")
		fmt.Println("0 : Centralizada")
		fmt.Println("1 : Distribuida")
		r := bufio.NewReader(os.Stdin)
		c, _, err := r.ReadRune()

		if err != nil {
			fmt.Println(err)
		}
		switch c {
		//Centralizado
		case '0':
			break
		//Distribuido
		case '1':
			break
		}
		break
	//Download
	case '1':
		fmt.Println("Ingrese nombre del libro a descargar: ")
		r := bufio.NewReader(os.Stdin)
		c, _, err := r.ReadRune()

		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(c)
		break
	}

	//var fileToBeChunked string
	//fileToBeChunked = "books/Mujercitas-Alcott_Louisa_May.pdf"
	//bookName = "Mujercitas-Alcott_Louisa_May.pdf"
	//runUploadBook(dc, fileToBeChunked)

}

func runUploadBook(dc data.DataNodeClient, fileToBeChunked string) error {
	// -    - - - - - - -  - -    particionar pdf en chunks - - - - -  - - - -

	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 250000 // 1 MB, change this to your requirement

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	book := make([]*data.Chunk, totalPartsNum)

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		// write to disk
		fileName := bookName + strconv.FormatUint(i, 10)
		_, err := os.Create(fileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// write/save buffer to disk
		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

		// books instantiation
		book[i] = &data.Chunk{Name: fileName, Data: partBuffer}

		fmt.Println("Split to : ", fileName)
		log.Println("tamaño: ", partSize)
	}

	// - - - - - --- -- - -  stream chunks - - - - - - - - - - - -
	stream, err := dc.UploadBook(context.Background())
	if err != nil {
		log.Println("Error de stream uploadBook")
	}
	a := 1
	for _, chunk := range book {
		if err := stream.Send(chunk); err != nil {
			log.Println("error al enviar chunk")
			log.Fatalf("%v.Send(%d) = %v", stream, a, err)
		}
		a = a + 1
	}
	reply, err := stream.CloseAndRecv()
	if err != nil {
		log.Println("Error recepcion response")
	}
	log.Printf("Route summary: %v", reply)
	return nil
}
