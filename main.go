package main

import (
	"archive/zip"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/brianvoe/gofakeit/v6"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
)

type PersonalDocuments struct {
	DocumentType   string `parquet:"name=documentType, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{randomstring:[CPF,RG]}"`
	DocumentNumber string `parquet:"name=DocumentNumber, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{ssn}"`
}

type Personal struct {
	Name              string            `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{name}"`
	Email             string            `parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{email}"`
	Phone             string            `parquet:"name=phone, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{phone}"`
	HomeTown          string            `parquet:"name=hometown, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{city}"`
	BrithState        string            `parquet:"name=brithState, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{state}"`
	Profession        string            `parquet:"name=profession, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{jobtitle}"`
	Income            string            `parquet:"name=income, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{number:1000,10000},00"`
	PersonalDocuments PersonalDocuments `parquet:"name=personalDocuments, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
}

type Persona struct {
	PersonId string   `parquet:"name=personId, type=BYTE_ARRAY, convertedtype=UTF8" fake:"{uuid}"`
	Personal Personal `parquet:"name=personal, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
	Status   bool     `parquet:"name=status, type=BOOLEAN"`
}

type Personas struct {
	Persona []Persona
}

func main() {

	args := os.Args[1:]

	// Parse command-line arguments
	genType := flag.String("type", "", "the type to process")
	quantity := flag.Int("qnt", 0, "the quantity to process")
	flag.Parse()

	if len(args) == 0 {
		fmt.Println("Error: no argument provided.")
		return
	}

	switch *genType {
	case "genData":
		genData(*quantity)
	case "convertData":
		convertData()
	default:
		fmt.Println("Invalid type:", *genType)
	}

	fmt.Println("Done")
}

func genData(qnt int) {

	fmt.Printf("Generating %v Personas \n", qnt)
	genPersonas := generatePersonaData(qnt)
	writeJson(genPersonas)
	writeParquet(genPersonas)

}

func convertData() {
	var wg sync.WaitGroup
	folderPath := "input"

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		// Skip directories and any hidden files.
		if info.IsDir() || filepath.Base(path)[0] == '.' {
			return nil
		}

		if parts := strings.Split(info.Name(), "."); parts[len(parts)-1] == "json" {
			parquetWriter(path, info.Name())
		}

		if parts := strings.Split(info.Name(), "."); parts[len(parts)-1] == "zip" {
			fmt.Println("Unziping")
			wg.Add(1)
			go unZip(&wg, path, info.Name())

		}

		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
	wg.Wait()
}

func writeJson(p Personas) {

	// Marshal the Person object to JSON
	jsonData, err := json.Marshal(p)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Create a file for writing
	size := len(p.Persona)
	file, err := os.Create(path.Join("generatedData", "JSON", fmt.Sprintf("generated-%v.json", size)))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// Write the JSON data to the file
	_, err = file.Write(jsonData)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

}

func writeParquet(p Personas) {

	fw, err := local.NewLocalFileWriter(path.Join(
		"generatedData", "PARQUET", fmt.Sprintf("generated-%v.parquet", len(p.Persona))))
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Persona), 2)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	//pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, persona := range p.Persona {

		if err = pw.Write(persona); err != nil {
			log.Println("Write error", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
}

func generatePersonaData(qnt int) (generatedPersonas Personas) {

	f := gofakeit.New(time.Now().UnixNano())
	var generatedPersona Persona

	for i := 1; i <= qnt; i++ {
		err := f.Struct(&generatedPersona)

		if err != nil {
			panic(err)
		}
		generatedPersonas.Persona = append(generatedPersonas.Persona, generatedPersona)
	}

	return generatedPersonas
}

func unZip(wg *sync.WaitGroup, path string, fileStr string) {
	defer wg.Done()
	// Open the zip file for reading.
	r, err := zip.OpenReader(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer r.Close()

	// Iterate over each file in the zip file.
	for _, f := range r.File {
		// Open the file in the zip for reading.
		rc, err := f.Open()
		if err != nil {
			fmt.Println(err)
			return
		}
		defer rc.Close()

		// Create the target file in the target directory.

		if err := os.Mkdir(filepath.Join("input", strings.Split(fileStr, ".zip")[0]), os.ModePerm); err != nil && !os.IsExist(err) {
			log.Fatal(err)
		}
		targetFile, err := os.Create(filepath.Join("input", strings.Split(fileStr, ".zip")[0], f.Name))

		if err != nil {
			fmt.Println(err)
			return
		}
		defer targetFile.Close()

		// Copy the contents of the file in the zip to the target file.
		_, err = io.Copy(targetFile, rc)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("Writing by zip")
		parquetWriter(filepath.Join("input", strings.Split(fileStr, ".zip")[0], f.Name), fileStr)
	}

}

func parquetWriter(path string, fileStr string) {

	fmt.Println(path, fileStr)

	parquetOutput := strings.Split(fileStr, ".zip")[0]

	// Read the JSON file into a byte slice.
	jsonBytes, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("failed to read input file: %v", err)
	}

	// Unmarshal the JSON data into a slice of maps.
	var personas []Persona

	err = json.Unmarshal(jsonBytes, &personas)
	if err != nil {
		log.Fatalf("failed to unmarshal JSON data: %v", err)
	}

	fw, err := local.NewLocalFileWriter("output/" + parquetOutput + ".parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Persona), 3)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, persona := range personas {

		if err = pw.Write(persona); err != nil {
			log.Println("Write error", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}

	log.Println("Write Finished")
	fw.Close()

}
