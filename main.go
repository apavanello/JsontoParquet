package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/xitongsys/parquet-go-source/local"
)

type Persona struct {
	Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Email string `parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID    string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
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
	fmt.Println("Done")
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
