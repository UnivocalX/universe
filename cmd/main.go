package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/UnivocalX/universe"
)

type ChecksumType string

const (
	SHA256 ChecksumType = "SHA256"
)

type Checksum struct {
	Path string
	Hash string
	Type ChecksumType
}

func (c Checksum) String() string {
	return fmt.Sprintf("%q: %q", c.Path, c.Hash)
}

type Storage interface {
	Search(path string) ([]string, error)
	GetData(ctx context.Context, path string) (io.ReadCloser, error)
}

func main() {
	origin := "testdata"
	if err := HandleOrigin(origin); err != nil {
		panic(err)
	}
}

func HandleOrigin(origin string) error {
	ctx := context.Background()
	var s Storage = &LocalStorage{}

	paths, err := s.Search(origin)
	if err != nil {
		return err
	}

	processFile := func(path string) (Checksum, error) {
		reader, err := s.GetData(ctx, path)
		if err != nil {
			return Checksum{}, err
		}

		hash, err := HashStream(reader)
		if err != nil {
			return Checksum{}, err
		}

		return Checksum{
			Path: path,
			Hash: hash,
			Type: SHA256,
		}, nil
	}
	
	// process files concurrently with a pipeline
	p := universe.From(ctx, paths...)
	checksums := universe.Map(p, processFile).
		Concurrent(0).
		Buffer(10).
		Execute()

	// print results
	checksums.ForEach(
		ctx,
		func(c Checksum, e error) {
			if e != nil {
				fmt.Printf("Error processing file: %v\n", e)
			}
			fmt.Printf("%s\n", c)
		},
	)

	return nil
}

func HashStream(r io.ReadCloser) (string, error) {
	h := sha256.New()
	defer r.Close()

	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

type LocalStorage struct{}

func (ls *LocalStorage) Search(path string) ([]string, error) {
	osFileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if osFileInfo.IsDir() {
		dirEntries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}

		var paths []string
		for _, entry := range dirEntries {
			if !entry.IsDir() {
				paths = append(paths, path+"/"+entry.Name())
			}
		}
		return paths, nil
	}

	return []string{path}, nil
}

func (ls *LocalStorage) GetData(ctx context.Context, path string) (io.ReadCloser, error) {
	return os.Open(path)
}
