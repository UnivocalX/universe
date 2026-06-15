package universe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"
	"time"
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

func HashStream(r io.Reader) (Checksum, error) {
	h := sha256.New()

	if _, err := io.Copy(h, r); err != nil {
		return Checksum{}, err
	}

	return Checksum{
		Hash: hex.EncodeToString(h.Sum(nil)),
		Type: SHA256,
	}, nil
}

func ComputeSHA256Pipline(ctx context.Context, paths <-chan string) Stream[Checksum] {
	ChecksumStream := Map(
		Ingest(ctx, paths),
		func(path string) (Checksum, error) {
			f, err := os.Open(path)
			if err != nil {
				return Checksum{}, err
			}
			defer f.Close()

			checksum, err := HashStream(f)
			if err != nil {
				return Checksum{}, err
			}

			checksum.Path = path
			return checksum, nil
		},
	).
		Concurrent(4).
		Buffer(10).
		Execute()

	return ChecksumStream
}

func ComputeChecksumsSequential(paths []string) ([]Checksum, error) {
	results := make([]Checksum, 0, len(paths))
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		checksum, err := HashStream(f)
		f.Close()
		if err != nil {
			return nil, err
		}
		checksum.Path = path
		results = append(results, checksum)
	}
	return results, nil
}

func TestComputeChecksums(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// read directory
	directory := "./testdata"
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	if len(entries) == 0 {
		t.Fatalf("No files found in directory: %s", directory)
	}

	// create list of file paths
	files := make([]string, len(entries))
	for i, entry := range entries {
		if entry.IsDir() {
			continue
		}
		files[i] = directory + "/" + entry.Name()
	}

	// compute checksums
	checksumStream := ComputeSHA256Pipline(ctx, Source(ctx, files))

	for checksum := range checksumStream.Collect(ctx) {
		if checksum.Err != nil {
			t.Errorf("Error computing checksum for %s: %v", checksum.Value.Path, checksum.Err)
			continue
		}
		t.Logf("Checksum for %s: %s", checksum.Value.Path, checksum.Value.Hash)
	}
}

func TestComputeChecksumsSequential(t *testing.T) {
	directory := "./testdata"
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	if len(entries) == 0 {
		t.Fatalf("No files found in directory: %s", directory)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, directory+"/"+entry.Name())
		}
	}

	checksums, err := ComputeChecksumsSequential(files)
	if err != nil {
		t.Fatalf("Error computing checksums: %v", err)
	}

	for _, checksum := range checksums {
		t.Logf("Checksum for %s: %s", checksum.Path, checksum.Hash)
	}
}
