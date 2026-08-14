package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

const (
	Repo    = "greyhands2/snerdmq"
	Version = "v0.1.1"
)

func main() {
	plat := runtime.GOOS
	arch := runtime.GOARCH

	// Map Go arch names to our release arch names
	if arch == "amd64" {
		arch = "x64"
	}
	if plat == "darwin" {
		plat = "macos"
	}

	ext := ""
	if plat == "windows" {
		ext = ".exe"
	}

	binaryName := fmt.Sprintf("snerdmq-%s-%s%s", plat, arch, ext)
	downloadURL := fmt.Sprintf("https://github.com/%s/releases/download/%s/%s", Repo, Version, binaryName)

	// Since Go modules are downloaded into a global cache (GOPATH/pkg/mod) that is read-only,
	// downloading a binary into the package directory itself is an anti-pattern and often fails due to permissions.
	// Instead, we will download the binary into the developer's local project directory (or $GOPATH/bin).
	// For maximum ease of use, we will download it to ./bin/snerdmq in the CURRENT WORKING DIRECTORY where they run the command.

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Snerd] Error getting current directory: %v\n", err)
		os.Exit(1)
	}

	binDir := filepath.Join(cwd, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "[Snerd] Error creating bin directory: %v\n", err)
		os.Exit(1)
	}

	destPath := filepath.Join(binDir, "snerdmq"+ext)

	fmt.Printf("[Snerd] Downloading pre-compiled engine from GitHub: %s...\n", binaryName)

	resp, err := http.Get(downloadURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Snerd] Failed to download binary: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		fmt.Printf("\n[Snerd] WARN: Binary not found at %s\n", downloadURL)
		fmt.Println("[Snerd] (This is expected if you haven't published a GitHub Release yet)")
		fmt.Println("[Snerd] Please provide BinaryPath manually when initializing SnerdQueue.")
		os.Exit(0)
	}

	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "[Snerd] Failed to download binary: HTTP %d\n", resp.StatusCode)
		os.Exit(1)
	}

	outFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Snerd] Error creating file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, resp.Body); err != nil {
		fmt.Fprintf(os.Stderr, "[Snerd] Error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("[Snerd] Successfully installed Snerd Engine to %s!\n", destPath)
}
