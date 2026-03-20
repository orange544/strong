package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"github.com/chain-lab/go-norn/offchain"
	"github.com/chain-lab/go-norn/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	defaultRPCEndpoint = "127.0.0.1:45558"
	defaultIPFSAPI     = "http://127.0.0.1:5001"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "put":
		if err := runPut(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "put failed: %v\n", err)
			os.Exit(1)
		}
	case "get":
		if err := runGet(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "get failed: %v\n", err)
			os.Exit(1)
		}
	default:
		printUsage()
		os.Exit(1)
	}
}

func runPut(args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	rpcAddr := fs.String("rpc", defaultRPCEndpoint, "blockchain gRPC address")
	ipfsAPI := fs.String("ipfs", defaultIPFSAPI, "ipfs api base url")
	timeoutSec := fs.Int("timeout", 8, "request timeout in seconds")
	receiver := fs.String("receiver", "", "receiver address (20 bytes hex)")
	key := fs.String("key", "", "data key on chain")
	filePath := fs.String("file", "", "path of file to upload")
	text := fs.String("text", "", "plain text to upload")
	txType := fs.String("type", "set", "transaction data mode: set|append")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *receiver == "" {
		return errors.New("receiver is required")
	}
	if *key == "" {
		return errors.New("key is required")
	}
	if *filePath == "" && *text == "" {
		return errors.New("use -file or -text to provide content")
	}
	if *filePath != "" && *text != "" {
		return errors.New("use only one content source: -file or -text")
	}
	if *txType != "set" && *txType != "append" {
		return errors.New("type must be set or append")
	}

	normalizedReceiver, err := normalizeAddress(*receiver)
	if err != nil {
		return err
	}

	var (
		payload []byte
		name    string
	)
	if *filePath != "" {
		payload, err = os.ReadFile(*filePath)
		if err != nil {
			return err
		}
		name = filepath.Base(*filePath)
	} else {
		payload = []byte(*text)
		name = "payload.txt"
	}

	timeout := time.Duration(*timeoutSec) * time.Second
	ipfsClient := offchain.NewIPFSClient(*ipfsAPI, timeout)
	cid, err := ipfsClient.AddBytes(name, payload)
	if err != nil {
		return err
	}

	bcClient, closeConn, err := newBlockchainClient(*rpcAddr, timeout)
	if err != nil {
		return err
	}
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &pb.SendTransactionWithDataReq{
		Type:     strPtr(*txType),
		Receiver: strPtr(normalizedReceiver),
		Key:      strPtr(*key),
		Value:    strPtr(cid),
	}
	resp, err := bcClient.SendTransactionWithData(ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("CID: %s\n", cid)
	fmt.Printf("TxHash: %s\n", resp.GetTxHash())
	fmt.Printf("Receiver: %s\n", normalizedReceiver)
	fmt.Printf("Key: %s\n", *key)
	return nil
}

func runGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	rpcAddr := fs.String("rpc", defaultRPCEndpoint, "blockchain gRPC address")
	ipfsAPI := fs.String("ipfs", defaultIPFSAPI, "ipfs api base url")
	timeoutSec := fs.Int("timeout", 8, "request timeout in seconds")
	address := fs.String("address", "", "receiver address (20 bytes hex)")
	key := fs.String("key", "", "data key on chain")
	outPath := fs.String("out", "", "optional output file path")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *address == "" {
		return errors.New("address is required")
	}
	if *key == "" {
		return errors.New("key is required")
	}

	normalizedAddress, err := normalizeAddress(*address)
	if err != nil {
		return err
	}

	timeout := time.Duration(*timeoutSec) * time.Second
	bcClient, closeConn, err := newBlockchainClient(*rpcAddr, timeout)
	if err != nil {
		return err
	}
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	readReq := &pb.ReadContractAddressReq{
		Address: strPtr(normalizedAddress),
		Key:     strPtr(*key),
	}
	readResp, err := bcClient.ReadContractAddress(ctx, readReq)
	if err != nil {
		return err
	}

	cid := strings.TrimSpace(readResp.GetHex())
	if cid == "" {
		return errors.New("empty cid on chain for this address/key")
	}

	ipfsClient := offchain.NewIPFSClient(*ipfsAPI, timeout)
	data, err := ipfsClient.Cat(cid)
	if err != nil {
		return err
	}

	fmt.Printf("CID: %s\n", cid)
	fmt.Printf("Bytes: %d\n", len(data))

	if *outPath != "" {
		if err = os.WriteFile(*outPath, data, 0644); err != nil {
			return err
		}
		fmt.Printf("Saved: %s\n", *outPath)
		return nil
	}

	if isText(data) {
		fmt.Printf("Content:\n%s\n", string(data))
		return nil
	}

	fmt.Printf("Content(base64):\n%s\n", base64.StdEncoding.EncodeToString(data))
	return nil
}

func newBlockchainClient(rpcAddr string, timeout time.Duration) (pb.BlockchainClient, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		rpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}

	closeFn := func() {
		_ = conn.Close()
	}
	return pb.NewBlockchainClient(conn), closeFn, nil
}

func normalizeAddress(address string) (string, error) {
	trimmed := strings.TrimSpace(address)
	trimmed = strings.TrimPrefix(trimmed, "0x")
	trimmed = strings.TrimPrefix(trimmed, "0X")

	decoded, err := hex.DecodeString(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid address hex: %w", err)
	}
	if len(decoded) != 20 {
		return "", fmt.Errorf("address must be exactly 20 bytes (got %d)", len(decoded))
	}
	return trimmed, nil
}

func isText(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	if !utf8.Valid(data) {
		return false
	}
	for _, b := range data {
		if b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		if b < 32 {
			return false
		}
	}
	return true
}

func strPtr(v string) *string {
	return &v
}

func printUsage() {
	fmt.Println(`ipfs-chain: IPFS <-> go-norn bridge

Usage:
  ipfs-chain put -receiver <hex20> -key <key> [-file <path> | -text <text>] [-rpc 127.0.0.1:45558] [-ipfs http://127.0.0.1:5001]
  ipfs-chain get -address <hex20> -key <key> [-out <path>] [-rpc 127.0.0.1:45558] [-ipfs http://127.0.0.1:5001]

Examples:
  ipfs-chain put -receiver f5c5822480a49523033fca24eb35bb5b8238b70d -key demo -text "hello ipfs"
  ipfs-chain get -address f5c5822480a49523033fca24eb35bb5b8238b70d -key demo -out ./demo.txt`)
}
