package main

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"github.com/chain-lab/go-norn/common"
	"github.com/chain-lab/go-norn/crypto"
	log "github.com/sirupsen/logrus"
	karmem "karmem.org/golang"
	"time"
)

func buildTransaction(jsonLine []byte, key *ecdsa.PrivateKey) *common.Transaction {
	timestamp := time.Now().UnixMilli()

	txBody := common.TransactionBody{
		Data:      jsonLine,
		Timestamp: timestamp,
		Expire:    timestamp + 3000,
	}

	publicKeyBytes := crypto.PublicKey2Bytes(&key.PublicKey)
	copy(txBody.Public[:], publicKeyBytes)
	txBody.Address = crypto.PublicKeyBytes2Address(txBody.Public)
	txBody.Hash = [32]byte{}
	txBody.Signature = []byte{}

	writer := karmem.NewWriter(1024)
	txBody.WriteAsRoot(writer)
	txBodyBytes := writer.Bytes()

	hash := sha256.New()
	hash.Write(txBodyBytes)
	txHashBytes := hash.Sum(nil)

	txSignatureBytes, err := ecdsa.SignASN1(rand.Reader, key, txHashBytes)
	if err != nil {
		log.WithField("error", err).Errorln("Sign transaction failed.")
		return nil
	}

	copy(txBody.Hash[:], txHashBytes)
	txBody.Signature = txSignatureBytes

	return &common.Transaction{
		Body: txBody,
	}
}
