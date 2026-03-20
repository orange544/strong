/**
  @author: decision
  @date: 2023/6/20
  @note: 交易发送模拟代码
**/

package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/hex"
	"github.com/chain-lab/go-norn/rpc/pb"
	"github.com/chain-lab/go-norn/utils"
	"github.com/gogo/protobuf/proto"
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yaml"
	log "github.com/sirupsen/logrus"
	rand2 "golang.org/x/exp/rand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"strings"
	"time"
)

func main() {
	LoadConfig("./config.yml")
	addresses := config.Strings("rpc.address")

	for {
		idx := rand2.Intn(len(addresses))
		addr := addresses[idx]
		log.Infof("Select host %s", addr)

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.WithField("error", err).Errorln("Start connect failed.")
			continue
		}
		log.Infof("Connect to host %s", addr)

		c := pb.NewTransactionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

		prv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			log.WithError(err).Errorln("Generate new key failed.")
			cancel()
			continue
		}

		file, err := os.Open("data.txt")
		if err != nil {
			log.Fatalf("Failed to open data file: %v", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		count := 0
		log.Infof("Start sending transactions.")

		for scanner.Scan() {
			line := scanner.Text()

			if strings.Contains(line, "报文错误：起飞站等于降落站") || !strings.Contains(line, "errMessage") {
				continue
			}

			tx := buildTransaction([]byte(line), prv)
			if tx == nil {
				continue
			}

			//log.Infof("Sending transaction with data: %s", string(tx.Body.Data))

			// 序列化交易
			bytesTransaction, err := utils.SerializeTransaction(tx)
			if err != nil {
				log.WithError(err).Errorln("Build transaction failed.")
				break
			}

			encodedTransaction := hex.EncodeToString(bytesTransaction)
			resp, err := c.SubmitTransaction(ctx, &pb.SubmitTransactionReq{
				SignedTransaction: proto.String(encodedTransaction),
			})
			if err != nil {
				log.WithError(err).Errorln("Signed transaction send failed.")
				break
			}

			if count >= 3000 || resp.GetStatus() == pb.SubmitTransactionStatus_Default {
				conn.Close()
				log.Errorln("Receive code default or upper to limit.")
				break
			}

			count += 1
		}
		cancel()
	}
}

func LoadConfig(filepath string) {
	config.WithOptions(config.ParseEnv)
	config.AddDriver(yaml.Driver)

	err := config.LoadFiles(filepath)
	if err != nil {
		log.WithField("error", err).Errorln("Load config file failed.")
	}
}
