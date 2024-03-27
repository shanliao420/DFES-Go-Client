package test

import (
	"context"
	"github.com/shanliao420/DFES-Go-Client/client"
	"io"
	"log"
	"os"
	"testing"
)

var registryAddr = ":6001"

func TestPush(t *testing.T) {
	client := client.NewDFESClient(registryAddr)
	file, err := os.Open("/Users/tangyubin/mine/go-project/DFES-Go-Client/data/data.dmg")
	if err != nil {
		log.Fatalln("open file error:", err)
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		log.Fatalln("read file error:", err)
	}
	dataId, err := client.Push(context.Background(), bytes)
	if err != nil {
		log.Fatalln("push file error:", err)
	}
	log.Println("push file successful, data id:", dataId)
}

func TestPushStream(t *testing.T) {
	client := client.NewDFESClient(registryAddr)
	file, err := os.Open("/Users/tangyubin/mine/go-project/DFES-Go-Client/data/data.dmg")
	if err != nil {
		log.Fatalln("open file error:", err)
	}
	dataId, err := client.PushStream(context.Background(), file)
	if err != nil {
		log.Fatalln("push file error:", err)
	}
	log.Println("push file successful, data id:", dataId)
}

func TestGet(t *testing.T) {
	client := client.NewDFESClient(registryAddr)
	dataId := "1772894832084127744"
	bytes, err := client.Get(context.Background(), dataId)
	if err != nil {
		log.Fatalln("get file error:", err)
	}
	log.Println("get data successful")
	err = os.WriteFile("/Users/tangyubin/mine/go-project/DFES-Go-Client/data/get/"+dataId+".dmg", bytes, 0700)
	if err != nil {
		log.Fatalln("open file error:", err)
	}
	log.Println("save data successful")
}

func TestGetStream(t *testing.T) {
	client := client.NewDFESClient(registryAddr)
	dataId := "1772894832084127744"
	reader, err := client.GetStream(context.Background(), dataId)
	if err != nil {
		log.Fatalln("get file error:", err)
	}
	log.Println("get data successful")
	bytes, err := io.ReadAll(reader)
	if err != nil {
		log.Fatalln("read bytes error:", err)
	}
	err = os.WriteFile("/Users/tangyubin/mine/go-project/DFES-Go-Client/data/get/"+dataId+".dmg", bytes, 0700)
	if err != nil {
		log.Fatalln("open file error:", err)
	}
	log.Println("save data successful")
}

func TestDelete(t *testing.T) {
	client := client.NewDFESClient(registryAddr)
	dataId := "1772890395844284416"
	result, err := client.Delete(context.Background(), dataId)
	if err != nil {
		log.Fatalln("delete err:", err)
	}
	log.Println("delete successful:", result)
}
