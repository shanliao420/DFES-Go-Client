package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/shanliao420/DFES/gateway"
	registryPB "github.com/shanliao420/DFES/gateway/proto"
	mate_server "github.com/shanliao420/DFES/mate-server"
	mateServerPB "github.com/shanliao420/DFES/mate-server/proto"
	"github.com/shanliao420/DFES/utils"
	"io"
	"log"
	"time"
)

type DFESClient struct {
	registerCenter registryPB.RegistryClient

	actionCache *utils.ActionCache
}

func NewDFESClient(registryAddr string) *DFESClient {
	rc := utils.NewRegistryClient(registryAddr)
	ac := utils.NewActionCache(20)
	ac.RegisterGetFunc(func(key interface{}) interface{} {
		return utils.NewMateServerClient(key.(string))
	})
	return &DFESClient{
		registerCenter: rc,
		actionCache:    ac,
	}
}

func (dc *DFESClient) Push(ctx context.Context, data []byte) (string, error) {
	mateServerAddr, err := dc.getMateServerAddrFromRegistry(ctx)
	if err != nil {
		return "", err
	}
	mateServer := dc.actionCache.Get(mateServerAddr).(mateServerPB.MateServiceClient)
	pushReq := &mateServerPB.PushRequest{
		Data: data,
	}
	pushResp, err := mateServer.Push(ctx, pushReq)
	if err != nil {
		log.Println("push data error:", err)
	}
	if pushResp.PushResult {
		return pushResp.DataId, nil
	}
	if pushResp.Code == mateServerPB.MateCode_NotLeader {
		for i := 0; i < 3; i++ {
			leaderMateServer := dc.actionCache.Get(pushResp.LeaderMateServerAddr).(mateServerPB.MateServiceClient)
			pushResp, err = leaderMateServer.Push(ctx, pushReq)
			if pushResp.PushResult {
				return pushResp.DataId, nil
			}
			time.Sleep(2 * time.Second)
		}
		return "", fmt.Errorf("retry 3 times in 6s, final error : %v", err)
	}
	return "", nil
}

func (dc *DFESClient) Get(ctx context.Context, id string) ([]byte, error) {
	mateServerAddr, err := dc.getMateServerAddrFromRegistry(ctx)
	if err != nil {
		return nil, err
	}
	mateServer := dc.actionCache.Get(mateServerAddr).(mateServerPB.MateServiceClient)
	getReq := &mateServerPB.GetRequest{
		DataId: id,
	}
	isDataExist, err := mateServer.IsDataExists(ctx, getReq)
	if err != nil {
		log.Println("make sure data exists err:", err)
		return nil, err
	}
	if !isDataExist.IsDataExists {
		return nil, errors.New("the data [" + id + "] was already removed ")
	}
	getResp, err := mateServer.Get(ctx, getReq)
	if err != nil {
		log.Println("get data error:", err)
	}
	if getResp.GetResult {
		return getResp.Data, nil
	}
	for i := 0; i < 3; i++ {
		mateServerAddr, _ := dc.getMateServerAddrFromRegistry(ctx)
		mateServer := dc.actionCache.Get(mateServerAddr).(mateServerPB.MateServiceClient)
		getResp, err = mateServer.Get(ctx, getReq)
		if getResp.GetResult {
			return getResp.Data, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("retry 3 times in 6s, final error: %v", err)
}

func (dc *DFESClient) Delete(ctx context.Context, id string) (bool, error) {
	mateServerAddr, err := dc.getMateServerAddrFromRegistry(ctx)
	if err != nil {
		return false, err
	}
	mateServer := dc.actionCache.Get(mateServerAddr).(mateServerPB.MateServiceClient)
	delReq := &mateServerPB.DeleteRequest{
		DataId: id,
	}
	delResp, err := mateServer.Delete(ctx, delReq)
	if err != nil {
		log.Println("push data error:", err)
	}
	if delResp.DeleteResult {
		return delResp.DeleteResult, nil
	}
	if delResp.Code == mateServerPB.MateCode_FileNotExist {
		return true, nil // file already deleted, simply return true
	}
	if delResp.Code == mateServerPB.MateCode_NotLeader {
		for i := 0; i < 3; i++ {
			leaderMateServer := dc.actionCache.Get(delResp.LeaderMateServerAddr).(mateServerPB.MateServiceClient)
			delResp, err = leaderMateServer.Delete(ctx, delReq)
			if delResp.DeleteResult {
				return delResp.DeleteResult, nil
			}
			time.Sleep(2 * time.Second)
		}
		return false, fmt.Errorf("retry 3 times in 6s, final error : %v", err)
	}
	return false, nil
}

func (dc *DFESClient) PushStream(ctx context.Context, stream io.Reader) (string, error) {
	mateServerAddr, err := dc.getMateServerAddrFromRegistry(ctx)
	if err != nil {
		return "", err
	}
	mateServer := dc.actionCache.Get(mateServerAddr).(mateServerPB.MateServiceClient)
	isLeaderResponse, err := mateServer.IsLeader(ctx, nil)
	if err != nil {
		log.Println("make sure leader err:", err)
		return "", err
	}
	if !isLeaderResponse.IsLeader {
		mateServer = dc.actionCache.Get(isLeaderResponse.LeaderMateServerAddr).(mateServerPB.MateServiceClient)
	}
	pushStream, err := mateServer.PushStream(ctx)
	if err != nil {
		log.Println("open push stream channel err:", err)
		return "", err
	}
	for {
		buff := make([]byte, mate_server.DefaultFragmentSize)
		n, err := stream.Read(buff)
		if err == io.EOF {
			log.Println("push stream push all")
			break
		}
		if err != nil {
			log.Println("push stream err:", err)
			return "", err
		}
		err = pushStream.Send(&mateServerPB.PushRequest{
			Data: buff[:n],
		})
		if err != nil {
			log.Println("push stream to rpc err:", err)
			return "", err
		}
	}
	pushResponse, err := pushStream.CloseAndRecv()
	if err != nil {
		log.Println("close push stream to rpc err:", err)
		return "", err
	}
	return pushResponse.DataId, nil
}

func (dc *DFESClient) GetStream(ctx context.Context, id string) (io.Reader, error) {
	mateServerAddr, err := dc.getMateServerAddrFromRegistry(ctx)
	if err != nil {
		return nil, err
	}
	mateServer := dc.actionCache.Get(mateServerAddr).(mateServerPB.MateServiceClient)
	getRequest := &mateServerPB.GetRequest{
		DataId: id,
	}
	isDataExist, err := mateServer.IsDataExists(ctx, getRequest)
	if err != nil {
		log.Println("make sure data exists err:", err)
		return nil, err
	}
	if !isDataExist.IsDataExists {
		return nil, errors.New("the data [" + id + "] was already removed ")
	}
	getStream, err := mateServer.GetStream(ctx, getRequest)
	if err != nil {
		log.Println("open get stream rpc channel err:", err)
		return nil, err
	}
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		for {
			data, err := getStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println("get stream from rpc channel err:", err)
				return
			}
			_, _ = w.Write(data.Data)
		}
	}()
	return r, nil
}

func (dc *DFESClient) getMateServerAddrFromRegistry(ctx context.Context) (string, error) {
	getResp, err := dc.registerCenter.GetProvideService(ctx, &registryPB.GetProvideInfo{
		ServiceType: gateway.MateService,
	})
	if err != nil {
		log.Println("get mate provider error:", err)
		return "", err
	}
	mateServerAddr := getResp.GetProvideService().GetServiceAddress().Host + ":" + getResp.GetProvideService().GetServiceAddress().Port
	return mateServerAddr, nil
}
