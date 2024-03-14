package client

import (
	"context"
	"fmt"
	"github.com/shanliao420/DFES/gateway"
	registryPB "github.com/shanliao420/DFES/gateway/proto"
	mateServerPB "github.com/shanliao420/DFES/mate-server/proto"
	"github.com/shanliao420/DFES/utils"
	"io"
	"log"
	"time"
)

type DFESClient struct {
	registerCenter registryPB.RegistryClient
}

func NewDFESClient(registryAddr string) *DFESClient {
	rc := utils.NewRegistryClient(registryAddr)
	return &DFESClient{
		registerCenter: rc,
	}
}

func (dc *DFESClient) Push(ctx context.Context, data []byte) (string, error) {
	getResp, err := dc.registerCenter.GetProvideService(ctx, &registryPB.GetProvideInfo{
		ServiceType: gateway.MateService,
	})
	if err != nil {
		log.Println("get mate provider error:", err)
		return "", err
	}
	mateServerAddr := getResp.GetProvideService().GetServiceAddress().Host + ":" + getResp.GetProvideService().GetServiceAddress().Port
	mateServer := utils.NewMateServerClient(mateServerAddr)
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
			leaderMateServer := utils.NewMateServerClient(pushResp.LeaderMateServerAddr)
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
	providerResp, err := dc.registerCenter.GetProvideService(ctx, &registryPB.GetProvideInfo{
		ServiceType: gateway.MateService,
	})
	if err != nil {
		log.Println("get mate provider error:", err)
		return nil, err
	}
	mateServerAddr := providerResp.GetProvideService().GetServiceAddress().Host + ":" + providerResp.GetProvideService().GetServiceAddress().Port
	mateServer := utils.NewMateServerClient(mateServerAddr)
	getReq := &mateServerPB.GetRequest{
		DataId: id,
	}
	getResp, err := mateServer.Get(ctx, getReq)
	if err != nil {
		log.Println("push data error:", err)
	}
	if getResp.GetResult {
		return getResp.Data, nil
	}
	for i := 0; i < 3; i++ {
		providerResp, err = dc.registerCenter.GetProvideService(ctx, &registryPB.GetProvideInfo{
			ServiceType: gateway.MateService,
		})
		mateServerAddr = providerResp.GetProvideService().GetServiceAddress().Host + ":" + providerResp.GetProvideService().GetServiceAddress().Port
		mateServer = utils.NewMateServerClient(mateServerAddr)
		getResp, err = mateServer.Get(ctx, getReq)
		if getResp.GetResult {
			return getResp.Data, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("retry 3 times in 6s, final error: %v", err)
}

func (dc *DFESClient) Delete(ctx context.Context, id string) (bool, error) {
	getResp, err := dc.registerCenter.GetProvideService(ctx, &registryPB.GetProvideInfo{
		ServiceType: gateway.MateService,
	})
	if err != nil {
		log.Println("get mate provider error:", err)
		return false, err
	}
	mateServerAddr := getResp.GetProvideService().GetServiceAddress().Host + ":" + getResp.GetProvideService().GetServiceAddress().Port
	mateServer := utils.NewMateServerClient(mateServerAddr)
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
	if delResp.Code == mateServerPB.MateCode_NotLeader {
		for i := 0; i < 3; i++ {
			leaderMateServer := utils.NewMateServerClient(delResp.LeaderMateServerAddr)
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
	//TODO implement me
	panic("implement me")
}

func (dc *DFESClient) GetStream(ctx context.Context, id string) (io.Reader, error) {
	//TODO implement me
	panic("implement me")
}
