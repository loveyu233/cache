// Package cache 提供被其他节点访问的能力
package cache

import (
	"cache/consistenthash"
	"cache/pb"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	defaultBasePath = "/_gocache/"
	defaultReplicas = 50
)

type HTTPPool struct {
	self     string
	basePath string
	mu       sync.Mutex
	peers    *consistenthash.Map
	// 映射远程节点与对应的 httpGetter。每一个远程节点对应一个 httpGetter，因为 httpGetter 与远程节点的地址 baseURL 有关
	httpGetters map[string]*httpGetter
}

func NewHTTPPool(self string, basePath ...string) *HTTPPool {
	httpPool := new(HTTPPool)
	httpPool.self = self
	if len(basePath) == 0 || basePath == nil {
		httpPool.basePath = defaultBasePath
	} else {
		httpPool.basePath = basePath[0]
	}
	return httpPool
}

func (h *HTTPPool) Log(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}

func (h *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 判断访问路径的前缀是否是 basePath
	if !strings.HasPrefix(r.URL.Path, h.basePath) {
		panic("错误路径: " + r.URL.Path)
	}
	h.Log("%s %s", r.Method, r.URL.Path)
	// 约定访问路径格式为 /<basepath>/<groupname>/<key>
	parts := strings.SplitN(r.URL.Path[len(h.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "错误请求", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "没有该group: "+groupName, http.StatusNotFound)
		return
	}
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	// 将缓存值作为 httpResponse 的 body 返回
	w.Write(body)
}

type httpGetter struct {
	bashUrl string
}

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	url := fmt.Sprintf("%v/%v/%v", h.bashUrl, url.QueryEscape(in.GetGroup()), url.QueryEscape(in.GetKey()))
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("服务请求返回异常:%s", resp.Status)
	}
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("服务请求返回值异常:%s", err.Error())
	}
	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("proto解码错误: %v", err)
	}
	return nil
}

// Set 实例化了一致性哈希算法，并且添加了传入的节点
func (h *HTTPPool) Set(peers ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.peers = consistenthash.NewMap(defaultReplicas)
	h.peers.Add(peers...)
	h.httpGetters = make(map[string]*httpGetter)
	// 每一个节点创建了一个 HTTP 客户端 httpGetter
	for _, peer := range peers {
		h.httpGetters[peer] = &httpGetter{bashUrl: peer + h.basePath}
	}
}

// PickPeer 根据具体的 key，选择节点，返回节点对应的 HTTP 客户端
func (h *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if peer := h.peers.Get(key); peer != "" && peer != h.self {
		h.Log("Pick peer %s", peer)
		return h.httpGetters[peer], true
	}
	return nil, false
}
