package proxy

import (
	"net"
	"testing"
	"time"

	"ehang.io/nps/lib/common"
	"ehang.io/nps/lib/conn"
	"ehang.io/nps/lib/file"
)

type udpTestBridge struct {
	target       net.Conn
	selected     *file.Client
	linkGroup    string
	linkClientId int
}

func (b *udpTestBridge) SendLinkInfo(clientId int, group string, link *conn.Link, t *file.Tunnel) (net.Conn, error) {
	b.linkGroup = group
	b.linkClientId = clientId
	return b.target, nil
}

func (b *udpTestBridge) SelectClient(group, target string) (int, error) {
	return 0, nil
}

func (b *udpTestBridge) ResolveClient(group, target string) (*file.Client, error) {
	return b.selected, nil
}

func TestUdpGroupFlowAttributedToSelectedClient(t *testing.T) {
	setupProxyTestConf(t)

	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	clientUDP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer clientUDP.Close()

	serverSide, npcSide := net.Pipe()
	defer serverSide.Close()
	defer npcSide.Close()

	representative := file.NewClient("representative", true, true)
	representative.Id = 1
	selected := file.NewClient("selected", true, true)
	selected.Id = 2
	task := &file.Tunnel{
		Id:          1,
		Client:      representative,
		ClientGroup: "office",
		Flow:        new(file.Flow),
		Target:      &file.Target{TargetStr: "10.0.0.5:53"},
	}
	bridge := &udpTestBridge{target: serverSide, selected: selected}
	server := NewUdpModeServer(nil, task)
	server.bridge = bridge
	server.listener = listener

	payload := []byte("udp-request")
	response := []byte("udp-response")
	npcErr := make(chan error, 1)
	go func() {
		buf := make([]byte, len(payload))
		if _, err := npcSide.Read(buf); err != nil {
			npcErr <- err
			return
		}
		_, err := npcSide.Write(response)
		npcErr <- err
	}()

	buf := common.BufPoolUdp.Get().([]byte)
	copy(buf, payload)
	sess := &udpSession{ready: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		server.runSession(clientUDP.LocalAddr().(*net.UDPAddr), "test-key", sess, buf, len(payload))
		close(done)
	}()

	_ = clientUDP.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(response))
	if _, err := clientUDP.Read(got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(response) {
		t.Fatalf("expected udp response %q, got %q", response, got)
	}

	_ = npcSide.SetReadDeadline(time.Now().Add(time.Second))
	if err := <-npcErr; err != nil {
		t.Fatal(err)
	}
	npcSide.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runSession did not exit after npc connection closed")
	}

	if bridge.linkGroup != "" {
		t.Fatalf("expected no second group selection in SendLinkInfo, got group %q", bridge.linkGroup)
	}
	if bridge.linkClientId != selected.Id {
		t.Fatalf("expected link for selected client %d, got %d", selected.Id, bridge.linkClientId)
	}
	if got := representative.Flow.InletFlow + representative.Flow.ExportFlow; got != 0 {
		t.Fatalf("representative client flow changed: %d", got)
	}
	want := int64(len(payload) + len(response))
	if selected.Flow.InletFlow != want || selected.Flow.ExportFlow != want {
		t.Fatalf("selected client flow = in %d/out %d, want %d/%d", selected.Flow.InletFlow, selected.Flow.ExportFlow, want, want)
	}
	if task.Flow.InletFlow != want || task.Flow.ExportFlow != want {
		t.Fatalf("tunnel flow = in %d/out %d, want %d/%d", task.Flow.InletFlow, task.Flow.ExportFlow, want, want)
	}
}
