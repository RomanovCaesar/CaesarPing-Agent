package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type TaskRequest struct {
	Action string `json:"action"`
	TaskId string `json:"taskId"`
	Target string `json:"target"`
	Type   string `json:"type"` // icmp, tcp, mtr
	Port   int    `json:"port"`
	Count  int    `json:"count"`
	Family string `json:"family"` // "4", "6" or "" (prefer IPv4)
}

type TaskResponse struct {
	TaskId string `json:"taskId"`
	Status string `json:"status"` // running, completed, error
	Output string `json:"output"`
}

var serverUrl = flag.String("server", "ws://localhost:3000/ws/agent", "Backend WebSocket URL")
var token = flag.String("token", "", "Agent Authentication Token (or set CAESARPING_TOKEN, which keeps it out of the process list)")
var bindIp = flag.String("bind", "auto", "Local IP to bind to (e.g., 143.14.86.197), or 'auto' to auto-detect primary physical interface")

var resolvedBindIp string // used for the connection to the server

// Source addresses for tests, per family; a test only binds to the one matching its target
var bindIPv4, bindIPv6 string

func main() {
	flag.Parse()
	if *token == "" {
		*token = os.Getenv("CAESARPING_TOKEN")
	}
	if *token == "" {
		log.Fatal("Token is required (-token or CAESARPING_TOKEN)")
	}

	u, err := url.Parse(*serverUrl)
	if err != nil {
		log.Fatal("Invalid server URL:", err)
	}

	if *bindIp == "auto" {
		bindIPv4 = getPrimaryIP(false)
		bindIPv6 = getPrimaryIP(true)
		resolvedBindIp = bindIPv4
		if bindIPv4 != "" || bindIPv6 != "" {
			log.Printf("Auto-detected primary physical interface IPs: IPv4=%q IPv6=%q", bindIPv4, bindIPv6)
		} else {
			log.Printf("Could not auto-detect primary physical interface. Proceeding with default routing.")
		}
	} else if *bindIp != "" {
		resolvedBindIp = *bindIp
		if addr, err := netip.ParseAddr(*bindIp); err == nil && addr.Unmap().Is4() {
			bindIPv4 = addr.Unmap().String()
		} else if err == nil {
			bindIPv6 = addr.String()
		}
		log.Printf("Using explicitly bound IP: %s", resolvedBindIp)
	}

	for {
		err := connectAndListen(u.String())
		if err != nil {
			log.Printf("Connection error: %v. Reconnecting in 5 seconds...", err)
		} else {
			log.Printf("Disconnected. Reconnecting in 5 seconds...")
		}
		time.Sleep(5 * time.Second)
	}
}

func connectAndListen(urlStr string) error {
	log.Printf("Connecting to %s", urlStr)

	headers := http.Header{}
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	// Sent as a header rather than in the URL, so it does not end up in proxy access logs
	headers.Set("Authorization", "Bearer "+*token)

	dialer := websocket.DefaultDialer
	if resolvedBindIp != "" {
		localAddr, err := net.ResolveTCPAddr("tcp", resolvedBindIp+":0")
		if err == nil {
			dialer = &websocket.Dialer{
				Proxy:            http.ProxyFromEnvironment,
				HandshakeTimeout: 45 * time.Second,
				NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					d := &net.Dialer{LocalAddr: localAddr}
					return d.DialContext(ctx, network, addr)
				},
			}
		} else {
			log.Printf("Invalid bind IP %s: %v", resolvedBindIp, err)
		}
	}

	c, resp, err := dialer.Dial(urlStr, headers)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial: %w (status: %d)", err, resp.StatusCode)
		}
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close()
	log.Printf("Connected successfully!")
	conn := &safeConn{c: c}

	// The server pings every 30s. Without a read deadline a half-open connection
	// (e.g. after a network outage) would block ReadMessage forever and never reconnect.
	c.SetReadDeadline(time.Now().Add(readTimeout))
	c.SetPingHandler(func(data string) error {
		c.SetReadDeadline(time.Now().Add(readTimeout))
		return c.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(10*time.Second))
	})

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}
		c.SetReadDeadline(time.Now().Add(readTimeout))

		var req TaskRequest
		if err := json.Unmarshal(message, &req); err != nil {
			log.Println("unmarshal error:", err)
			continue
		}

		if req.Action == "start_test" {
			go handleTask(conn, req)
		}
	}
}

// safeConn serializes writes: gorilla/websocket allows only one concurrent writer,
// and output from several tasks (and stdout/stderr of one task) arrives in parallel.
type safeConn struct {
	c  *websocket.Conn
	mu sync.Mutex
}

func (s *safeConn) WriteJSON(v interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.c.WriteJSON(v)
}

// Limits how many ping/mtr/tcping processes run at once; further tasks wait.
const maxConcurrentTasks = 8

var taskSlots = make(chan struct{}, maxConcurrentTasks)

func handleTask(c *safeConn, req TaskRequest) {
	send := func(status, output string) {
		resp := TaskResponse{
			TaskId: req.TaskId,
			Status: status,
			Output: output,
		}
		c.WriteJSON(resp)
	}

	// Reject anything that ping/mtr/tcping could parse as an option
	if !isValidTarget(req.Target) {
		send("error", "Invalid target")
		return
	}

	taskSlots <- struct{}{}
	defer func() { <-taskSlots }()

	// Resolve here and run the tools against the checked address, so a DNS answer
	// cannot change between the check and the test
	addr, err := resolvePublicTarget(req.Target, req.Family)
	if err != nil {
		send("error", err.Error())
		return
	}
	target := addr.String()

	// Bind only to a source address of the same family as the target; binding an
	// IPv4 source to an IPv6 target makes ping/mtr fail
	bindAddr := bindIPv4
	if addr.Is6() {
		bindAddr = bindIPv6
	}

	count := req.Count
	if count <= 0 {
		count = 4
	} else if count > maxCount {
		count = maxCount
	}
	countStr := fmt.Sprintf("%d", count)

	// Upper bound on a test's run time, so a hung tool does not hold a task slot forever
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second+time.Duration(count)*3*time.Second)
	defer cancel()

	if req.Type == "icmp" {
		args := []string{"-c", countStr}
		if bindAddr != "" {
			args = append(args, "-I", bindAddr)
		}
		args = append(args, target)
		cmd := exec.CommandContext(ctx, "ping", args...)
		runCommand(cmd, send)
	} else if req.Type == "mtr" {
		args := []string{"-r", "-c", countStr}
		if bindAddr != "" {
			args = append(args, "-a", bindAddr)
		}
		args = append(args, target)
		cmd := exec.CommandContext(ctx, "mtr", args...)
		runCommand(cmd, send)
	} else if req.Type == "tcp" {
		if req.Port < 1 || req.Port > 65535 {
			send("error", "Invalid port")
			return
		}
		cmd := exec.CommandContext(ctx, "tcping", "-c", countStr, target, fmt.Sprintf("%d", req.Port))
		runCommand(cmd, send)
	} else {
		send("error", "Unsupported test type")
	}
}

const readTimeout = 90 * time.Second

const maxCount = 20

var hostnameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.?$`)

func isValidTarget(target string) bool {
	if net.ParseIP(target) != nil {
		return true
	}
	return len(target) <= 253 && hostnameRe.MatchString(target)
}

// Loopback, private, link-local, CGNAT, documentation, multicast and similar ranges
var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

func isPublicAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	for _, p := range nonPublicPrefixes {
		if p.Contains(addr) {
			return false
		}
	}
	return addr.IsValid()
}

// resolvePublicTarget returns the address to test: of the requested family ("4" or
// "6"), or preferring IPv4 when family is empty. It refuses targets where any
// resolved address is not public.
func resolvePublicTarget(target, family string) (netip.Addr, error) {
	if addr, err := netip.ParseAddr(target); err == nil {
		addr = addr.Unmap()
		if !isPublicAddr(addr) {
			return netip.Addr{}, fmt.Errorf("Private, loopback and reserved addresses cannot be tested.")
		}
		if (family == "4" && !addr.Is4()) || (family == "6" && !addr.Is6()) {
			return netip.Addr{}, fmt.Errorf("%s is not an IPv%s address", target, family)
		}
		return addr, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", target)
	if err != nil || len(addrs) == 0 {
		return netip.Addr{}, fmt.Errorf("Failed to resolve %s", target)
	}
	var chosen netip.Addr
	for _, a := range addrs {
		a = a.Unmap()
		if !isPublicAddr(a) {
			return netip.Addr{}, fmt.Errorf("Private, loopback and reserved addresses cannot be tested.")
		}
		if (family == "4" && !a.Is4()) || (family == "6" && !a.Is6()) {
			continue
		}
		if !chosen.IsValid() || (a.Is4() && !chosen.Is4()) {
			chosen = a
		}
	}
	if !chosen.IsValid() {
		return netip.Addr{}, fmt.Errorf("%s has no IPv%s address", target, family)
	}
	return chosen, nil
}

// getPrimaryIP returns the first public address of the wanted family on a physical interface.
func getPrimaryIP(wantIPv6 bool) string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagPointToPoint != 0 {
			continue
		}
		name := iface.Name
		if strings.HasPrefix(name, "lo") || strings.HasPrefix(name, "tun") || strings.HasPrefix(name, "tap") ||
			strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "br-") || strings.HasPrefix(name, "veth") ||
			strings.HasPrefix(name, "wg") || strings.HasPrefix(name, "ppp") {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || (ip.To4() == nil) != wantIPv6 {
				continue
			}
			if a, ok := netip.AddrFromSlice(ip); ok && isPublicAddr(a) {
				return a.Unmap().String()
			}
		}
	}
	return ""
}

func runCommand(cmd *exec.Cmd, send func(string, string)) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		send("error", err.Error())
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		send("error", err.Error())
		return
	}

	if err := cmd.Start(); err != nil {
		send("error", err.Error())
		return
	}

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			send("running", scanner.Text())
		}
	}()

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		send("running", scanner.Text())
	}

	if err := cmd.Wait(); err != nil {
		send("completed", fmt.Sprintf("\nProcess exited with error: %v", err))
	} else {
		send("completed", "\nTest completed.")
	}
}
