package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os/exec"
	"time"

	"github.com/gorilla/websocket"
)

type TaskRequest struct {
	Action string `json:"action"`
	TaskId string `json:"taskId"`
	Target string `json:"target"`
	Type   string `json:"type"` // icmp, tcp, mtr
	Port   int    `json:"port"`
}

type TaskResponse struct {
	TaskId string `json:"taskId"`
	Status string `json:"status"` // running, completed, error
	Output string `json:"output"`
}

var serverUrl = flag.String("server", "ws://localhost:3000/ws/agent", "Backend WebSocket URL")
var token = flag.String("token", "", "Agent Authentication Token")

func main() {
	flag.Parse()
	if *token == "" {
		log.Fatal("Token is required")
	}

	u, err := url.Parse(*serverUrl)
	if err != nil {
		log.Fatal("Invalid server URL:", err)
	}

	q := u.Query()
	q.Set("token", *token)
	u.RawQuery = q.Encode()

	log.Printf("Connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			return
		}

		var req TaskRequest
		if err := json.Unmarshal(message, &req); err != nil {
			log.Println("unmarshal error:", err)
			continue
		}

		if req.Action == "start_test" {
			go handleTask(c, req)
		}
	}
}

func handleTask(c *websocket.Conn, req TaskRequest) {
	send := func(status, output string) {
		resp := TaskResponse{
			TaskId: req.TaskId,
			Status: status,
			Output: output,
		}
		c.WriteJSON(resp)
	}

	if req.Type == "icmp" {
		cmd := exec.Command("ping", "-c", "4", req.Target) // Linux ping.
		runCommand(cmd, send)
	} else if req.Type == "mtr" {
		cmd := exec.Command("mtr", "-r", "-c", "10", req.Target)
		runCommand(cmd, send)
	} else if req.Type == "tcp" {
		runTcpPing(req.Target, req.Port, send)
	}
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

func runTcpPing(target string, port int, send func(string, string)) {
	addr := fmt.Sprintf("%s:%d", target, port)
	send("running", fmt.Sprintf("TCP Ping to %s", addr))
	
	for i := 1; i <= 4; i++ {
		start := time.Now()
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		duration := time.Since(start)
		
		if err != nil {
			send("running", fmt.Sprintf("Seq %d: Connection failed: %v", i, err))
		} else {
			conn.Close()
			send("running", fmt.Sprintf("Seq %d: Connected in %v", i, duration))
		}
		time.Sleep(1 * time.Second)
	}
	send("completed", "\nTCP Ping completed.")
}
