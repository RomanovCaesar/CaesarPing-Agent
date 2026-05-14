package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
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
	Count  int    `json:"count"`
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

	c, resp, err := websocket.DefaultDialer.Dial(urlStr, headers)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial: %w (status: %d)", err, resp.StatusCode)
		}
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close()
	log.Printf("Connected successfully!")

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
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

	countStr := fmt.Sprintf("%d", req.Count)
	if req.Count <= 0 {
		countStr = "4"
	}

	if req.Type == "icmp" {
		cmd := exec.Command("ping", "-c", countStr, req.Target) // Linux ping.
		runCommand(cmd, send)
	} else if req.Type == "mtr" {
		cmd := exec.Command("mtr", "-r", "-c", countStr, req.Target)
		runCommand(cmd, send)
	} else if req.Type == "tcp" {
		cmd := exec.Command("tcping", "-c", countStr, req.Target, fmt.Sprintf("%d", req.Port))
		runCommand(cmd, send)
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
