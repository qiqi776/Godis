package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6378")
	if err != nil {
		fmt.Println("Connect failed:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to Godis. Enter command:")
	reader := bufio.NewReader(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "exit" || text == "quit" {
			break
		}
		if text == "" {
			continue
		}

		conn.Write([]byte(text))
		
		// 简单读取响应
		response, _ := serverReader.ReadString('\n')
		fmt.Print(response)
	}
}