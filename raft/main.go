package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

// 定义节点池
var nodeTable map[string]string

// 选举超时时间（单位：秒）
var timeout = 3

// 心跳检测超时时间
var heartBeatTimeout = 7

// 心跳检测频率（单位：秒）
var heartBeatTimes = 3
var currentCommand string
// 用于存储消息
var MessageStore = make(map[int]string)

// 动态计算节点数量
var raftCount int

func main() {
	// 定义十个节点  节点编号 - 监听端口号
	nodeTable = map[string]string{
		"A": ":9000",
		"B": ":9001",
		"C": ":9002",
		"D": ":9003",
		"E": ":9004",
		"F": ":9005",
		"G": ":9006",
		"H": ":9007",
		"I": ":9008",
		"J": ":9009",
	  }
	  
	// 运行程序时候 指定节点编号
	if len(os.Args) < 2 {
		log.Fatal("程序参数不正确")
	}
	id := os.Args[1]

	// 动态计算实际运行的节点数量
	raftCount = len(nodeTable)

	// 传入节点编号，端口号，创建raft实例
	raft := NewRaft(id, nodeTable[id])
	// 启用RPC,注册raft
	go rpcRegister(raft)
	// 启动 applyLogsFromLeader 函数，处理来自 Leader 节点的日志条目

	// 开启心跳检测
	go raft.heartbeat()
	// 开启一个Http监听
	if id == "A" {
		go raft.httpListen()
	}
	Circle:
	// 开启选举
	go func() {
		for {
			// 成为候选人节点
			if raft.becomeCandidate() {
				// 成为候选人节点后 向其他节点要选票来进行选举
				if raft.election() {
					break
				} else {
					continue
				}
			} else {
				break
			}
		}
	}()
	// 进行心跳检测
	for {
		// 0.5秒检测一次
		time.Sleep(time.Millisecond * 5000)
		if raft.lastHeartBeartTime != 0 && (millisecond()-raft.lastHeartBeartTime) > int64(raft.timeout*1000) {
			fmt.Printf("心跳检测超时，已超过%d秒\n", raft.timeout)
			fmt.Println("即将重新开启选举")
			raft.reDefault()
			raft.setCurrentLeader("-1")
			raft.lastHeartBeartTime = 0
			goto Circle
		}
	}
}