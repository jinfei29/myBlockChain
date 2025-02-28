package main

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"time"
)

// RPC服务注册
func rpcRegister(raft *Raft) {
	err := rpc.Register(raft)
	if err != nil {
		log.Fatalf("RPC服务注册失败: %v", err)
	}
	port := raft.node.Port
	rpc.HandleHTTP()
	err = http.ListenAndServe(port, nil)
	if err != nil {
		log.Fatalf("监听端口失败: %v", err)
	}
}

// 发送消息和命令
func (rf *Raft) broadcast(method string, args interface{}, fun func(ok bool)) {
	for nodeID, port := range nodeTable {
		if nodeID == rf.node.ID {
			continue
		}
		address := "127.0.0.1" + port
		rp, err := rpc.DialHTTP("tcp", address)
		if err != nil {
			fmt.Printf("连接到节点 %s 失败: %v\n", nodeID, err)
			fun(false)
			continue
		}

		var result bool
		err = rp.Call(method, args, &result)
		if err != nil {
			fmt.Printf("调用RPC方法 %s 失败: %v\n", method, err)
			fun(false)
			continue
		}
		fun(result)
		rp.Close()
	}
}

// 投票 
func (rf *Raft) Vote(node NodeInfo, b *bool) error {
	if rf.votedFor != "-1" || rf.currentLeader != "-1" {
		*b = false
	} else {
		rf.setVoteFor(node.ID)
		fmt.Printf("投票成功，已投%s节点\n", node.ID)
		*b = true
	}
	return nil
}

// 确认领导者
func (rf *Raft) ConfirmationLeader(node NodeInfo, b *bool) error {
	rf.setCurrentLeader(node.ID)
	*b = true
	fmt.Println("已发现网络中的领导节点，", node.ID, "成为了领导者！")
	rf.reDefault()
	return nil
}

// 心跳检测回复
func (rf *Raft) HeartbeatRe(node NodeInfo, b *bool) error {
	rf.setCurrentLeader(node.ID)
	rf.lastHeartBeartTime = millisecond()
	fmt.Printf("接收到来自领导节点%s的心跳检测\n", node.ID)
	fmt.Printf("当前时间为:%d\n\n", millisecond())
	*b = true
	return nil
}


// 确认消息
func (rf *Raft) ConfirmationMessage(message Message, b *bool) error {
	go func() {
		for {
			if _, ok := MessageStore[message.MsgID]; ok {
				fmt.Printf("raft验证通过，可以打印消息，id为：%d\n", message.MsgID)
				fmt.Println("消息为：", MessageStore[message.MsgID], "")
				rf.lastMessageTime = millisecond()
				break
			} else {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()
	*b = true
	return nil
}

// 领导者节点接收到转发过来的消息，并生成随机命令
func (rf *Raft) LeaderReceiveMessage(message Message, b *bool) error {
    fmt.Printf("领导者节点接收到转发过来的消息，id为:%d\n", message.MsgID)
	start := time.Now() // 记录日志复制开始时间
    MessageStore[message.MsgID] = message.Msg
    *b = true
    fmt.Println("准备将消息进行广播...")
    num := 0
    // 将 Leader 生成的命令存储在全局变量中
    currentCommand = rf.generateRandomCommand()

    // 广播消息和命令
    go rf.broadcast("Raft.ReceiveMessageAndCommand", MessageAndCommand{Msg: message.Msg, Command: currentCommand, Term: rf.currentTerm}, func(ok bool) {
        if ok {
            num++
        }
    })
	for {
		if num > raftCount/2-1 {
			end := time.Now() // 记录选举结束时间
			duration := end.Sub(start) // 计算选举持续时间
			rf.totalBroadcastTime += duration // 将本次广播的总时间添加到总时间记录变量中
			fmt.Printf("全网已超过半数节点接收到消息id：%d\nraft验证通过，可以打印消息，id为：%d\n", message.MsgID, message.MsgID)
			fmt.Println("消息为：", MessageStore[message.MsgID], "")
			rf.lastMessageTime = millisecond()
			fmt.Println("准备将消息提交信息发送至客户端...，日志复制时间为：%v\n",duration)
			fmt.Printf("本次消息广播总时间为：%v\n", rf.totalBroadcastTime)
			go rf.broadcast("Raft.ConfirmationMessage", message, func(ok bool) {})

			// 模拟命令执行并记录日志
			rf.appendAndApplyLog(currentCommand,message.Msg)
		//	rf.appendAndApplyLog(message.Msg) // 将消息内容也记录到日志中
			rf.applyLogs()

			break
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}
	return nil
}


// 接收来自 Leader 节点的消息和命令
func (rf *Raft) ReceiveMessageAndCommand(msgAndCmd MessageAndCommand, b *bool) error {
	rf.SyncLogs()
    fmt.Printf("节点 %s 接收到消息和命令：\n", rf.me)
    fmt.Printf("消息：%s\n", msgAndCmd.Msg)
    fmt.Printf("命令：%s\n", msgAndCmd.Command)
    fmt.Printf("任期号：%d\n", msgAndCmd.Term)
    *b = true
	rf.currentTerm=msgAndCmd.Term
    // 将消息和命令写入本地日志文件
    rf.appendAndApplyLog(msgAndCmd.Command,msgAndCmd.Msg)  // 使用全局变量 currentCommand 作为命令写入日志
    //rf.appendAndApplyLog(msgAndCmd.Msg)
    rf.applyLogs()
    return nil
}