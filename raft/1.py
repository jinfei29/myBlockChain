import subprocess
import threading

def start_node(node_name):
    subprocess.Popen([r"D:\Git\gitcode\blockchain_consensus_algorithm\raft\raft.exe", node_name])

nodes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T']

threads = []
for node in nodes:
    thread = threading.Thread(target=start_node, args=(node,))
    thread.start()
    threads.append(thread)

# 等待所有线程完成
for thread in threads:
    thread.join()
