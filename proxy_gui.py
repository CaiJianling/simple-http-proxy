#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
图形界面网络代理工具
支持HTTP/HTTPS和SOCKS5协议
包含实时监控、日志查看、流量统计等功能
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import socket
import select
import time
import threading
import queue
import json
import os
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib.animation as animation

class ProxyServer:
    def __init__(self):
        self.server_socket = None
        self.running = False
        self.clients = {}
        self.buffer_size = 1024
        self.delay = 1
        self.host = '0.0.0.0'
        self.port = 7890
        self.max_clients = 100
        self.stats = {
            'total_connections': 0,
            'active_connections': 0,
            'total_bytes_sent': 0,
            'total_bytes_received': 0,
            'start_time': None
        }
        self.connection_history = []
        self.traffic_data = {'time': [], 'sent': [], 'received': []}
        
        # SOCKS5 常量定义
        self.SOCKS5_VERSION = 0x05
        self.SOCKS5_AUTH_NONE = 0x00
        self.SOCKS5_AUTH_PASSWORD = 0x02
        self.SOCKS5_AUTH_NO_ACCEPTABLE = 0xFF
        self.SOCKS5_CMD_CONNECT = 0x01
        self.SOCKS5_ATYP_IPV4 = 0x01
        self.SOCKS5_ATYP_DOMAIN = 0x03
        self.SOCKS5_ATYP_IPV6 = 0x04
        self.SOCKS5_REP_SUCCESS = 0x00
        self.SOCKS5_REP_GENERAL_FAILURE = 0x01
        self.SOCKS5_REP_CONNECTION_REFUSED = 0x05
        self.SOCKS5_REP_COMMAND_NOT_SUPPORTED = 0x07
        self.SOCKS5_REP_ADDRESS_TYPE_NOT_SUPPORTED = 0x08
        
    def start(self, host, port, buffer_size, delay, max_clients):
        try:
            self.host = host
            self.port = port
            self.buffer_size = buffer_size
            self.delay = delay
            self.max_clients = max_clients
            
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(self.max_clients)
            self.server_socket.setblocking(False)
            
            self.running = True
            self.stats['start_time'] = datetime.now()
            return True
        except Exception as e:
            return False
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None
    
    def handle_client(self, client_socket, client_address):
        try:
            self.stats['total_connections'] += 1
            self.stats['active_connections'] += 1
            
            # 首先接收客户端数据来判断协议类型
            client_socket.settimeout(5.0)  # 设置超时时间
            try:
                first_data = client_socket.recv(self.buffer_size)
                if not first_data:
                    return
                
                self.stats['total_bytes_received'] += len(first_data)
                
                # 判断协议类型
                if self.is_socks5_request(first_data):
                    # 处理 SOCKS5 请求
                    self.clients[client_address]['protocol'] = 'SOCKS5'
                    self.handle_socks5_client(client_socket, client_address, first_data)
                elif self.is_http_request(first_data):
                    # 处理 HTTP 请求
                    self.clients[client_address]['protocol'] = 'HTTP'
                    self.handle_http_client(client_socket, client_address, first_data)
                else:
                    # 未知协议，关闭连接
                    self.log_message(f"Unknown protocol from {client_address}")
                    
            except socket.timeout:
                self.log_message(f"Connection timeout from {client_address}")
                
        except Exception as e:
            self.log_message(f"Error handling client {client_address}: {str(e)}")
        finally:
            client_socket.close()
            self.stats['active_connections'] -= 1
            if client_address in self.clients:
                del self.clients[client_address]
    
    def handle_http_client(self, client_socket, client_address, first_data):
        """处理HTTP客户端请求"""
        try:
            # 处理第一个HTTP请求
            response = self.handle_http_request(first_data)
            if response:
                client_socket.send(response)
                self.stats['total_bytes_sent'] += len(response)
            
            # 继续处理后续请求
            while self.running:
                try:
                    data = client_socket.recv(self.buffer_size)
                    if not data:
                        break
                    
                    self.stats['total_bytes_received'] += len(data)
                    
                    response = self.handle_http_request(data)
                    if response:
                        client_socket.send(response)
                        self.stats['total_bytes_sent'] += len(response)
                    
                    if self.delay > 0:
                        time.sleep(self.delay / 1000.0)
                        
                except socket.error:
                    break
                    
        except Exception as e:
            self.log_message(f"HTTP client error {client_address}: {str(e)}")
    
    def handle_socks5_client(self, client_socket, client_address, first_data):
        """处理SOCKS5客户端请求"""
        try:
            # SOCKS5 握手阶段
            if not self.handle_socks5_handshake(client_socket, first_data):
                return
            
            # SOCKS5 连接请求阶段
            target_socket = self.handle_socks5_connect_request(client_socket)
            if not target_socket:
                return
            
            # 数据转发阶段
            self.socks5_relay_data(client_socket, target_socket)
            
        except Exception as e:
            self.log_message(f"SOCKS5 client error {client_address}: {str(e)}")
        finally:
            if 'target_socket' in locals() and target_socket:
                target_socket.close()
    
    def is_http_request(self, data):
        try:
            lines = data.split(b'\r\n')
            if len(lines) > 0:
                parts = lines[0].split(b' ')
                return len(parts) >= 3 and parts[2].startswith(b'HTTP')
        except:
            pass
        return False
    
    def is_socks5_request(self, data):
        """判断是否为SOCKS5请求"""
        try:
            if len(data) < 3:
                return False
            # SOCKS5 握手格式: [VER=5, NMETHODS, METHODS]
            return data[0] == self.SOCKS5_VERSION and len(data) == data[1] + 2
        except:
            pass
        return False
    
    def handle_socks5_handshake(self, client_socket, first_data):
        """处理SOCKS5握手阶段"""
        try:
            # 解析客户端握手请求
            if len(first_data) < 3 or first_data[0] != self.SOCKS5_VERSION:
                return False
            
            nmethods = first_data[1]
            methods = list(first_data[2:2+nmethods])
            
            # 选择无认证方式（优先选择）
            if self.SOCKS5_AUTH_NONE in methods:
                response = bytes([self.SOCKS5_VERSION, self.SOCKS5_AUTH_NONE])
                client_socket.send(response)
                self.stats['total_bytes_sent'] += len(response)
                self.log_message("SOCKS5 handshake successful - no authentication required")
                return True
            else:
                # 客户端不支持无认证，拒绝连接
                response = bytes([self.SOCKS5_VERSION, self.SOCKS5_AUTH_NO_ACCEPTABLE])
                client_socket.send(response)
                self.stats['total_bytes_sent'] += len(response)
                self.log_message("SOCKS5 handshake failed - no acceptable authentication method")
                return False
                
        except Exception as e:
            self.log_message(f"SOCKS5 handshake error: {str(e)}")
            return False
    
    def handle_socks5_connect_request(self, client_socket):
        """处理SOCKS5连接请求"""
        try:
            # 接收连接请求
            data = client_socket.recv(self.buffer_size)
            if not data or len(data) < 10:
                return None
            
            self.stats['total_bytes_received'] += len(data)
            
            # 解析SOCKS5连接请求
            if data[0] != self.SOCKS5_VERSION or data[1] != self.SOCKS5_CMD_CONNECT:
                # 不支持命令类型
                self.send_socks5_reply(client_socket, self.SOCKS5_REP_COMMAND_NOT_SUPPORTED)
                return None
            
            # 解析目标地址
            atyp = data[3]
            if atyp == self.SOCKS5_ATYP_IPV4:
                # IPv4地址
                if len(data) < 10:
                    return None
                dst_addr = socket.inet_ntoa(data[4:8])
                dst_port = int.from_bytes(data[8:10], 'big')
                header_len = 10
            elif atyp == self.SOCKS5_ATYP_DOMAIN:
                # 域名
                domain_len = data[4]
                if len(data) < 7 + domain_len:
                    return None
                dst_addr = data[5:5+domain_len].decode('utf-8')
                dst_port = int.from_bytes(data[5+domain_len:7+domain_len], 'big')
                header_len = 7 + domain_len
            else:
                # 不支持的地址类型
                self.send_socks5_reply(client_socket, self.SOCKS5_REP_ADDRESS_TYPE_NOT_SUPPORTED)
                return None
            
            # 尝试连接目标服务器
            try:
                target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                target_socket.settimeout(10)
                target_socket.connect((dst_addr, dst_port))
                
                # 发送成功响应
                self.send_socks5_reply(client_socket, self.SOCKS5_REP_SUCCESS)
                
                self.log_message(f"SOCKS5 connected to {dst_addr}:{dst_port}")
                return target_socket
                
            except Exception as e:
                # 连接失败
                self.send_socks5_reply(client_socket, self.SOCKS5_REP_CONNECTION_REFUSED)
                self.log_message(f"SOCKS5 connection failed to {dst_addr}:{dst_port}: {str(e)}")
                return None
                
        except Exception as e:
            self.log_message(f"SOCKS5 connect request error: {str(e)}")
            return None
    
    def send_socks5_reply(self, client_socket, reply_code):
        """发送SOCKS5响应"""
        try:
            # SOCKS5 响应格式: [VER=5, REP, RSV=0, ATYP=1, BND.ADDR=4字节, BND.PORT=2字节]
            response = bytes([
                self.SOCKS5_VERSION,
                reply_code,
                0x00,  # RSV
                self.SOCKS5_ATYP_IPV4,
                0x00, 0x00, 0x00, 0x00,  # BND.ADDR (0.0.0.0)
                0x00, 0x00  # BND.PORT (0)
            ])
            client_socket.send(response)
            self.stats['total_bytes_sent'] += len(response)
        except Exception as e:
            self.log_message(f"SOCKS5 reply error: {str(e)}")
    
    def socks5_relay_data(self, client_socket, target_socket):
        """SOCKS5数据转发"""
        try:
            # 设置为非阻塞模式
            client_socket.setblocking(False)
            target_socket.setblocking(False)
            
            while self.running:
                try:
                    # 使用 select 进行多路复用
                    readable, _, _ = select.select([client_socket, target_socket], [], [], 1.0)
                    
                    # 客户端到目标服务器
                    if client_socket in readable:
                        data = client_socket.recv(self.buffer_size)
                        if not data:
                            break
                        self.stats['total_bytes_received'] += len(data)
                        target_socket.send(data)
                        self.stats['total_bytes_sent'] += len(data)
                    
                    # 目标服务器到客户端
                    if target_socket in readable:
                        data = target_socket.recv(self.buffer_size)
                        if not data:
                            break
                        self.stats['total_bytes_received'] += len(data)
                        client_socket.send(data)
                        self.stats['total_bytes_sent'] += len(data)
                    
                    if self.delay > 0:
                        time.sleep(self.delay / 1000.0)
                        
                except socket.error:
                    break
                    
        except Exception as e:
            self.log_message(f"SOCKS5 relay error: {str(e)}")
    
    def log_message(self, message):
        """记录日志消息到GUI界面"""
        # 这里假设有一个日志队列可用，如果没有则使用print
        try:
            if hasattr(self, 'log_queue'):
                self.log_queue.put({
                    'type': 'info',
                    'message': f"[SOCKS5] {message}",
                    'timestamp': datetime.now()
                })
            else:
                print(f"[SOCKS5] {message}")
        except:
            print(f"[SOCKS5] {message}")
    
    def handle_http_request(self, data):
        try:
            # 简单的HTTP响应
            response = b"HTTP/1.1 200 OK\r\n"
            response += b"Content-Type: text/html\r\n"
            response += b"Connection: close\r\n"
            response += b"\r\n"
            response += b"<html><body><h1>Proxy Server Response</h1></body></html>"
            return response
        except Exception as e:
            return None
    
    def run(self, log_queue, stats_queue):
        # 保存日志队列引用
        self.log_queue = log_queue
        
        while self.running:
            try:
                readable, _, _ = select.select([self.server_socket], [], [], 1.0)
                
                if self.server_socket in readable:
                    client_socket, client_address = self.server_socket.accept()
                    client_socket.setblocking(False)
                    
                    self.clients[client_address] = {
                        'socket': client_socket,
                        'connect_time': datetime.now(),
                        'bytes_sent': 0,
                        'bytes_received': 0,
                        'protocol': 'Unknown'
                    }
                    
                    # 记录连接日志
                    log_queue.put({
                        'type': 'connection',
                        'message': f"New connection from {client_address[0]}:{client_address[1]}",
                        'timestamp': datetime.now()
                    })
                    
                    # 启动客户端处理线程
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                
                # 更新统计信息
                stats_queue.put(self.stats.copy())
                
            except Exception as e:
                if self.running:
                    log_queue.put({
                        'type': 'error',
                        'message': f"Server error: {str(e)}",
                        'timestamp': datetime.now()
                    })

class ProxyGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("网络代理服务器")
        self.root.geometry("1200x800")
        
        # 设置样式
        self.setup_styles()
        
        # 代理服务器实例
        self.proxy_server = ProxyServer()
        self.server_thread = None
        
        # 队列用于线程间通信
        self.log_queue = queue.Queue()
        self.stats_queue = queue.Queue()
        
        # 创建界面
        self.create_widgets()
        
        # 启动更新定时器
        self.update_gui()
        
    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        
        # 配置颜色
        self.colors = {
            'bg': '#f0f0f0',
            'fg': '#333333',
            'accent': '#007acc',
            'success': '#28a745',
            'danger': '#dc3545',
            'warning': '#ffc107'
        }
        
    def create_widgets(self):
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=2)
        main_frame.rowconfigure(2, weight=1)
        
        # 控制面板
        self.create_control_panel(main_frame)
        
        # 状态面板
        self.create_status_panel(main_frame)
        
        # 日志面板
        self.create_log_panel(main_frame)
        
        # 连接列表面板
        self.create_connections_panel(main_frame)
        
        # 流量图表面板
        self.create_traffic_chart(main_frame)
        
    def create_control_panel(self, parent):
        control_frame = ttk.LabelFrame(parent, text="服务器控制", padding="10")
        control_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # 服务器设置
        settings_frame = ttk.Frame(control_frame)
        settings_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 主机地址
        ttk.Label(settings_frame, text="主机地址:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.host_var = tk.StringVar(value="0.0.0.0")
        self.host_entry = ttk.Entry(settings_frame, textvariable=self.host_var, width=15)
        self.host_entry.grid(row=0, column=1, padx=(0, 10))
        
        # 端口
        ttk.Label(settings_frame, text="端口:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        self.port_var = tk.StringVar(value="7890")
        self.port_entry = ttk.Entry(settings_frame, textvariable=self.port_var, width=8)
        self.port_entry.grid(row=0, column=3, padx=(0, 10))
        
        # 客户端数量
        ttk.Label(settings_frame, text="最大客户端:").grid(row=0, column=4, sticky=tk.W, padx=(0, 5))
        self.max_clients_var = tk.StringVar(value="100")
        self.max_clients_entry = ttk.Entry(settings_frame, textvariable=self.max_clients_var, width=8)
        self.max_clients_entry.grid(row=0, column=5, padx=(0, 10))
        
        # 缓冲区大小
        ttk.Label(settings_frame, text="缓冲区大小:").grid(row=1, column=0, sticky=tk.W, padx=(0, 5), pady=(5, 0))
        self.buffer_var = tk.StringVar(value="1024")
        self.buffer_entry = ttk.Entry(settings_frame, textvariable=self.buffer_var, width=15)
        self.buffer_entry.grid(row=1, column=1, padx=(0, 10), pady=(5, 0))
        
        # 转发延迟
        ttk.Label(settings_frame, text="转发延迟(ms):").grid(row=1, column=2, sticky=tk.W, padx=(0, 5), pady=(5, 0))
        self.delay_var = tk.StringVar(value="1")
        self.delay_entry = ttk.Entry(settings_frame, textvariable=self.delay_var, width=8)
        self.delay_entry.grid(row=1, column=3, padx=(0, 10), pady=(5, 0))
        
        # 控制按钮
        button_frame = ttk.Frame(control_frame)
        button_frame.pack(fill=tk.X)
        
        self.start_button = ttk.Button(
            button_frame, 
            text="▶ 启动服务器", 
            command=self.start_server,
            style='Success.TButton'
        )
        self.start_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.stop_button = ttk.Button(
            button_frame, 
            text="⏹ 停止服务器", 
            command=self.stop_server,
            state=tk.DISABLED,
            style='Danger.TButton'
        )
        self.stop_button.pack(side=tk.LEFT)
        
        # 创建按钮样式
        style = ttk.Style()
        style.configure('Success.TButton', foreground=self.colors['success'])
        style.configure('Danger.TButton', foreground=self.colors['danger'])
        
    def create_status_panel(self, parent):
        status_frame = ttk.LabelFrame(parent, text="服务器状态", padding="10")
        status_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10), pady=(0, 10))
        
        # 状态指示器
        self.status_label = ttk.Label(
            status_frame, 
            text="● 已停止",
            foreground=self.colors['danger'],
            font=('Arial', 12, 'bold')
        )
        self.status_label.pack(pady=(0, 10))
        
        # 统计信息
        stats_frame = ttk.Frame(status_frame)
        stats_frame.pack(fill=tk.X)
        
        self.stats_labels = {}
        stats_items = [
            ('总连接数:', 'total_connections'),
            ('活跃连接:', 'active_connections'),
            ('发送字节:', 'total_bytes_sent'),
            ('接收字节:', 'total_bytes_received'),
            ('运行时间:', 'uptime')
        ]
        
        for i, (label, key) in enumerate(stats_items):
            ttk.Label(stats_frame, text=label).grid(row=i, column=0, sticky=tk.W, pady=2)
            self.stats_labels[key] = ttk.Label(stats_frame, text="0", font=('Arial', 10, 'bold'))
            self.stats_labels[key].grid(row=i, column=1, sticky=tk.W, padx=(10, 0), pady=2)
        
    def create_log_panel(self, parent):
        log_frame = ttk.LabelFrame(parent, text="实时日志", padding="10")
        log_frame.grid(row=1, column=1, rowspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 日志文本框
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            height=15, 
            width=60,
            font=('Consolas', 9)
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # 日志标签颜色
        self.log_text.tag_configure('connection', foreground=self.colors['success'])
        self.log_text.tag_configure('error', foreground=self.colors['danger'])
        self.log_text.tag_configure('info', foreground=self.colors['accent'])
        
    def create_connections_panel(self, parent):
        conn_frame = ttk.LabelFrame(parent, text="活跃连接", padding="10")
        conn_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # 连接列表
        self.conn_tree = ttk.Treeview(
            conn_frame, 
            columns=('Address', 'Port', 'Protocol', 'Connect Time', 'Status'),
            height=8,
            show='headings'
        )
        
        # 配置列
        self.conn_tree.heading('Address', text='IP地址')
        self.conn_tree.heading('Port', text='端口')
        self.conn_tree.heading('Protocol', text='协议')
        self.conn_tree.heading('Connect Time', text='连接时间')
        self.conn_tree.heading('Status', text='状态')
        
        self.conn_tree.column('Address', width=100)
        self.conn_tree.column('Port', width=60)
        self.conn_tree.column('Protocol', width=60)
        self.conn_tree.column('Connect Time', width=100)
        self.conn_tree.column('Status', width=60)
        
        # 滚动条
        scrollbar = ttk.Scrollbar(conn_frame, orient=tk.VERTICAL, command=self.conn_tree.yview)
        self.conn_tree.configure(yscrollcommand=scrollbar.set)
        
        self.conn_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
    def create_traffic_chart(self, parent):
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
        
        chart_frame = ttk.LabelFrame(parent, text="流量图表", padding="10")
        chart_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 创建图形
        self.fig = Figure(figsize=(10, 4), dpi=80)
        self.ax = self.fig.add_subplot(111)
        self.ax.set_title('网络流量实时监控')
        self.ax.set_xlabel('时间')
        self.ax.set_ylabel('流量 (字节)')
        
        self.line_sent, = self.ax.plot([], [], 'g-', label='发送', linewidth=2)
        self.line_received, = self.ax.plot([], [], 'b-', label='接收', linewidth=2)
        self.ax.legend()
        self.ax.grid(True, alpha=0.3)
        
        # 嵌入到tkinter
        self.canvas = FigureCanvasTkAgg(self.fig, chart_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # 启动动画
        self.ani = animation.FuncAnimation(
            self.fig, 
            self.update_chart,
            interval=1000,
            blit=True
        )
        
    def start_server(self):
        try:
            host = self.host_var.get()
            port = int(self.port_var.get())
            buffer_size = int(self.buffer_var.get())
            delay = int(self.delay_var.get())
            max_clients = int(self.max_clients_var.get())
            
            if self.proxy_server.start(host, port, buffer_size, delay, max_clients):
                # 更新界面状态
                self.status_label.config(
                    text="● 运行中",
                    foreground=self.colors['success']
                )
                self.start_button.config(state=tk.DISABLED)
                self.stop_button.config(state=tk.NORMAL)
                
                # 禁用设置输入
                self.host_entry.config(state=tk.DISABLED)
                self.port_entry.config(state=tk.DISABLED)
                self.buffer_entry.config(state=tk.DISABLED)
                self.delay_entry.config(state=tk.DISABLED)
                self.max_clients_entry.config(state=tk.DISABLED)
                
                # 启动服务器线程
                self.server_thread = threading.Thread(
                    target=self.proxy_server.run,
                    args=(self.log_queue, self.stats_queue)
                )
                self.server_thread.daemon = True
                self.server_thread.start()
                
                self.add_log("服务器启动成功", 'info')
            else:
                messagebox.showerror("错误", "服务器启动失败，请检查端口是否被占用")
                
        except ValueError as e:
            messagebox.showerror("错误", "请输入有效的数值")
        except Exception as e:
            messagebox.showerror("错误", f"启动服务器时出错: {str(e)}")
            
    def stop_server(self):
        try:
            self.proxy_server.stop()
            
            # 更新界面状态
            self.status_label.config(
                text="● 已停止",
                foreground=self.colors['danger']
            )
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            
            # 启用设置输入
            self.host_entry.config(state=tk.NORMAL)
            self.port_entry.config(state=tk.NORMAL)
            self.buffer_entry.config(state=tk.NORMAL)
            self.delay_entry.config(state=tk.NORMAL)
            self.max_clients_entry.config(state=tk.NORMAL)
            
            self.add_log("服务器已停止", 'info')
            
        except Exception as e:
            messagebox.showerror("错误", f"停止服务器时出错: {str(e)}")
            
    def add_log(self, message, log_type='info'):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n", log_type)
        self.log_text.see(tk.END)
        
    def update_gui(self):
        try:
            # 处理日志队列
            while not self.log_queue.empty():
                log_data = self.log_queue.get_nowait()
                self.add_log(log_data['message'], log_data['type'])
                
            # 处理统计队列
            while not self.stats_queue.empty():
                stats = self.stats_queue.get_nowait()
                self.update_stats(stats)
                
        except queue.Empty:
            pass
            
        # 继续更新
        self.root.after(100, self.update_gui)
        
    def update_stats(self, stats):
        # 更新统计标签
        self.stats_labels['total_connections'].config(text=str(stats['total_connections']))
        self.stats_labels['active_connections'].config(text=str(stats['active_connections']))
        self.stats_labels['total_bytes_sent'].config(text=self.format_bytes(stats['total_bytes_sent']))
        self.stats_labels['total_bytes_received'].config(text=self.format_bytes(stats['total_bytes_received']))
        
        # 更新运行时间
        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            self.stats_labels['uptime'].config(text=str(uptime).split('.')[0])
        
        # 更新连接列表
        self.update_connections_list()
        
        # 更新流量图表
        self.update_traffic_chart(stats)
        
    def update_connections_list(self):
        # 清除现有项目
        for item in self.conn_tree.get_children():
            self.conn_tree.delete(item)
            
        # 添加活跃连接
        if hasattr(self.proxy_server, 'clients'):
            for addr, info in self.proxy_server.clients.items():
                protocol = info.get('protocol', 'Unknown')
                self.conn_tree.insert('', tk.END, values=(
                    addr[0],
                    addr[1],
                    protocol,
                    info['connect_time'].strftime("%H:%M:%S"),
                    "活跃"
                ))
            
    def update_chart(self, frame):
        try:
            # 获取当前统计数据
            stats = self.proxy_server.stats
            current_time = time.strftime("%H:%M:%S")
            
            # 更新数据
            self.traffic_data['time'].append(current_time)
            self.traffic_data['sent'].append(stats['total_bytes_sent'])
            self.traffic_data['received'].append(stats['total_bytes_received'])
            
            # 保持最近50个数据点
            max_points = 50
            for key in self.traffic_data:
                if len(self.traffic_data[key]) > max_points:
                    self.traffic_data[key] = self.traffic_data[key][-max_points:]
            
            # 更新图表
            self.line_sent.set_data(range(len(self.traffic_data['time'])), self.traffic_data['sent'])
            self.line_received.set_data(range(len(self.traffic_data['time'])), self.traffic_data['received'])
            
            # 更新坐标轴
            self.ax.set_xlim(0, max(50, len(self.traffic_data['time'])))
            if len(self.traffic_data['sent']) > 0 and len(self.traffic_data['received']) > 0:
                max_val = max(max(self.traffic_data['sent']), max(self.traffic_data['received']))
                self.ax.set_ylim(0, max_val * 1.1 if max_val > 0 else 1)
            
            # 更新x轴标签
            if len(self.traffic_data['time']) > 0:
                self.ax.set_xticks(range(0, len(self.traffic_data['time']), max(1, len(self.traffic_data['time']) // 10)))
                self.ax.set_xticklabels([self.traffic_data['time'][i] for i in range(0, len(self.traffic_data['time']), max(1, len(self.traffic_data['time']) // 10))], rotation=45)
            
            return self.line_sent, self.line_received
            
        except Exception as e:
            return self.line_sent, self.line_received
    
    def format_bytes(self, bytes):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024.0:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.2f} TB"

def main():
    root = tk.Tk()
    app = ProxyGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()