#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
图形界面网络代理工具 - 轻量版
使用纯tkinter实现，无需matplotlib依赖
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
import os
import sys
import json
from datetime import datetime
import struct

# 系统托盘相关依赖（可选，缺失时降级为普通最小化）
try:
    import pystray
    from PIL import Image, ImageDraw, ImageFont
    TRAY_AVAILABLE = True
except Exception:
    TRAY_AVAILABLE = False


class ConfigManager:
    """配置管理器 - 保存和加载代理服务器配置"""
    
    def __init__(self, config_file='proxy_config.json'):
        self.config_file = config_file
        self.backup_file = config_file + '.backup'
        self.default_config = {
            'host': '0.0.0.0',
            'port': 7890,
            'buffer_size': 1024,
            'delay': 1,
            'max_clients': 100,
            'auto_start': False,
            'server_running': False,
            'last_start_time': None,
            'blacklist': []  # 黑名单IP列表
        }
    
    def save_config(self, config):
        """保存配置到文件"""
        try:
            # 先备份现有配置
            if os.path.exists(self.config_file):
                import shutil
                shutil.copy2(self.config_file, self.backup_file)
            
            # 保存新配置
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"保存配置失败: {e}")
            return False
    
    def load_config(self):
        """从文件加载配置"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded_config = json.load(f)
                    # 合并默认配置，确保所有必需的键都存在
                    config = self.default_config.copy()
                    config.update(loaded_config)
                    return config
            else:
                return self.default_config.copy()
        except Exception as e:
            print(f"加载配置失败: {e}")
            return self.default_config.copy()
    
    def get_current_config(self, gui_instance):
        """从GUI实例获取当前配置"""
        try:
            config = {
                'host': gui_instance.host_var.get(),
                'port': int(gui_instance.port_var.get()),
                'buffer_size': int(gui_instance.buffer_var.get()),
                'delay': int(gui_instance.delay_var.get()),
                'max_clients': int(gui_instance.max_clients_var.get()),
                'auto_start': gui_instance.auto_start_var.get() if hasattr(gui_instance, 'auto_start_var') else False,
                'server_running': gui_instance.proxy_server.running if hasattr(gui_instance, 'proxy_server') else False,
                'last_start_time': gui_instance.last_start_time if hasattr(gui_instance, 'last_start_time') else None,
                'blacklist': gui_instance.blacklist if hasattr(gui_instance, 'blacklist') else []
            }
            return config
        except Exception as e:
            print(f"获取当前配置失败: {e}")
            return self.default_config.copy()


class ProxyServer:
    """代理服务器核心类"""
    
    def __init__(self, blacklist=None):
        self.server_socket = None
        self.running = False
        self.clients = {}
        self.blacklist = blacklist or []  # 黑名单
        
        # 基本配置
        self.buffer_size = 1024
        self.delay = 1
        self.host = '0.0.0.0'
        self.port = 7890
        self.max_clients = 100
        
        # 统计信息
        self.stats = {
            'total_connections': 0,
            'active_connections': 0,
            'total_bytes_sent': 0,
            'total_bytes_received': 0,
            'start_time': None
        }
        
        # 流量历史数据
        self.traffic_history = []
        self.log_queue = None
        
        # SOCKS5 协议常量
        self.SOCKS5_VERSION = 0x05
        self.SOCKS5_AUTH_NONE = 0x00
        self.SOCKS5_AUTH_NO_ACCEPTABLE = 0xFF
        self.SOCKS5_CMD_CONNECT = 0x01
        self.SOCKS5_ATYP_IPV4 = 0x01
        self.SOCKS5_ATYP_DOMAIN = 0x03
        self.SOCKS5_ATYP_IPV6 = 0x04
        self.SOCKS5_REP_SUCCESS = 0x00
        self.SOCKS5_REP_CONNECTION_REFUSED = 0x05
        self.SOCKS5_REP_COMMAND_NOT_SUPPORTED = 0x07
        self.SOCKS5_REP_ADDRESS_TYPE_NOT_SUPPORTED = 0x08
        
    def start(self, host, port, buffer_size, delay, max_clients):
        """启动代理服务器"""
        try:
            # 保存配置参数
            self.host = host
            self.port = port
            self.buffer_size = buffer_size
            self.delay = delay
            self.max_clients = max_clients
            
            # 创建服务器套接字
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(self.max_clients)
            self.server_socket.setblocking(False)
            
            # 设置运行状态
            self.running = True
            self.stats['start_time'] = datetime.now()
            
            return True
            
        except Exception as e:
            return False
        
    def stop(self):
        """停止代理服务器"""
        self.running = False
        
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None
            
    def handle_client(self, client_socket, client_address):
        """处理客户端连接"""
        # 提前初始化，确保 finally 块（含黑名单早返回/异常路径）始终能访问到
        bytes_sent = 0
        bytes_received = 0
        protocol_type = "Unknown"
        try:
            # 检查黑名单
            client_ip = client_address[0]
            if hasattr(self, 'blacklist') and client_ip in self.blacklist:
                # 拒绝黑名单IP的连接
                if self.log_queue:
                    self.log_queue.put({
                        'type': 'error',
                        'message': f"黑名单IP {client_ip} 尝试连接已被拒绝",
                        'timestamp': datetime.now()
                    })

                # 发送拒绝响应
                try:
                    error_response = b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"
                    client_socket.send(error_response)
                    bytes_sent = len(error_response)
                except:
                    pass

                client_socket.close()
                return

            # 更新统计信息
            self.stats['total_connections'] += 1
            self.stats['active_connections'] += 1

            # 接收客户端初始请求数据以判断协议类型
            try:
                # 临时设置为阻塞模式以便接收初始数据
                client_socket.setblocking(True)
                client_socket.settimeout(5.0)  # 设置5秒超时

                data = client_socket.recv(self.buffer_size)
                if not data:
                    return

                bytes_received += len(data)
                self.stats['total_bytes_received'] += len(data)
                
                # 协议类型判断和处理
                if data.startswith(b'\x05'):
                    # SOCKS5协议
                    protocol_type = "SOCKS5"
                    self.clients[client_address] = {
                        'socket': client_socket,
                        'connect_time': datetime.now(),
                        'bytes_sent': 0,
                        'bytes_received': bytes_received,
                        'protocol': 'SOCKS5',
                        'status': 'Connected'
                    }
                    self.handle_socks5_request(client_socket, data, client_address)
                    
                elif self.is_http_request(data):
                    # HTTP/HTTPS协议
                    if data.startswith(b'CONNECT'):
                        protocol_type = "HTTPS"
                    else:
                        protocol_type = "HTTP"
                    
                    self.clients[client_address] = {
                        'socket': client_socket,
                        'connect_time': datetime.now(),
                        'bytes_sent': 0,
                        'bytes_received': bytes_received,
                        'protocol': protocol_type,
                        'status': 'Connected'
                    }
                    # 重新设置为非阻塞模式用于HTTP处理
                    client_socket.setblocking(False)
                    self.handle_http_proxy_request(client_socket, data, client_address)
                    
                else:
                    # 未知协议，发送错误响应
                    protocol_type = "Unknown"
                    error_response = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
                    client_socket.send(error_response)
                    bytes_sent += len(error_response)
                    self.stats['total_bytes_sent'] += len(error_response)
                    
            except socket.error:
                pass
                
        except Exception as e:
            pass
            
        finally:
            # 清理连接
            try:
                client_socket.close()
            except:
                pass
                
            self.stats['active_connections'] -= 1
            if client_address in self.clients:
                del self.clients[client_address]
            
            # 记录连接关闭
            self.traffic_history.append({
                'time': datetime.now(),
                'bytes_sent': bytes_sent,
                'bytes_received': bytes_received,
                'duration': datetime.now() - datetime.now()
            })
            
    def handle_socks5_request(self, client_socket, req_data, client_address):
        """处理SOCKS5代理请求"""
        try:
            # SOCKS5握手阶段
            if len(req_data) >= 3 and req_data[0] == self.SOCKS5_VERSION:
                # 检查认证方法
                nmethods = req_data[1]
                methods = req_data[2:2+nmethods]
                
                # 只支持无认证(0x00)
                if self.SOCKS5_AUTH_NONE in methods:
                    # 选择无认证
                    client_socket.send(bytes([self.SOCKS5_VERSION, self.SOCKS5_AUTH_NONE]))
                    
                    if self.log_queue:
                        self.log_queue.put({
                            'type': 'info',
                            'message': f"SOCKS5 handshake completed - no authentication "
                                      f"from {client_address[0]}:{client_address[1]}",
                            'timestamp': datetime.now()
                        })
                else:
                    # 无可接受的方法
                    client_socket.send(bytes([self.SOCKS5_VERSION, self.SOCKS5_AUTH_NO_ACCEPTABLE]))
                    client_socket.close()
                    
                    if self.log_queue:
                        self.log_queue.put({
                            'type': 'error',
                            'message': f"SOCKS5 handshake failed - no acceptable authentication method "
                                      f"from {client_address[0]}:{client_address[1]}",
                            'timestamp': datetime.now()
                        })
                    return
            
            # 接收连接请求 - 设置超时
            client_socket.settimeout(30)  # 设置超时时间
            req_data = client_socket.recv(self.buffer_size)
            
            if len(req_data) < 10:
                client_socket.close()
                return
            
            # 解析SOCKS5请求
            version = req_data[0]
            cmd = req_data[1]
            addr_type = req_data[3]
            
            # 验证协议版本
            if version != self.SOCKS5_VERSION:
                client_socket.close()
                return
            
            # 只支持CONNECT命令
            if cmd != self.SOCKS5_CMD_CONNECT:
                # 命令不支持
                response = bytes([
                    self.SOCKS5_VERSION, self.SOCKS5_REP_COMMAND_NOT_SUPPORTED,
                    0x00, self.SOCKS5_ATYP_IPV4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                ])
                client_socket.send(response)
                client_socket.close()
                return
            
            # 解析目标地址
            if addr_type == self.SOCKS5_ATYP_IPV4:
                # IPv4地址
                if len(req_data) < 10:
                    client_socket.close()
                    return
                target_addr = socket.inet_ntoa(req_data[4:8])
                target_port = int.from_bytes(req_data[8:10], 'big')
                
            elif addr_type == self.SOCKS5_ATYP_DOMAIN:
                # 域名
                if len(req_data) < 5:
                    client_socket.close()
                    return
                domain_length = req_data[4]
                if len(req_data) < 5 + domain_length + 2:
                    client_socket.close()
                    return
                target_addr = req_data[5:5+domain_length].decode()
                target_port = int.from_bytes(req_data[5+domain_length:7+domain_length], 'big')
                
            elif addr_type == self.SOCKS5_ATYP_IPV6:
                # IPv6地址 - 不支持
                response = bytes([
                    self.SOCKS5_VERSION, self.SOCKS5_REP_ADDRESS_TYPE_NOT_SUPPORTED,
                    0x00, self.SOCKS5_ATYP_IPV4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                ])
                client_socket.send(response)
                client_socket.close()
                return
                
            else:
                # 未知的地址类型
                response = bytes([
                    self.SOCKS5_VERSION, self.SOCKS5_REP_ADDRESS_TYPE_NOT_SUPPORTED,
                    0x00, self.SOCKS5_ATYP_IPV4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                ])
                client_socket.send(response)
                client_socket.close()
                return
            
            # 连接到目标服务器
            try:
                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.settimeout(30)
                server_socket.connect((target_addr, target_port))
                
                # 发送成功响应
                response = bytes([
                    self.SOCKS5_VERSION, self.SOCKS5_REP_SUCCESS,
                    0x00, self.SOCKS5_ATYP_IPV4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                ])
                client_socket.send(response)
                
                if self.log_queue:
                    self.log_queue.put({
                        'type': 'connection',
                        'message': f"SOCKS5 connection established to {target_addr}:{target_port} "
                                  f"from {client_address[0]}:{client_address[1]}",
                        'timestamp': datetime.now()
                    })
                
                # 开始转发数据 - 重置为非阻塞模式
                client_socket.setblocking(False)
                self.forward_data(client_socket, server_socket, client_address)
                
            except Exception as e:
                # 连接失败
                response = bytes([
                    self.SOCKS5_VERSION, self.SOCKS5_REP_CONNECTION_REFUSED,
                    0x00, self.SOCKS5_ATYP_IPV4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                ])
                client_socket.send(response)
                client_socket.close()
                
                if self.log_queue:
                    self.log_queue.put({
                        'type': 'error',
                        'message': f"SOCKS5 connection failed to {target_addr}:{target_port} - {str(e)}",
                        'timestamp': datetime.now()
                    })
                
        except Exception as e:
            # 清理连接
            try:
                client_socket.close()
            except:
                pass
                
            if self.log_queue:
                self.log_queue.put({
                    'type': 'error',
                    'message': f"SOCKS5 error from {client_address[0]}:{client_address[1]} - {str(e)}",
                    'timestamp': datetime.now()
                })
                
    def handle_http_proxy_request(self, client_socket, req_data, client_address):
        """处理HTTP/HTTPS代理请求"""
        try:
            # 解析HTTP请求
            lines = req_data.split(b'\r\n')
            if len(lines) == 0:
                return
            
            request_line = lines[0]
            parts = request_line.split(b' ')
            if len(parts) < 3:
                return
            
            method = parts[0]
            uri = parts[1]
            version = parts[2]
            
            # 解析Host头
            host = None
            port = 80
            
            for line in lines[1:]:
                if line.lower().startswith(b'host:'):
                    host_line = line[5:].strip()
                    if b':' in host_line:
                        host, port = host_line.split(b':', 1)
                        port = int(port)
                    else:
                        host = host_line
                    break
            
            if not host:
                error_response = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
                client_socket.send(error_response)
                return
            
            # 处理HTTP请求
            if method in [b'GET', b'POST', b'PUT', b'DELETE', b'HEAD']:
                # 修改URI，移除主机部分
                if uri.startswith(b'http://'):
                    uri = uri.replace(b'http://' + host, b'')
                
                # 构建新的请求
                new_request = b'%s %s %s\r\n' % (method, uri, version)
                
                for line in lines[1:]:
                    if line and not line.lower().startswith(b'proxy-'):
                        new_request += line + b'\r\n'
                new_request += b'\r\n'
                
                # 连接到目标服务器
                try:
                    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    server_socket.settimeout(30)
                    server_socket.connect((host.decode(), port))
                    
                    # 发送请求
                    server_socket.send(new_request)
                    
                    if self.log_queue:
                        self.log_queue.put({
                            'type': 'connection',
                            'message': f"HTTP {method.decode()} request to {host.decode()}:{port} "
                                      f"from {client_address[0]}:{client_address[1]}",
                            'timestamp': datetime.now()
                        })
                    
                    # 转发数据
                    self.forward_data(client_socket, server_socket, client_address)
                    
                except Exception as e:
                    error_response = b'HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n'
                    client_socket.send(error_response)
                    
                    if self.log_queue:
                        self.log_queue.put({
                            'type': 'error',
                            'message': f"HTTP connection failed to {host.decode()}:{port} - {str(e)}",
                            'timestamp': datetime.now()
                        })
            
            # 处理HTTPS CONNECT请求
            elif method == b'CONNECT':
                # 解析目标地址
                if b':' in uri:
                    target_host, target_port = uri.split(b':', 1)
                    target_port = int(target_port)
                else:
                    target_host = uri
                    target_port = 443
                
                try:
                    # 连接到目标服务器
                    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    server_socket.settimeout(30)
                    server_socket.connect((target_host.decode(), target_port))
                    
                    # 发送连接成功响应
                    success_response = b'%s 200 Connection Established\r\nConnection: close\r\n\r\n' % version
                    client_socket.send(success_response)
                    
                    if self.log_queue:
                        self.log_queue.put({
                            'type': 'connection',
                            'message': f"HTTPS CONNECT to {target_host.decode()}:{target_port} "
                                      f"from {client_address[0]}:{client_address[1]}",
                            'timestamp': datetime.now()
                        })
                    
                    # 转发数据
                    self.forward_data(client_socket, server_socket, client_address)
                    
                except Exception as e:
                    error_response = b'%s 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n' % version
                    client_socket.send(error_response)
                    
                    if self.log_queue:
                        self.log_queue.put({
                            'type': 'error',
                            'message': f"HTTPS CONNECT failed to {target_host.decode()}:{target_port} - {str(e)}",
                            'timestamp': datetime.now()
                        })
            
            else:
                # 不支持的HTTP方法
                error_response = b'HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n'
                client_socket.send(error_response)
                
        except Exception as e:
            if self.log_queue:
                self.log_queue.put({
                    'type': 'error',
                    'message': f"HTTP proxy error from {client_address[0]}:{client_address[1]} - {str(e)}",
                    'timestamp': datetime.now()
                })
                
    def forward_data(self, client_socket, server_socket, client_address):
        """在客户端和服务器之间转发数据"""
        try:
            sockets = [client_socket, server_socket]
            total_bytes_sent = 0
            total_bytes_received = 0
            
            while self.running:
                try:
                    readable, _, _ = select.select(sockets, [], [], 2.0)
                    
                    if not readable:
                        continue
                    
                    for sock in readable:
                        try:
                            data = sock.recv(self.buffer_size)
                            if not data:
                                return
                            
                            if sock is client_socket:
                                # 从客户端接收数据，发送到服务器
                                server_socket.send(data)
                                total_bytes_sent += len(data)
                                self.stats['total_bytes_sent'] += len(data)
                            else:
                                # 从服务器接收数据，发送到客户端
                                client_socket.send(data)
                                total_bytes_received += len(data)
                                self.stats['total_bytes_received'] += len(data)
                            
                            if self.delay > 0:
                                time.sleep(self.delay / 1000.0)
                                
                        except socket.error:
                            return
                            
                except select.error:
                    # select错误，通常是socket关闭
                    break
                except socket.error:
                    # socket错误
                    break
                        
        except Exception as e:
            if self.log_queue:
                self.log_queue.put({
                    'type': 'error',
                    'message': f"Data forwarding error - {str(e)}",
                    'timestamp': datetime.now()
                })
                
        finally:
            # 清理连接
            try:
                client_socket.close()
            except:
                pass
            try:
                server_socket.close()
            except:
                pass
                
    def is_http_request(self, data):
        """判断是否为HTTP请求"""
        try:
            # 检查是否以HTTP方法开头
            http_methods = [
                b'GET', b'POST', b'PUT', b'DELETE', b'HEAD',
                b'OPTIONS', b'CONNECT', b'TRACE', b'PATCH'
            ]
            
            for method in http_methods:
                if data.startswith(method + b' '):
                    return True
            return False
            
        except:
            return False
            
    def handle_http_request(self, data):
        """处理HTTP请求并返回响应"""
        try:
            # 构建简单的HTTP响应
            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/html\r\n"
                b"Connection: close\r\n"
                b"\r\n"
                b"<html><body><h1>Proxy Server Response</h1></body></html>"
            )
            return response
            
        except Exception as e:
            return None
            
    def run(self, log_queue, stats_queue):
        """运行代理服务器主循环"""
        self.log_queue = log_queue  # 设置日志队列
        
        while self.running:
            try:
                # 检查是否有新的连接
                readable, _, _ = select.select([self.server_socket], [], [], 1.0)
                
                if self.server_socket in readable:
                    client_socket, client_address = self.server_socket.accept()
                    client_socket.setblocking(False)
                    
                    # 记录连接日志
                    if self.log_queue:
                        self.log_queue.put({
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
                if self.running and self.log_queue:
                    self.log_queue.put({
                        'type': 'error',
                        'message': f"Server error: {str(e)}",
                        'timestamp': datetime.now()
                    })


class TrafficCanvas(tk.Canvas):
    """自定义流量图表组件"""
    
    def __init__(self, parent, width=600, height=200, bg='white'):
        super().__init__(parent, width=width, height=height, bg=bg, highlightthickness=1)
        self.width = width
        self.height = height
        self.bg = bg
        self.traffic_data = {'sent': [], 'received': []}
        self.max_points = 60  # 显示最近60个数据点
        
    def update_traffic(self, bytes_sent, bytes_received):
        """更新流量数据"""
        current_time = datetime.now().strftime("%H:%M:%S")
        
        self.traffic_data['sent'].append(bytes_sent)
        self.traffic_data['received'].append(bytes_received)
        
        # 保持最近的数据点
        for key in self.traffic_data:
            if len(self.traffic_data[key]) > self.max_points:
                self.traffic_data[key] = self.traffic_data[key][-self.max_points:]
        
        self.redraw()
        
    def redraw(self):
        """重绘图表"""
        self.delete("all")
        
        # 绘制背景网格
        self.draw_grid()
        
        # 绘制数据线
        if len(self.traffic_data['sent']) > 1:
            self.draw_line(self.traffic_data['sent'], 'green', '发送')
            self.draw_line(self.traffic_data['received'], 'blue', '接收')
        
        # 绘制图例
        self.draw_legend()
        
    def draw_grid(self):
        """绘制网格"""
        # 清除之前的网格
        self.delete('grid')
        
        # 水平网格线和标签
        for i in range(0, 6):
            y = self.height - 40 - (i * (self.height - 80) // 5)
            self.create_line(60, y, self.width - 30, y, fill='#f0f0f0', width=1, tags='grid')
            # 添加Y轴标签
            value = i * 20  # 假设最大值为100，每格20
            self.create_text(45, y, text=f"{value}", anchor='e', font=('Arial', 8), fill='#666666', tags='grid')
        
        # 垂直网格线和标签
        time_labels = ['60s', '50s', '40s', '30s', '20s', '10s', '0s']
        for i in range(0, 7):
            x = 60 + (i * (self.width - 90) // 6)
            self.create_line(x, 20, x, self.height - 40, fill='#f0f0f0', width=1, tags='grid')
            # 添加X轴时间标签
            self.create_text(x, self.height - 25, text=time_labels[i], anchor='center', 
                           font=('Arial', 8), fill='#666666', tags='grid')
        
        # 坐标轴
        self.create_line(60, self.height - 40, self.width - 30, self.height - 40, 
                        width=2, fill='#333333', tags='grid')  # X轴
        self.create_line(60, 20, 60, self.height - 40, 
                        width=2, fill='#333333', tags='grid')  # Y轴
        
        # 轴标题
        self.create_text(25, self.height // 2, text="流量 (KB/s)", anchor='center', 
                        font=('Arial', 9, 'bold'), angle=90, fill='#333333', tags='grid')
        self.create_text(self.width // 2, self.height - 5, text="时间", anchor='center', 
                        font=('Arial', 9, 'bold'), fill='#333333', tags='grid')
        
    def draw_line(self, data, color, label):
        """绘制数据线"""
        if not data or len(data) < 2:
            return
        
        # 清除之前的线条
        self.delete(f'line_{label}')
        
        # 计算最大值用于缩放
        max_val = max(max(self.traffic_data['sent']) if self.traffic_data['sent'] else 1,
                     max(self.traffic_data['received']) if self.traffic_data['received'] else 1)
        
        if max_val == 0:
            max_val = 1
        
        # 绘制线条
        points = []
        for i, value in enumerate(data):
            x = 60 + (i * (self.width - 90) // max(1, len(data) - 1))
            y = self.height - 40 - (value * (self.height - 80) // max_val)
            points.extend([x, y])
        
        if len(points) >= 4:
            self.create_line(points, fill=color, width=3, smooth=True, tags=f'line_{label}')
        
        # 绘制数据点（只显示最近的几个）
        point_count = min(10, len(data))  # 最多显示10个点
        start_idx = max(0, len(data) - point_count)
        
        for i in range(start_idx, len(data)):
            value = data[i]
            x = 60 + (i * (self.width - 90) // max(1, len(data) - 1))
            y = self.height - 40 - (value * (self.height - 80) // max_val)
            self.create_oval(x-4, y-4, x+4, y+4, fill=color, outline='white', 
                           width=2, tags=f'line_{label}')
            
    def draw_legend(self):
        """绘制图例"""
        # 清除之前的图例
        self.delete('legend')
        
        # 图例背景
        legend_x = self.width - 130
        legend_y = 30
        legend_width = 100
        legend_height = 50
        
        self.create_rectangle(legend_x, legend_y, legend_x + legend_width, legend_y + legend_height, 
                            fill='#ffffff', outline='#cccccc', width=1, tags='legend')
        
        # 图例标题
        self.create_text(legend_x + legend_width // 2, legend_y + 12, text="流量图例", 
                        anchor='center', font=('Arial', 9, 'bold'), tags='legend')
        
        # 图例项
        line_y1 = legend_y + 25
        line_y2 = legend_y + 40
        
        # 发送图例
        self.create_line(legend_x + 10, line_y1, legend_x + 25, line_y1, 
                        fill='green', width=3, tags='legend')
        self.create_oval(legend_x + 22, line_y1 - 3, legend_x + 28, line_y1 + 3, 
                        fill='green', outline='white', width=2, tags='legend')
        self.create_text(legend_x + 40, line_y1, text="发送", anchor='w', 
                        font=('Arial', 9), tags='legend')
        
        # 接收图例
        self.create_line(legend_x + 10, line_y2, legend_x + 25, line_y2, 
                        fill='blue', width=3, tags='legend')
        self.create_oval(legend_x + 22, line_y2 - 3, legend_x + 28, line_y2 + 3, 
                        fill='blue', outline='white', width=2, tags='legend')
        self.create_text(legend_x + 40, line_y2, text="接收", anchor='w', 
                        font=('Arial', 9), tags='legend')


class ProxyGUI:
    """代理服务器图形界面类"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("网络代理服务器 - 支持HTTP/HTTPS/SOCKS5")
        self.root.geometry("1200x800")  # 固定窗口大小
        self.root.minsize(1100, 700)   # 设置最小尺寸
        
        # 配置管理器
        self.config_manager = ConfigManager()
        self.config = self.config_manager.load_config()
        
        # 黑名单
        self.blacklist = self.config.get('blacklist', [])
        
        # 设置窗口图标和样式
        self.setup_window_style()
        
        # 设置界面样式和颜色
        self.setup_styles()
        
        # 代理服务器实例
        self.proxy_server = ProxyServer(blacklist=self.blacklist)
        self.server_thread = None
        
        # 队列用于线程间通信
        self.log_queue = queue.Queue()
        self.stats_queue = queue.Queue()

        # 系统托盘相关状态
        self._tray_icon = None
        self._tray_hiding = False
        self._tray_quitting = False

        # 流量速率统计状态（用于当前速率与峰值速率计算）
        self._rate_last_time = None
        self._rate_last_sent = 0
        self._rate_last_received = 0
        self._peak_sent_rate = 0.0
        self._peak_received_rate = 0.0

        # 创建界面
        self.create_widgets()

        # 加载保存的配置
        self.load_saved_config()

        # 启动更新定时器
        self.update_gui()

        # 如果配置了自动启动，或者上次关闭时服务器正在运行，则启动服务器
        should_auto_start = self.config.get('auto_start', False) or self.config.get('server_running', False)
        if should_auto_start:
            self.root.after(1000, self.auto_start_server)
            if self.config.get('server_running', False):
                self.add_log("检测到上次异常关闭，正在自动恢复服务...", 'info')

        # 绑定窗口关闭事件：点关闭按钮直接关闭程序
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        # 绑定最小化事件：最小化时缩小到系统托盘
        self.root.bind("<Unmap>", self._on_unmap)
        
    def setup_window_style(self):
        """设置窗口样式"""
        # 设置窗口样式
        self.root.configure(bg='#f5f5f5')
        
        # 尝试设置窗口图标（如果存在）
        try:
            self.root.iconbitmap(default='proxy.ico')
        except:
            pass  # 如果没有图标文件就忽略
            
        # 设置默认字体
        self.root.option_add('*Font', 'Arial 10')
        self.root.option_add('*Label.Font', 'Arial 10')
        self.root.option_add('*Button.Font', 'Arial 10')
        
        # 设置窗口居中
        self.center_window()
        
        # 创建菜单栏
        self.create_menu_bar()
        
    def create_menu_bar(self):
        """创建菜单栏"""
        menubar = tk.Menu(self.root)
        
        # 文件菜单
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="保存配置", command=self.save_current_config)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self.on_closing)
        menubar.add_cascade(label="文件", menu=file_menu)
        
        # 配置菜单
        config_menu = tk.Menu(menubar, tearoff=0)
        config_menu.add_command(label="查看当前配置", command=self.show_current_config)
        config_menu.add_separator()
        config_menu.add_command(label="黑名单管理", command=self.manage_blacklist)
        config_menu.add_separator()
        config_menu.add_command(label="重置为默认配置", command=self.reset_to_default_config)
        config_menu.add_command(label="恢复备份配置", command=self.restore_backup_config)
        menubar.add_cascade(label="配置", menu=config_menu)
        
        # 帮助菜单
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="关于", command=self.show_about)
        menubar.add_cascade(label="帮助", menu=help_menu)
        
        self.root.config(menu=menubar)
        
    def reset_to_default_config(self):
        """重置为默认配置"""
        try:
            # 重置为默认配置
            self.config = self.config_manager.default_config.copy()
            self.config_manager.save_config(self.config)
            
            # 重新加载配置到界面
            self.load_saved_config()
            
            self.add_log("配置已重置为默认值", 'info')
            messagebox.showinfo("提示", "配置已重置为默认值")
            
        except Exception as e:
            messagebox.showerror("错误", f"重置配置失败: {str(e)}")
            
    def show_about(self):
        """显示关于对话框"""
        about_text = "网络代理服务器 - 轻量版\n\n"
        about_text += "支持 HTTP/HTTPS/SOCKS5 协议\n"
        about_text += "具有实时监控、日志查看、流量统计等功能\n\n"
        about_text += "配置会自动保存，下次启动时自动加载"
        
        messagebox.showinfo("关于", about_text)
        
    def restore_backup_config(self):
        """恢复备份配置"""
        try:
            if os.path.exists(self.config_manager.backup_file):
                # 从备份文件加载配置
                with open(self.config_manager.backup_file, 'r', encoding='utf-8') as f:
                    backup_config = json.load(f)
                
                # 更新当前配置
                self.config = backup_config
                self.config_manager.save_config(self.config)
                
                # 重新加载配置到界面
                self.load_saved_config()
                
                self.add_log("已从备份恢复配置", 'info')
                messagebox.showinfo("提示", "已从备份恢复配置")
            else:
                messagebox.showwarning("提示", "没有找到备份配置文件")
                
        except Exception as e:
            messagebox.showerror("错误", f"恢复备份配置失败: {str(e)}")
            
    def manage_blacklist(self):
        """黑名单管理对话框"""
        try:
            # 创建黑名单管理窗口
            blacklist_window = tk.Toplevel(self.root)
            blacklist_window.title("黑名单管理")
            blacklist_window.geometry("500x400")
            blacklist_window.transient(self.root)
            blacklist_window.grab_set()
            
            # 窗口居中
            blacklist_window.update_idletasks()
            x = (blacklist_window.winfo_screenwidth() - blacklist_window.winfo_width()) // 2
            y = (blacklist_window.winfo_screenheight() - blacklist_window.winfo_height()) // 2
            blacklist_window.geometry(f"+{x}+{y}")
            
            # 创建主框架
            main_frame = ttk.Frame(blacklist_window, padding="10")
            main_frame.pack(fill=tk.BOTH, expand=True)
            
            # 标题
            title_label = ttk.Label(main_frame, text="黑名单管理", font=('Arial', 12, 'bold'))
            title_label.pack(pady=(0, 10))
            
            # 说明标签
            desc_label = ttk.Label(main_frame, text="每行输入一个IP地址，黑名单中的IP将被拒绝连接")
            desc_label.pack(pady=(0, 10))
            
            # 创建文本框和滚动条
            text_frame = ttk.Frame(main_frame)
            text_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
            
            # 文本框
            self.blacklist_text = tk.Text(text_frame, height=15, width=50, font=('Consolas', 10))
            self.blacklist_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            
            # 滚动条
            scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.blacklist_text.yview)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            self.blacklist_text.configure(yscrollcommand=scrollbar.set)
            
            # 加载当前黑名单
            current_blacklist = '\n'.join(self.blacklist)
            self.blacklist_text.insert(1.0, current_blacklist)
            
            # 按钮框架
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill=tk.X)
            
            # 保存按钮
            save_btn = ttk.Button(button_frame, text="保存", command=lambda: self.save_blacklist(blacklist_window))
            save_btn.pack(side=tk.RIGHT, padx=(5, 0))
            
            # 取消按钮
            cancel_btn = ttk.Button(button_frame, text="取消", command=blacklist_window.destroy)
            cancel_btn.pack(side=tk.RIGHT)
            
            # 清空按钮
            clear_btn = ttk.Button(button_frame, text="清空", command=lambda: self.blacklist_text.delete(1.0, tk.END))
            clear_btn.pack(side=tk.RIGHT, padx=(0, 5))
            
            # 示例按钮
            example_btn = ttk.Button(button_frame, text="加载示例", command=self.load_blacklist_example)
            example_btn.pack(side=tk.LEFT)
            
        except Exception as e:
            messagebox.showerror("错误", f"打开黑名单管理失败: {str(e)}")
            
    def save_blacklist(self, window):
        """保存黑名单"""
        try:
            # 获取文本框内容
            blacklist_text = self.blacklist_text.get(1.0, tk.END).strip()
            
            # 解析IP地址
            if blacklist_text:
                # 分割成行，去除空行和空格
                ip_lines = [line.strip() for line in blacklist_text.split('\n') if line.strip()]
                
                # 验证IP地址格式
                valid_ips = []
                invalid_ips = []
                
                for ip in ip_lines:
                    if self.is_valid_ip(ip):
                        valid_ips.append(ip)
                    else:
                        invalid_ips.append(ip)
                
                if invalid_ips:
                    response = messagebox.askyesno(
                        "验证提示", 
                        f"发现 {len(invalid_ips)} 个无效IP地址:\n{', '.join(invalid_ips)}\n\n"
                        f"是否继续保存有效的 {len(valid_ips)} 个IP地址？"
                    )
                    if not response:
                        return
                
                self.blacklist = valid_ips
            else:
                self.blacklist = []
            
            # 更新代理服务器的黑名单
            self.proxy_server.blacklist = self.blacklist.copy()
            
            # 保存配置
            self.save_current_config()
            
            # 记录日志
            if self.blacklist:
                self.add_log(f"黑名单已更新，包含 {len(self.blacklist)} 个IP地址", 'info')
            else:
                self.add_log("黑名单已清空", 'info')
            
            window.destroy()
            
        except Exception as e:
            messagebox.showerror("错误", f"保存黑名单失败: {str(e)}")
            
    def is_valid_ip(self, ip):
        """验证IP地址格式"""
        try:
            # 简单的IP地址验证
            parts = ip.split('.')
            if len(parts) != 4:
                return False
            
            for part in parts:
                if not part.isdigit():
                    return False
                num = int(part)
                if num < 0 or num > 255:
                    return False
            
            return True
        except:
            return False
            
    def load_blacklist_example(self):
        """加载黑名单示例"""
        example_text = "# 黑名单示例 - 每行一个IP地址\n"
        example_text += "# 可以添加注释（以#开头）\n\n"
        example_text += "192.168.1.100\n"
        example_text += "10.0.0.50\n"
        example_text += "172.16.0.25\n"
        
        self.blacklist_text.delete(1.0, tk.END)
        self.blacklist_text.insert(1.0, example_text)
            
    def add_to_blacklist(self):
        """将选中的连接加入黑名单"""
        try:
            selected_items = self.conn_tree.selection()
            if selected_items:
                item = selected_items[0]
                values = self.conn_tree.item(item)['values']
                if values:
                    ip = values[0]
                    
                    if ip not in self.blacklist:
                        self.blacklist.append(ip)
                        self.proxy_server.blacklist = self.blacklist.copy()  # 更新代理服务器的黑名单
                        self.save_current_config()
                        self.add_log(f"🚫 已将IP {ip} 加入黑名单", 'info')
                        messagebox.showinfo("提示", f"已将IP {ip} 加入黑名单")
                        
                        # 断开该IP的所有连接
                        self.disconnect_connection()
                        
                        # 更新连接列表
                        self.update_connections_list()
                    else:
                        messagebox.showwarning("提示", f"IP {ip} 已在黑名单中")
            else:
                messagebox.showwarning("提示", "请先选择一个连接")
                
        except Exception as e:
            messagebox.showerror("错误", f"加入黑名单失败: {str(e)}")
            
    def remove_from_blacklist(self):
        """从黑名单中移除选中的IP"""
        try:
            selected_items = self.conn_tree.selection()
            if selected_items:
                item = selected_items[0]
                values = self.conn_tree.item(item)['values']
                if values:
                    ip = values[0]
                    
                    if ip in self.blacklist:
                        self.blacklist.remove(ip)
                        self.proxy_server.blacklist = self.blacklist.copy()  # 更新代理服务器的黑名单
                        self.save_current_config()
                        self.add_log(f"✅ 已将IP {ip} 从黑名单移除", 'info')
                        messagebox.showinfo("提示", f"已将IP {ip} 从黑名单移除")
                    else:
                        messagebox.showwarning("提示", f"IP {ip} 不在黑名单中")
            else:
                messagebox.showwarning("提示", "请先选择一个连接")
                
        except Exception as e:
            messagebox.showerror("错误", f"从黑名单移除失败: {str(e)}")
            
    def show_current_config(self):
        """显示当前配置信息（从JSON文件重新加载最新配置）"""
        try:
            # 从JSON文件重新加载最新配置
            latest_config = self.config_manager.load_config()
            
            config_text = "当前配置信息（从配置文件读取）：\n\n"
            config_text += f"主机地址: {latest_config.get('host', 'N/A')}:{latest_config.get('port', 'N/A')}\n"
            config_text += f"端口: {latest_config.get('port', 'N/A')}\n"
            config_text += f"缓冲区大小: {latest_config.get('buffer_size', 'N/A')} 字节\n"
            config_text += f"延迟: {latest_config.get('delay', 'N/A')} ms\n"
            config_text += f"最大客户端数: {latest_config.get('max_clients', 'N/A')}\n"
            config_text += f"自动启动: {'是' if latest_config.get('auto_start', False) else '否'}\n"
            config_text += f"服务器运行状态: {'运行中' if latest_config.get('server_running', False) else '已停止'}\n"
            
            # 显示黑名单信息
            blacklist = latest_config.get('blacklist', [])
            config_text += f"黑名单IP数量: {len(blacklist)}\n"
            if blacklist:
                config_text += f"黑名单IP: {', '.join(blacklist[:5])}"
                if len(blacklist) > 5:
                    config_text += f" 等{len(blacklist)-5}个"
                config_text += "\n"
            
            last_start = latest_config.get('last_start_time')
            if last_start:
                try:
                    last_start_dt = datetime.fromisoformat(last_start)
                    time_str = last_start_dt.strftime("%Y-%m-%d %H:%M:%S")
                    config_text += f"上次启动时间: {time_str}\n"
                except:
                    config_text += "上次启动时间: 未知\n"
            else:
                config_text += "上次启动时间: 从未启动\n"
                
            config_text += f"\n配置文件: {self.config_manager.config_file}"
            if os.path.exists(self.config_manager.backup_file):
                config_text += f"\n备份文件: {self.config_manager.backup_file}"
            
            # 添加内存配置和文件配置的对比信息
            if self.config != latest_config:
                config_text += "\n\n⚠️  注意：内存中的配置与配置文件不一致！"
                config_text += "\n建议保存当前配置以保持一致性。"
            else:
                config_text += "\n\n✅ 内存配置与配置文件一致"
                
            messagebox.showinfo("当前配置", config_text)
            
        except Exception as e:
            messagebox.showerror("错误", f"显示配置信息失败: {str(e)}")
        
    def setup_styles(self):
        """设置界面样式"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # 配置颜色
        self.colors = {
            'bg': '#ffffff',
            'fg': '#333333',
            'accent': '#007acc',
            'success': '#28a745',
            'danger': '#dc3545',
            'warning': '#ffc107',
            'info': '#17a2b8',
            'light_gray': '#f8f9fa',
            'border': '#dee2e6'
        }
        
        # 配置全局样式
        self.root.configure(bg=self.colors['light_gray'])
        
        # 配置各种组件样式
        style.configure('Main.TFrame', background=self.colors['light_gray'])
        style.configure('Control.TLabelframe', background=self.colors['bg'], 
                       relief='solid', borderwidth=1)
        style.configure('Control.TLabelframe.Label', font=('Arial', 11, 'bold'))
        
        # 标签样式
        style.configure('TLabel', background=self.colors['bg'], foreground=self.colors['fg'])
        
        # 按钮样式
        style.configure('Success.TButton', 
                       foreground=self.colors['success'], 
                       font=('Arial', 10, 'bold'),
                       padding=6)
        style.configure('Danger.TButton', 
                       foreground=self.colors['danger'], 
                       font=('Arial', 10, 'bold'),
                       padding=6)
        
        # 输入框样式
        style.configure('TEntry', fieldbackground='white', 
                       foreground=self.colors['fg'],
                       borderwidth=1,
                       relief='solid')
        
        # 树形视图样式
        style.configure('Conn.Treeview', 
                       background='white',
                       fieldbackground='white',
                       foreground=self.colors['fg'],
                       rowheight=22,
                       font=('Arial', 9))
        style.configure('Conn.Treeview.Heading',
                       background=self.colors['light_gray'],
                       foreground=self.colors['fg'],
                       font=('Arial', 9, 'bold'))
        
        # 标签框架样式
        style.configure('TLabelframe', 
                       background=self.colors['bg'],
                       relief='solid',
                       borderwidth=1)
        style.configure('TLabelframe.Label',
                       background=self.colors['bg'],
                       foreground=self.colors['fg'],
                       font=('Arial', 11, 'bold'))
        
    def center_window(self):
        """将窗口居中显示"""
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
        
    def create_widgets(self):
        """创建界面组件"""
        # 创建主框架 - 紧凑内边距
        main_frame = ttk.Frame(self.root, padding="15", style='Main.TFrame')
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)  # 左右平分
        main_frame.rowconfigure(1, weight=1)
        
        # 控制面板 - 顶部横跨两列
        self.create_control_panel(main_frame)
        
        # 左侧面板 - 状态和日志（增加权重）
        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10), pady=(0, 0))
        left_frame.columnconfigure(0, weight=1)
        left_frame.rowconfigure(1, weight=3)  # 日志占更多空间
        
        # 状态面板
        self.create_status_panel(left_frame)
        
        # 日志面板
        self.create_log_panel(left_frame)
        
        # 右侧面板 - 流量图表和连接列表
        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(0, weight=2)  # 流量图表占2/3
        right_frame.rowconfigure(1, weight=1)  # 连接列表占1/3
        
        # 流量图表面板
        self.create_traffic_panel(right_frame)
        
        # 连接列表面板
        self.create_connections_panel(right_frame)
        
    def create_control_panel(self, parent):
        """创建控制面板"""
        control_frame = ttk.LabelFrame(parent, text="服务器控制", padding="10", style='Control.TLabelframe')
        control_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # 紧凑的单行设置
        settings_frame = ttk.Frame(control_frame)
        settings_frame.pack(fill=tk.X, pady=(0, 5))
        
        # 紧凑的标签样式
        label_style = {'sticky': tk.E, 'padx': (0, 3), 'pady': 2}
        entry_style = {'sticky': tk.W, 'padx': (0, 10), 'pady': 2}
        
        # 所有设置项放在一行
        ttk.Label(settings_frame, text="主机:").grid(row=0, column=0, **label_style)
        self.host_var = tk.StringVar(value="0.0.0.0")
        self.host_entry = ttk.Entry(settings_frame, textvariable=self.host_var, width=12)
        self.host_entry.grid(row=0, column=1, **entry_style)
        
        ttk.Label(settings_frame, text="端口:").grid(row=0, column=2, **label_style)
        self.port_var = tk.StringVar(value="7890")
        self.port_entry = ttk.Entry(settings_frame, textvariable=self.port_var, width=6)
        self.port_entry.grid(row=0, column=3, **entry_style)
        
        ttk.Label(settings_frame, text="最大客户端:").grid(row=0, column=4, **label_style)
        self.max_clients_var = tk.StringVar(value="100")
        self.max_clients_entry = ttk.Entry(settings_frame, textvariable=self.max_clients_var, width=6)
        self.max_clients_entry.grid(row=0, column=5, **entry_style)
        
        ttk.Label(settings_frame, text="缓冲区:").grid(row=0, column=6, **label_style)
        self.buffer_var = tk.StringVar(value="1024")
        self.buffer_entry = ttk.Entry(settings_frame, textvariable=self.buffer_var, width=8)
        self.buffer_entry.grid(row=0, column=7, **entry_style)
        
        ttk.Label(settings_frame, text="延迟(ms):").grid(row=0, column=8, **label_style)
        self.delay_var = tk.StringVar(value="1")
        self.delay_entry = ttk.Entry(settings_frame, textvariable=self.delay_var, width=6)
        self.delay_entry.grid(row=0, column=9, **entry_style)
        
        # 控制按钮 - 紧凑排列
        button_frame = ttk.Frame(control_frame)
        button_frame.pack(fill=tk.X, pady=(5, 0))
        
        style = ttk.Style()
        style.configure('Success.TButton', foreground=self.colors['success'], font=('Arial', 9))
        style.configure('Danger.TButton', foreground=self.colors['danger'], font=('Arial', 9))
        
        self.start_button = ttk.Button(
            button_frame, 
            text="▶ 启动", 
            command=self.start_server,
            style='Success.TButton'
        )
        self.start_button.pack(side=tk.LEFT, padx=(0, 5))
        
        self.stop_button = ttk.Button(
            button_frame, 
            text="⏹ 停止", 
            command=self.stop_server,
            state=tk.DISABLED,
            style='Danger.TButton'
        )
        self.stop_button.pack(side=tk.LEFT)
        
        # 自动启动复选框
        self.auto_start_var = tk.BooleanVar(value=self.config.get('auto_start', False))
        self.auto_start_check = ttk.Checkbutton(
            button_frame,
            text="自动启动",
            variable=self.auto_start_var,
            command=self.on_auto_start_changed
        )
        self.auto_start_check.pack(side=tk.LEFT, padx=(20, 0))
        
    def create_status_panel(self, parent):
        """创建状态面板"""
        status_frame = ttk.LabelFrame(parent, text="服务器状态", padding="10")
        status_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 状态指示器 - 居中显示
        self.status_label = ttk.Label(
            status_frame, 
            text="● 已停止",
            foreground=self.colors['danger'],
            font=('Arial', 12, 'bold')
        )
        self.status_label.grid(row=0, column=0, columnspan=4, pady=(5, 5))
        
        # 上次运行时间显示 - 更醒目的样式
        self.last_run_label = ttk.Label(
            status_frame,
            text="上次运行: 从未运行",
            foreground=self.colors['info'],
            font=('Arial', 9, 'italic')
        )
        self.last_run_label.grid(row=1, column=0, columnspan=4, pady=(0, 5))
        
        # 运行时长显示
        self.run_duration_label = ttk.Label(
            status_frame,
            text="运行时长: 0小时0分钟",
            foreground=self.colors['accent'],
            font=('Arial', 8)
        )
        self.run_duration_label.grid(row=2, column=0, columnspan=4, pady=(0, 5))
        
        # 黑名单数量显示
        self.blacklist_count_label = ttk.Label(
            status_frame,
            text="黑名单: 0个IP",
            foreground=self.colors['danger'],
            font=('Arial', 8)
        )
        self.blacklist_count_label.grid(row=3, column=0, columnspan=4, pady=(0, 10))
        
        # 统计信息 - 两列布局
        stats_items = [
            ('总连接数:', 'total_connections'),
            ('活跃连接:', 'active_connections'),
            ('发送流量:', 'total_bytes_sent'),
            ('接收流量:', 'total_bytes_received')
        ]
        
        self.stats_labels = {}
        
        # 两列布局，每行两个项目（总共2行，4个项目）
        for i, (label, key) in enumerate(stats_items):
            row = i // 2 + 4  # 从第4行开始，因为第0行是状态指示器，第1行是上次运行时间，第2行是运行时长，第3行是黑名单数量
            col = (i % 2) * 2  # 0, 2, 0, 2 列模式
            
            ttk.Label(status_frame, text=label, font=('Arial', 9)).grid(row=row, column=col, sticky=tk.E, padx=(0, 3), pady=2)
            self.stats_labels[key] = ttk.Label(status_frame, text="0", font=('Arial', 9, 'bold'))
            self.stats_labels[key].grid(row=row, column=col+1, sticky=tk.W, padx=(0, 10), pady=2)
            
    def create_log_panel(self, parent):
        """创建日志面板"""
        log_frame = ttk.LabelFrame(parent, text="实时日志", padding="10")
        log_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 0))
        
        # 提升日志文本框高度
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            height=22,  # 增加高度
            width=50,
            font=('Consolas', 9),
            bg='#ffffff',
            fg='#333333',
            relief='flat',
            borderwidth=1
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 日志标签颜色
        self.log_text.tag_configure('connection', foreground=self.colors['success'])
        self.log_text.tag_configure('error', foreground=self.colors['danger'])
        self.log_text.tag_configure('info', foreground=self.colors['accent'])
        
    def create_connections_panel(self, parent):
        """创建连接列表面板"""
        conn_frame = ttk.LabelFrame(parent, text="当前连接", padding="10")
        conn_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        
        # 连接数量和操作按钮容器
        header_frame = ttk.Frame(conn_frame)
        header_frame.pack(fill=tk.X, pady=(0, 5))
        
        # 连接数量标签
        self.conn_count_label = ttk.Label(header_frame, text="当前连接: 0", font=('Arial', 10, 'bold'))
        self.conn_count_label.pack(side=tk.LEFT)
        
        # 刷新按钮
        refresh_btn = ttk.Button(header_frame, text="🔄 刷新", command=self.refresh_connections, width=6)
        refresh_btn.pack(side=tk.RIGHT, padx=(5, 0))
        
        # 连接列表 - 减少高度
        self.conn_tree = ttk.Treeview(
            conn_frame, 
            columns=('Address', 'Port', 'Protocol', 'Duration', 'Traffic'),
            height=6,  # 进一步减少高度
            show='headings',
            style='Conn.Treeview'
        )
        
        # 配置连接列表样式
        style = ttk.Style()
        style.configure('Conn.Treeview', rowheight=20, font=('Arial', 8))
        style.configure('Conn.Treeview.Heading', font=('Arial', 8, 'bold'))
        
        # 配置列标题和宽度
        columns_config = [
            ('Address', 'IP地址', 80),
            ('Port', '端口', 45),
            ('Protocol', '协议', 45),
            ('Duration', '时长', 60),
            ('Traffic', '流量', 55)
        ]
        
        for col, text, width in columns_config:
            self.conn_tree.heading(col, text=text)
            self.conn_tree.column(col, width=width, anchor=tk.CENTER)
        
        # 滚动条
        scrollbar = ttk.Scrollbar(conn_frame, orient=tk.VERTICAL, command=self.conn_tree.yview)
        self.conn_tree.configure(yscrollcommand=scrollbar.set)
        
        self.conn_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 添加右键菜单
        self.conn_menu = tk.Menu(self.conn_tree, tearoff=0, font=('Arial', 8))
        self.conn_menu.add_command(label="断开", command=self.disconnect_connection)
        self.conn_menu.add_command(label="复制IP", command=self.copy_connection_ip)
        self.conn_menu.add_separator()
        self.conn_menu.add_command(label="加入黑名单", command=self.add_to_blacklist)
        self.conn_menu.add_command(label="从黑名单移除", command=self.remove_from_blacklist)
        self.conn_menu.add_separator()
        self.conn_menu.add_command(label="详情", command=self.view_connection_details)
        
        self.conn_tree.bind("<Button-3>", self.show_connection_menu)
        
    def create_traffic_panel(self, parent):
        """创建流量监控面板"""
        traffic_frame = ttk.LabelFrame(parent, text="流量监控", padding="10")
        traffic_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 创建图表容器
        chart_container = ttk.Frame(traffic_frame, relief='flat', borderwidth=1)
        chart_container.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
        
        # 创建自定义流量图表
        self.traffic_canvas = TrafficCanvas(chart_container, width=500, height=250)
        self.traffic_canvas.pack(fill=tk.BOTH, expand=True)
        
        # 流量统计标签 - 紧凑布局
        stats_frame = ttk.Frame(traffic_frame)
        stats_frame.pack(fill=tk.X, padx=5)
        
        # 当前速率标签
        current_frame = ttk.LabelFrame(stats_frame, text="当前速率", padding="5")
        current_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 8))
        
        self.traffic_labels = {
            'current_sent': ttk.Label(current_frame, text="发送: 0 B/s", font=('Arial', 9)),
            'current_received': ttk.Label(current_frame, text="接收: 0 B/s", font=('Arial', 9))
        }
        
        self.traffic_labels['current_sent'].grid(row=0, column=0, sticky=tk.W, pady=1)
        self.traffic_labels['current_received'].grid(row=1, column=0, sticky=tk.W, pady=1)
        
        # 峰值速率标签
        peak_frame = ttk.LabelFrame(stats_frame, text="峰值速率", padding="5")
        peak_frame.grid(row=0, column=1, sticky=(tk.W, tk.E))
        
        self.traffic_labels['peak_sent'] = ttk.Label(peak_frame, text="发送: 0 B/s", font=('Arial', 9))
        self.traffic_labels['peak_received'] = ttk.Label(peak_frame, text="接收: 0 B/s", font=('Arial', 9))
        
        self.traffic_labels['peak_sent'].grid(row=0, column=0, sticky=tk.W, pady=1)
        self.traffic_labels['peak_received'].grid(row=1, column=0, sticky=tk.W, pady=1)
        
        # 配置列权重
        stats_frame.columnconfigure(0, weight=1)
        stats_frame.columnconfigure(1, weight=1)
        
    def start_server(self):
        """启动服务器"""
        try:
            host = self.host_var.get()
            port = int(self.port_var.get())
            buffer_size = int(self.buffer_var.get())
            delay = int(self.delay_var.get())
            max_clients = int(self.max_clients_var.get())

            # 记录启动时间（在启动前记录）
            self.last_start_time = datetime.now().isoformat()
            self.add_log(f"正在启动服务器，时间: {self.last_start_time}", 'info')
            
            # 立即更新界面上的上次运行时间显示
            if hasattr(self, 'last_run_label'):
                current_time = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
                self.last_run_label.config(text=f"上次运行: {current_time}")

            if self.proxy_server.start(host, port, buffer_size, delay, max_clients):
                # 保存当前配置（包含服务器运行状态和启动时间）
                current_config = self.config_manager.get_current_config(self)
                current_config['server_running'] = True
                current_config['last_start_time'] = self.last_start_time
                self.config_manager.save_config(current_config)
                self.add_log("配置已保存", 'info')

                # 重置速率统计（启动时清零峰值与基线）
                self._reset_rate_stats()

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
        """停止服务器"""
        try:
            self.proxy_server.stop()

                # 保存当前配置（包括停止状态，但保留上次启动时间）
            current_config = self.config_manager.get_current_config(self)
            current_config['server_running'] = False
            # 保留上次启动时间，不要清空它
            if hasattr(self, 'last_start_time') and self.last_start_time:
                current_config['last_start_time'] = self.last_start_time
            self.config_manager.save_config(current_config)

            # 重置速率统计（停止时清零当前/峰值显示）
            self._reset_rate_stats()

            # 清空连接列表（停止后不再有 stats_queue 推送，需主动刷新一次）
            try:
                self.proxy_server.clients.clear()
            except Exception:
                pass
            self.update_connections_list()

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
        """添加日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n", log_type)
        self.log_text.see(tk.END)
        
    def update_gui(self):
        """更新GUI界面"""
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
        except Exception as e:
            # 单次更新中的异常不应打断整个刷新循环
            # （否则连接列表/统计/日志会全部冻结）
            print(f"update_gui 单次刷新出错（已忽略）: {e}")

        # 继续更新
        self.root.after(100, self.update_gui)
        
    def load_saved_config(self):
        """加载保存的配置到界面"""
        try:
            # 设置基本配置
            self.host_var.set(self.config.get('host', '0.0.0.0'))
            self.port_var.set(str(self.config.get('port', 7890)))
            self.buffer_var.set(str(self.config.get('buffer_size', 1024)))
            self.delay_var.set(str(self.config.get('delay', 1)))
            self.max_clients_var.set(str(self.config.get('max_clients', 100)))
            
            # 设置自动启动复选框（如果存在）
            if hasattr(self, 'auto_start_var'):
                self.auto_start_var.set(self.config.get('auto_start', False))
            
            # 加载上次启动时间
            self.last_start_time = self.config.get('last_start_time')
            
            # 加载黑名单
            self.blacklist = self.config.get('blacklist', [])
            
            # 更新黑名单数量显示
            if hasattr(self, 'blacklist_count_label'):
                blacklist_count = len(self.blacklist)
                self.blacklist_count_label.config(text=f"黑名单: {blacklist_count}个IP")
            
            # 更新上次运行时间标签并显示详细信息
            if self.last_start_time:
                try:
                    last_start_dt = datetime.fromisoformat(self.last_start_time)
                    formatted_time = last_start_dt.strftime("%Y年%m月%d日 %H:%M:%S")
                    self.last_run_label.config(text=f"上次运行: {formatted_time}")
                    
                    # 计算距离现在的时间
                    time_diff = datetime.now() - last_start_dt
                    days = time_diff.days
                    hours = time_diff.seconds // 3600
                    minutes = (time_diff.seconds % 3600) // 60
                    
                    if days > 0:
                        time_ago = f"{days}天{hours}小时前"
                    elif hours > 0:
                        time_ago = f"{hours}小时{minutes}分钟前"
                    elif minutes > 0:
                        time_ago = f"{minutes}分钟前"
                    else:
                        time_ago = "刚刚"
                        
                    self.add_log(f"🕐 上次运行时间: {formatted_time}", 'info')
                    self.add_log(f"⏰ 距离现在: {time_ago}", 'info')
                    
                    # 显示服务器状态信息
                    if self.config.get('server_running', False):
                        self.add_log("⚠️  注意: 上次程序异常关闭时服务器正在运行", 'warning')
                    else:
                        self.add_log("✅ 上次程序正常关闭", 'info')
                        
                except Exception as e:
                    self.last_run_label.config(text=f"上次运行: {self.last_start_time}")
                    self.add_log(f"上次启动时间: {self.last_start_time}", 'info')
            else:
                self.last_run_label.config(text="上次运行: 从未运行")
                self.add_log("📅 这是您第一次运行本程序", 'info')
            
            # 显示配置加载信息
            if os.path.exists(self.config_manager.config_file):
                self.add_log("🚀 网络代理服务器启动中...", 'info')
                self.add_log("📦 版本: v2.0 - 支持配置保存和自动恢复功能", 'info')
                self.add_log("已加载保存的配置", 'info')
                
                # 显示配置摘要
                config_summary = f"配置摘要 - 主机: {self.config.get('host')}:{self.config.get('port')}"
                config_summary += f", 缓冲区: {self.config.get('buffer_size')}字节"
                config_summary += f", 最大客户端: {self.config.get('max_clients')}"
                config_summary += f", 自动启动: {'开启' if self.config.get('auto_start') else '关闭'}"
                self.add_log(config_summary, 'info')
                
                if self.config.get('server_running', False):
                    self.add_log("⚠️  检测到上次异常关闭，准备自动恢复...", 'warning')
            else:
                self.add_log("🎉 欢迎使用网络代理服务器！", 'info')
                self.add_log("📦 版本: v2.0 - 支持配置保存和自动恢复功能", 'info')
                self.add_log("💡 这是您第一次运行本程序，将使用默认配置", 'info')
                self.add_log("🔧 您可以通过菜单栏保存和恢复配置", 'info')
                
        except Exception as e:
            print(f"加载配置到界面失败: {e}")
            self.add_log(f"加载配置失败: {str(e)}", 'error')
            
    def save_current_config(self):
        """保存当前配置"""
        try:
            current_config = self.config_manager.get_current_config(self)
            if self.config_manager.save_config(current_config):
                self.add_log("配置已保存", 'info')
                # 显示保存成功的视觉反馈
                self.show_save_feedback()
                return True
            return False
        except Exception as e:
            print(f"保存配置失败: {e}")
            self.add_log(f"保存配置失败: {str(e)}", 'error')
            return False
            
    def show_save_feedback(self):
        """显示保存成功的视觉反馈"""
        try:
            # 创建临时标签显示保存成功
            feedback_label = ttk.Label(
                self.root, 
                text="✓ 配置已保存", 
                foreground=self.colors['success'],
                font=('Arial', 9)
            )
            feedback_label.place(relx=0.98, rely=0.02, anchor='ne')
            
            # 2秒后移除标签
            self.root.after(2000, feedback_label.destroy)
        except:
            pass
            
    def auto_start_server(self):
        """自动启动服务器"""
        try:
            # 记录自动启动时间
            self.last_start_time = datetime.now().isoformat()
            self.add_log(f"正在自动启动服务器... 时间: {self.last_start_time}", 'info')
            
            # 立即更新界面上的上次运行时间显示
            if hasattr(self, 'last_run_label'):
                current_time = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
                self.last_run_label.config(text=f"上次运行: {current_time}")
                
            self.start_server()
        except Exception as e:
            self.add_log(f"自动启动服务器失败: {str(e)}", 'error')
            
    def on_auto_start_changed(self):
        """自动启动选项变更处理"""
        try:
            # 立即保存自动启动设置
            current_config = self.config_manager.get_current_config(self)
            self.config_manager.save_config(current_config)
            status = "启用" if self.auto_start_var.get() else "禁用"
            self.add_log(f"自动启动已{status}", 'info')
        except Exception as e:
            print(f"保存自动启动设置失败: {e}")
            
    def on_closing(self):
        """窗口关闭时的处理（点关闭按钮=直接关闭程序）"""
        # 防止重复触发
        if getattr(self, '_tray_quitting', False):
            return
        self._tray_quitting = True
        try:
            # 先停止托盘图标
            self._stop_tray_icon()
            # 保存当前配置
            self.save_current_config()
            # 如果服务器正在运行，先停止它
            if self.proxy_server.running:
                self.proxy_server.stop()
            # 销毁窗口
            self.root.destroy()
        except Exception as e:
            print(f"关闭窗口时出错: {e}")
            self.root.destroy()

    # ---------------- 系统托盘功能 ----------------

    def _on_unmap(self, event):
        """窗口最小化时缩小到系统托盘"""
        if event.widget is not self.root:
            return
        if self._tray_hiding or self._tray_quitting:
            return
        try:
            # 只有真正最小化（iconic 状态）才进托盘
            # Windows 下 state() 返回 'iconic'，部分平台为 'icon'
            if str(self.root.state()) in ('icon', 'iconic'):
                self._tray_hiding = True
                self.root.withdraw()  # 隐藏窗口到托盘
                self._show_tray_icon()
        except Exception:
            self._tray_hiding = False

    def _create_tray_image(self):
        """生成托盘图标（无图标文件时用PIL绘制）"""
        try:
            size = 64
            img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)
            # 蓝色圆角背景
            margin = 4
            draw.rounded_rectangle(
                [margin, margin, size - margin, size - margin],
                radius=12, fill=(0, 122, 204, 255)
            )
            # 白色双向箭头表示代理转发
            cx, cy = size // 2, size // 2
            # 上行箭头
            draw.line([(cx - 14, cy + 6), (cx - 14, cy - 8)], fill='white', width=4)
            draw.polygon(
                [(cx - 14, cy - 12), (cx - 19, cy - 5), (cx - 9, cy - 5)],
                fill='white'
            )
            # 下行箭头
            draw.line([(cx + 14, cy - 6), (cx + 14, cy + 8)], fill='white', width=4)
            draw.polygon(
                [(cx + 14, cy + 12), (cx + 9, cy + 5), (cx + 19, cy + 5)],
                fill='white'
            )
            return img
        except Exception:
            return None

    def _tray_setup(self, icon):
        """托盘图标就绪后回调：显示图标并弹出气泡提示"""
        try:
            icon.visible = True
        except Exception:
            pass
        try:
            # 弹出气泡通知用户已最小化到托盘
            icon.notify(
                "程序已最小化到系统托盘，双击图标或右键菜单可恢复窗口",
                "网络代理服务器",
            )
        except Exception:
            pass

    def _show_tray_icon(self):
        """显示系统托盘图标"""
        if not TRAY_AVAILABLE:
            # 无依赖则退化为普通最小化
            self._tray_hiding = False
            try:
                self.root.deiconify()
                self.root.iconify()
            except Exception:
                pass
            return
        if self._tray_icon is not None:
            return  # 已显示
        try:
            image = self._create_tray_image()
            menu = pystray.Menu(
                pystray.MenuItem("显示主窗口", self._tray_show_window, default=True),
                pystray.MenuItem("退出程序", self._tray_quit_app),
            )
            self._tray_icon = pystray.Icon(
                "ProxyServerTray",
                image,
                "网络代理服务器",
                menu,
            )
            # 在后台线程运行托盘消息循环，并传入 setup 回调以在就绪后弹气泡
            threading.Thread(
                target=self._tray_icon.run,
                args=(self._tray_setup,),
                daemon=True,
            ).start()
        except Exception as e:
            print(f"启动托盘图标失败: {e}")
            self._tray_icon = None
            self._tray_hiding = False

    def _stop_tray_icon(self):
        """停止托盘图标"""
        if self._tray_icon is not None:
            try:
                self._tray_icon.stop()
            except Exception:
                pass
            self._tray_icon = None

    def _tray_show_window(self, icon=None, item=None):
        """从托盘恢复显示主窗口（在托盘线程中调用）"""
        # 必须在Tk主线程中操作UI
        self.root.after(0, self._restore_window)

    def _restore_window(self):
        """恢复主窗口"""
        self._tray_hiding = False
        try:
            self._stop_tray_icon()
            self.root.deiconify()
            self.root.lift()
            self.root.focus_force()
        except Exception:
            pass

    def _tray_quit_app(self, icon=None, item=None):
        """从托盘退出程序（在托盘线程中调用）"""
        # 必须在Tk主线程中关闭
        self.root.after(0, self.on_closing)

    def update_stats(self, stats):
        """更新统计信息"""
        # 更新统计标签
        self.stats_labels['total_connections'].config(text=str(stats['total_connections']))
        self.stats_labels['active_connections'].config(text=str(stats['active_connections']))
        self.stats_labels['total_bytes_sent'].config(text=self.format_bytes(stats['total_bytes_sent']))
        self.stats_labels['total_bytes_received'].config(text=self.format_bytes(stats['total_bytes_received']))
        
        # 更新运行时间（仅用于显示当前运行时长）
        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            uptime_str = str(uptime).split('.')[0]
            
            # 更新运行时长标签
            if hasattr(self, 'run_duration_label'):
                total_seconds = int(uptime.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                
                if hours > 0:
                    duration_text = f"运行时长: {hours}小时{minutes}分钟"
                elif minutes > 0:
                    duration_text = f"运行时长: {minutes}分钟{seconds}秒"
                else:
                    duration_text = f"运行时长: {seconds}秒"
                    
                self.run_duration_label.config(text=duration_text)
        else:
            # 服务器未运行时的处理
            if hasattr(self, 'run_duration_label'):
                self.run_duration_label.config(text="运行时长: 未运行")
        
        # 更新连接列表
        self.update_connections_list()
        
        # 更新流量图表
        self.update_traffic_chart(stats)
        
    def update_connections_list(self):
        """更新连接列表"""
        # 清除现有项目
        for item in self.conn_tree.get_children():
            self.conn_tree.delete(item)

        # 更新连接数量
        conn_count = len(self.proxy_server.clients)
        self.conn_count_label.config(text=f"当前连接: {conn_count}")

        # 更新黑名单数量
        blacklist_count = len(self.blacklist) if hasattr(self, 'blacklist') else 0
        if hasattr(self, 'blacklist_count_label'):
            self.blacklist_count_label.config(text=f"黑名单: {blacklist_count}个IP")

        # 添加活跃连接
        # 注意：self.proxy_server.clients 会被工作线程在连接关闭时增删，
        # 直接 .items() 迭代可能抛出 RuntimeError: dictionary changed size
        # during iteration，从而打断整个 update_gui 循环（update_gui 只捕获
        # queue.Empty），导致连接列表/统计/日志全部冻结。这里取快照避免竞争。
        try:
            clients_snapshot = list(self.proxy_server.clients.items())
        except RuntimeError:
            clients_snapshot = []

        for addr, info in clients_snapshot:
            try:
                protocol = info.get('protocol', 'Unknown')

                # 计算连接时长
                duration = datetime.now() - info['connect_time']
                duration_str = str(duration).split('.')[0]  # 移除微秒

                # 计算流量
                total_traffic = info.get('bytes_sent', 0) + info.get('bytes_received', 0)
                traffic_str = self.format_bytes(total_traffic)

                self.conn_tree.insert('', tk.END, values=(
                    addr[0],
                    addr[1],
                    protocol,
                    duration_str,
                    traffic_str
                ))
            except Exception:
                # 单条连接信息异常时跳过，不影响整体刷新
                continue
            
    def show_connection_menu(self, event):
        """显示连接列表右键菜单"""
        item = self.conn_tree.identify_row(event.y)
        if item:
            self.conn_tree.selection_set(item)
            
            # 获取选中的IP
            values = self.conn_tree.item(item)['values']
            if values:
                ip = values[0]
                
                # 动态更新菜单项状态
                self.conn_menu.delete(0, tk.END)  # 清空菜单
                
                # 添加基本菜单项
                self.conn_menu.add_command(label="断开", command=self.disconnect_connection)
                self.conn_menu.add_command(label="复制IP", command=self.copy_connection_ip)
                self.conn_menu.add_separator()
                
                # 根据IP是否在黑名单中添加相应菜单项
                if ip in self.blacklist:
                    self.conn_menu.add_command(label="从黑名单移除", command=self.remove_from_blacklist)
                else:
                    self.conn_menu.add_command(label="加入黑名单", command=self.add_to_blacklist)
                
                self.conn_menu.add_separator()
                self.conn_menu.add_command(label="详情", command=self.view_connection_details)
            
            self.conn_menu.post(event.x_root, event.y_root)
            
    def disconnect_connection(self):
        """断开选中的连接"""
        selected_items = self.conn_tree.selection()
        if selected_items:
            for item in selected_items:
                values = self.conn_tree.item(item)['values']
                if values:
                    ip, port = values[0], values[1]
                    # 查找对应的连接并关闭
                    for addr, info in list(self.proxy_server.clients.items()):
                        if addr[0] == ip and str(addr[1]) == str(port):
                            try:
                                info['socket'].close()
                                self.add_log(f"已断开连接 {ip}:{port}", 'info')
                            except Exception as e:
                                self.add_log(f"断开连接失败 {ip}:{port}: {str(e)}", 'error')
                            break
                            
    def copy_connection_ip(self):
        """复制选中连接的IP地址"""
        selected_items = self.conn_tree.selection()
        if selected_items:
            item = selected_items[0]
            values = self.conn_tree.item(item)['values']
            if values:
                ip = values[0]
                self.root.clipboard_clear()
                self.root.clipboard_append(ip)
                self.add_log(f"已复制IP地址: {ip}", 'info')
                
    def refresh_connections(self):
        """刷新连接列表"""
        self.update_connections_list()
        self.add_log("连接列表已刷新", 'info')
        
    def view_connection_details(self):
        """查看连接详情"""
        selected_items = self.conn_tree.selection()
        if selected_items:
            item = selected_items[0]
            values = self.conn_tree.item(item)['values']
            if values:
                ip, port, protocol, duration, traffic = values
                details = f"连接详情：\n"
                details += f"IP地址: {ip}\n"
                details += f"端口: {port}\n"
                details += f"协议: {protocol}\n"
                details += f"连接时长: {duration}\n"
                details += f"流量: {traffic}"
                messagebox.showinfo("连接详情", details)
            
    def update_traffic_chart(self, stats):
        """更新流量图表"""
        # 更新图表数据
        self.traffic_canvas.update_traffic(stats['total_bytes_sent'], stats['total_bytes_received'])

        # 计算当前流量速率：基于总字节数的增量 / 采样间隔时间
        now = datetime.now()
        current_sent_rate = 0.0
        current_received_rate = 0.0

        if self._rate_last_time is not None:
            elapsed = (now - self._rate_last_time).total_seconds()
            if elapsed > 0:
                sent_delta = stats['total_bytes_sent'] - self._rate_last_sent
                received_delta = stats['total_bytes_received'] - self._rate_last_received
                # 增量为负说明计数被重置（如重启），此时不计算本次速率
                if sent_delta >= 0:
                    current_sent_rate = sent_delta / elapsed
                if received_delta >= 0:
                    current_received_rate = received_delta / elapsed

        # 更新采样基线
        self._rate_last_time = now
        self._rate_last_sent = stats['total_bytes_sent']
        self._rate_last_received = stats['total_bytes_received']

        # 更新峰值速率
        if current_sent_rate > self._peak_sent_rate:
            self._peak_sent_rate = current_sent_rate
        if current_received_rate > self._peak_received_rate:
            self._peak_received_rate = current_received_rate

        # 更新流量标签
        self.traffic_labels['current_sent'].config(
            text=f"发送: {self.format_bytes(current_sent_rate)}/s")
        self.traffic_labels['current_received'].config(
            text=f"接收: {self.format_bytes(current_received_rate)}/s")
        self.traffic_labels['peak_sent'].config(
            text=f"发送: {self.format_bytes(self._peak_sent_rate)}/s")
        self.traffic_labels['peak_received'].config(
            text=f"接收: {self.format_bytes(self._peak_received_rate)}/s")

    def _reset_rate_stats(self):
        """重置速率统计（在服务器启动/停止时调用）"""
        self._rate_last_time = None
        self._rate_last_sent = 0
        self._rate_last_received = 0
        self._peak_sent_rate = 0.0
        self._peak_received_rate = 0.0
        # 重置界面显示
        if hasattr(self, 'traffic_labels'):
            try:
                self.traffic_labels['current_sent'].config(text="发送: 0 B/s")
                self.traffic_labels['current_received'].config(text="接收: 0 B/s")
                self.traffic_labels['peak_sent'].config(text="发送: 0 B/s")
                self.traffic_labels['peak_received'].config(text="接收: 0 B/s")
            except Exception:
                pass
        
    def format_bytes(self, bytes):
        """格式化字节数显示"""
        if bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024.0:
                return f"{bytes:.1f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.1f} TB"


def main():
    """主函数"""
    root = tk.Tk()
    app = ProxyGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()