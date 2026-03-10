#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SOCKS5代理测试脚本
"""

import socket
import struct
import sys

def test_socks5_proxy(host='127.0.0.1', port=7890, target_host='httpbin.org', target_port=80):
    """
    测试SOCKS5代理连接
    """
    try:
        # 创建socket连接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        
        print(f"正在连接到SOCKS5代理 {host}:{port}")
        sock.connect((host, port))
        print("连接成功！")
        
        # SOCKS5握手阶段
        print("发送SOCKS5握手请求...")
        # VER=5, NMETHODS=1, METHODS=[0] (无认证)
        handshake_request = b'\x05\x01\x00'
        sock.send(handshake_request)
        
        # 接收握手响应
        handshake_response = sock.recv(2)
        if len(handshake_response) != 2:
            print("握手响应长度错误")
            return False
            
        if handshake_response[0] != 0x05:
            print(f"SOCKS5版本错误: {handshake_response[0]}")
            return False
            
        if handshake_response[1] != 0x00:
            print(f"认证方法不被接受: {handshake_response[1]}")
            return False
            
        print("SOCKS5握手成功！")
        
        # SOCKS5连接请求阶段
        print(f"发送连接请求到 {target_host}:{target_port}")
        
        # 将域名转换为字节
        target_host_bytes = target_host.encode('utf-8')
        host_len = len(target_host_bytes)
        
        # VER=5, CMD=1 (CONNECT), RSV=0, ATYP=3 (域名)
        # DST.ADDR=域名长度+域名, DST.PORT=端口
        connect_request = struct.pack('!BBBBB', 0x05, 0x01, 0x00, 0x03, host_len) + target_host_bytes + struct.pack('!H', target_port)
        
        sock.send(connect_request)
        
        # 接收连接响应
        connect_response = sock.recv(10)
        if len(connect_response) < 10:
            print("连接响应长度错误")
            return False
            
        if connect_response[0] != 0x05:
            print(f"SOCKS5版本错误: {connect_response[0]}")
            return False
            
        if connect_response[1] != 0x00:
            error_codes = {
                0x01: "通用失败",
                0x02: "连接不允许",
                0x03: "网络不可达",
                0x04: "主机不可达",
                0x05: "连接被拒绝",
                0x06: "TTL超时",
                0x07: "命令不支持",
                0x08: "地址类型不支持"
            }
            error_msg = error_codes.get(connect_response[1], f"未知错误代码: {connect_response[1]}")
            print(f"连接失败: {error_msg}")
            return False
            
        print("SOCKS5连接成功建立！")
        
        # 测试HTTP请求
        print("发送HTTP测试请求...")
        http_request = f"GET /ip HTTP/1.1\r\nHost: {target_host}\r\nConnection: close\r\n\r\n"
        sock.send(http_request.encode('utf-8'))
        
        # 接收HTTP响应
        response = sock.recv(4096)
        if response:
            print("HTTP响应接收成功！")
            print("响应前200字节:")
            print(response[:200].decode('utf-8', errors='ignore'))
            return True
        else:
            print("没有收到HTTP响应")
            return False
            
    except Exception as e:
        print(f"测试失败: {str(e)}")
        return False
    finally:
        try:
            sock.close()
        except:
            pass

def main():
    print("=== SOCKS5代理测试 ===")
    
    # 测试配置
    proxy_host = '127.0.0.1'
    proxy_port = 7890
    target_host = 'httpbin.org'
    target_port = 80
    
    print(f"代理服务器: {proxy_host}:{proxy_port}")
    print(f"目标服务器: {target_host}:{target_port}")
    print()
    
    success = test_socks5_proxy(proxy_host, proxy_port, target_host, target_port)
    
    if success:
        print("\n✅ SOCKS5代理测试通过！")
        return 0
    else:
        print("\n❌ SOCKS5代理测试失败！")
        return 1

if __name__ == '__main__':
    sys.exit(main())