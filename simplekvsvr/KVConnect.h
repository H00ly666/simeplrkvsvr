/**
 * Created by 刘嘉辉 on 10/21/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente process_pool.h.
 */

#pragma once

#ifndef KVCONNECTION_H
#define KVCONNECTION_H

#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <assert.h>
#include <sys/stat.h>
#include <string.h>
#include <string>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <errno.h>

#include "locker.h"
#include "PEngine.h"

//extern PEngine engine;

class simplekvTask
{
public:
    //定义成public的存value
    //char *value;
    std::string value;

    /*读缓冲区的大小*/
    static const int READ_BUFFER_SIZE = 1024;
    static const int WRITE_BUFFER_SIZE = 1024;
    /*simplekv请求方法*/
    enum METHOD { GET = 0, SET, DELETE, STATS, QUIT };
    /*解析客户时主状态机所处的状态*/
    /*行读取结果*/
    PEngine *engine;
public:
    simplekvTask(int fd, char* buf, PEngine *eng) {
        m_sockfd = fd;
        engine = eng;
        memcpy(m_read_buf, buf, strlen(buf));
    }

    simplekvTask(){}
    ~simplekvTask(){}

public:
    /*初始化新接受的连接*/
    void init(int sockfd, const sockaddr_in& addr, PEngine *eng);
    /*关闭连接*/
    void close_conn( bool real_close = true);
    /*处理客户请求*/
    void process();
    /*非阻塞读*/
    bool read_line();
    /*非阻塞写*/
    bool write_line();

private:
    /*初始化连接*/
    void init();
    /*解析simplekv请求*/
    int process_read();
    /*填充simplekv请求*/
    bool process_write(int ret);

    /*分析simplekv请求*/

    int do_request();


public:
    /*所有scoket上的事件都被注册到同一个epoll内核事件表中　所以将其设置为静态的*/
    static int m_epollfd;
    /*统计用户数量*/
    static int m_user_count;

    /*该连接的socket和地址*/
    int m_sockfd;
    sockaddr_in m_address;

    int argc=0;
    char **argv;

    /*读缓冲区*/
    char m_read_buf[READ_BUFFER_SIZE];

private:
    /*标识度缓冲区中已经读入的客户数据的最后一个字节的的下一个位置*/
    int m_read_idx;
    /*当前正在分析的字符在读缓冲区的位置*/
    int m_checked_idx;
    /*当前正在解析的行的起始位置*/
    int m_start_line;
    /*写缓冲区*/
    char m_write_buf[WRITE_BUFFER_SIZE];
    int m_write_idx;

    //CHECK_STATE m_check_state;
    /*请求方法*/
    METHOD m_method;

    char* m_version;
    /*主机名*/
    char* m_host;
    int m_content_length;
    /*请求是否要保持连接*/
    bool m_linger;

    /*writev执行写操作 便于集中写*/
    struct iovec m_iv[2];
    /*数量*/
    int m_iv_count;
};

#endif
