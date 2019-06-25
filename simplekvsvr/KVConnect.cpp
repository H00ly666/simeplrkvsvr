/**
 * Created by 刘嘉辉 on 10/22/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente http_conn.cpp.
 */


#include <string>
#include <string.h>
#include <unistd.h>

#include "KVConnect.h"

using namespace std;

/*设置非阻塞套接字*/
int setnonblocking(int fd)
{
    int old_option = fcntl(fd, F_GETFL);
    int new_option = old_option | O_NONBLOCK;
    fcntl(fd, F_SETFL, new_option);
    return old_option;
}

void addfd(int epollfd, int fd, bool one_shot)
{
    epoll_event event;
    event.data.fd = fd;
    event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    //event.events = EPOLLIN  | EPOLLRDHUP;
    if(one_shot)
    {
        event.events |= EPOLLONESHOT;
    }
    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
    setnonblocking(fd);
}

void removefd(int epollfd, int fd)
{
    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, 0);
    close(fd);
}

void modfd(int epollfd, int fd, int ev)
{
    epoll_event event;
    event.data.fd = fd;
    //event.events = ev | EPOLLET | EPOLLONESHOT | EPOLLRDHUP;
    event.events = ev  | EPOLLONESHOT | EPOLLRDHUP;
    //event.events = ev;
    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &event);
}

int simplekvTask::m_user_count = 0;
int simplekvTask::m_epollfd = -1;

void simplekvTask::close_conn(bool real_close)
{
    if(real_close && (m_sockfd != -1))
    {
        removefd(m_epollfd, m_sockfd);
        m_sockfd = -1;
        m_user_count--;
    }
}

void simplekvTask::init(int sockfd, const sockaddr_in& addr, PEngine *eng)
{
    //engine 类从这里穿进来 我真是个小机灵鬼
    engine = eng;
    m_sockfd = sockfd;
    m_address = addr;
    int error = 0;
    socklen_t len = sizeof(error);
    getsockopt(m_sockfd, SOL_SOCKET, SO_ERROR, &error, &len);
    int reuse = 1;
    setsockopt(m_sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    addfd(m_epollfd, sockfd, true);
    m_user_count++;

    init();
}

/*进行状态机的初始化*/
void simplekvTask::init()
{
    //m_linger = false;

    m_method = GET;
    m_content_length = 0;
    m_host = 0;
    m_start_line = 0;
    m_checked_idx = 0;
    m_read_idx = 0;
    m_write_idx = 0;
    memset( m_read_buf, '\0', READ_BUFFER_SIZE );
    memset( m_write_buf, '\0', WRITE_BUFFER_SIZE );
}
/*循环读取客户数据 直到对方关闭或无数据可读*/
bool simplekvTask::read_line()
{

    int bytes_read = 0;
    int  length = 0;

    read(m_sockfd, &length, sizeof(length));
   // cout <<"接收到的长度"<< length <<endl;

    while (bytes_read != length) {
        int len = read(m_sockfd, m_read_buf + bytes_read , length);
        if (len == -1) {
            if(errno == EAGAIN) {

                //perror("EAGAIN ");
                continue;
            } else if (errno == EWOULDBLOCK) {

                perror("EWOULDBLOCEK ");
                return false;
            }
        } else if (len == 0) {
            return true;
        } else if (len > 0) {
            bytes_read += len;
        }

    }

    //cout << "m_read_buf :" << strlen(m_read_buf) << "readindex"<< m_read_idx  << "^^" << m_read_buf << "\n" <<endl;
    return true;
}

/*主状态机*/
int simplekvTask::process_read()
{   /*
    char *p = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    char **argv;*/
    //if (m_read_idx <= 6)
    //    return 1;
    argc = 0;

    char *p = m_read_buf;
    p++;
    while(*p != '\r') {
        argc = (argc * 10) + (*p - '0');
        p++;
    }
    //cout <<"argc: "<<argc <<endl;
    //如果argc>0 ,小于3
    if (argc<0 || argc > 3) {
        //初始化状态机
        init();
        return 0;
    }

    p += 3;
    //cout << m_read_buf <<endl;
    argv = (char **) malloc (sizeof(char *) * argc);

    for(int i=0 ; i < argc ; i++) {
        int len = 0;
        while(*p != '\r') {
            len = (len * 10) + (*p - '0');
            p++;
        }
        p += 2;
        //开始解析字符
        argv[i] = (char *) malloc (len+1);

        for (int j = 0; j<len; j++) {
            argv[i][j] = *p;
            p++;
        }
        argv[i][len] = '\0';
        p += 3;
    }

    // cout <<"argv: "<<argv[0] <<endl;
	if (!strcasecmp (argv[0], "get")) {
        string key(argv[1]);
        engine->read(key, value);
        m_method = GET;
	} else if(!strcasecmp (argv[0], "set")) {
      //  cout <<"set" <<endl;
        string key(argv[1]);
        engine->put(key, argv[2]);
        m_method = SET;
    } else if(!strcasecmp (argv[0], "delete")) {
        string key(argv[1]);
        engine->Delete(key);
        m_method = DELETE;
    } else if(!strcasecmp (argv[0], "stats")) {
        engine->status(m_write_buf);
        m_method =  STATS;
    } else if(!strcasecmp (argv[0], "quit")) {
        //退出程序
        m_method = QUIT;
    }

    for (int j = 0;j< argc;j++) {
        free(argv[j]);
    }
    free(argv);

    return 1;
}

bool simplekvTask::write_line()
{
    int temp = 0;
    int bytes_have_send = 0;
    int bytes_to_send = 0;
        //m_write_idx;
    if (bytes_to_send == 0)
    {
        modfd(m_epollfd, m_sockfd, EPOLLIN);
        init();
        return true;
    }

    while(true)
    {
        temp = writev(m_sockfd, m_iv, m_iv_count);
        if (temp <= -1)
        {
            /**
             * 如果TCP写缓冲没有空间　则等待下一轮EPOLLOUT事件
             * 虽然在此期间服务器无法立即接受到同一客户的下一个请求
             * 但是这可以保证连接的完整性
             */
            if(errno == EAGAIN)
            {
                modfd(m_epollfd, m_sockfd, EPOLLOUT);
                return true;
            }
            return false;
        }

        bytes_to_send -= temp;
        bytes_have_send += temp;
        if (bytes_to_send <= bytes_have_send)
        {
            if(m_linger)
            {
                modfd(m_epollfd, m_sockfd, EPOLLIN);
                return true;
            }
            else
            {
                modfd(m_epollfd, m_sockfd, EPOLLIN);
                return false;
            }
        }
    }
}

/*填充HTTP应答*/
bool simplekvTask::process_write(int a)
{
    char str[10] = "OK\n";
    bool ret = true;
    switch (m_method)
    {
        case GET :
        {
            write(m_sockfd, value.c_str(), value.length());
            break;
        }
        case SET :
        {
            int retat = write(m_sockfd, str, strlen(str));
            //cout << rett <<"个字节发给" <<  m_sockfd<< endl;
            //perror("write ");
            break;
        }
        case DELETE :
        {
            int length = write(m_sockfd, str, strlen(str));
            if (length<0)
                ret = false;
            break;
        }
        case STATS :
        {
            write(m_sockfd, m_write_buf, strlen(m_write_buf));
            break;
        }
        case QUIT :
        {
            int length = write(m_sockfd, str, strlen(str));
            if (length < 0)
                ret = false;
            break;
        }
    }

    init();
    return ret;
}

/*由线程池内的工作线程调用　处理http请求的入口*/
void simplekvTask::process()
{
    /* 先处理read */
    read_line();
    int read_ret = process_read();


    bool write_ret = process_write(read_ret);
    if (! write_ret)
    {
        close_conn();
    }
    modfd(m_epollfd, m_sockfd, EPOLLIN);
}
