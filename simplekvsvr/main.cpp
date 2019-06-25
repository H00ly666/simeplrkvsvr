/**
 * Created by 刘嘉辉 on 10/30/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente main.h.
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <cassert>
#include <sys/epoll.h>

#include "locker.h"
#include "threadpool.h"
#include "PEngine.h"
#include "KVConnect.h"

#define MAX_FD 65536
#define MAX_EVENT_NUMBER 100000

using namespace std;

extern int addfd(int epollfd, int fd, bool one_shot);
extern int removefd(int epollfd, int fd );

void addsig(int sig, void(handler )(int), bool restart = true)
{
    struct sigaction sa;
    memset(&sa, '\0', sizeof(sa));
    sa.sa_handler = handler;
    //继续执行中断的函数
    if(restart)
    {
        sa.sa_flags |= SA_RESTART;
    }
    sigfillset(&sa.sa_mask );
    assert(sigaction(sig, &sa, NULL)!= -1 );
}

void show_error(int connfd, const char* info)
{
    printf("%s", info);
    send(connfd, info, strlen(info ), 0);
    close(connfd);
}


int main(int argc, char* argv[])
{
    if(argc <= 2)
    {
        printf("usage: %s ip_address port_number\n", basename(argv[0]));
        return 1;
    }

    const char* ip = argv[1];
    int port = atoi(argv[2]);

    addsig(SIGPIPE, SIG_IGN);
    PEngine *engine = new PEngine("./log");

    threadpool<simplekvTask>* pool = NULL;
    try
    {
        pool = new threadpool<simplekvTask>;
    }
    catch(...)
    {
        return 1;
    }

    /*预先为每一个连接分配一个simplekvTask对象  内存不值钱的想法*/
    simplekvTask* users = new simplekvTask[MAX_FD];
    assert(users);
    int user_count = 0;

    int listenfd = socket(PF_INET, SOCK_STREAM, 0);
    assert(listenfd >= 0);
    struct linger tmp = { 1, 0 };
    setsockopt(listenfd, SOL_SOCKET, SO_LINGER, &tmp, sizeof(tmp));

    int ret = 0;
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    address.sin_family = AF_INET;
    inet_pton(AF_INET, ip, &address.sin_addr);
    address.sin_port = htons(port);

    ret = bind(listenfd, (struct sockaddr* )&address, sizeof(address));
    assert(ret >= 0 );

    ret = listen(listenfd, 5);
    assert(ret >= 0);

    int epollfd = epoll_create(5);
    epoll_event events[ MAX_EVENT_NUMBER];
    assert(epollfd != -1);
    addfd(epollfd, listenfd, false);

    simplekvTask::m_epollfd = epollfd;

    while(true)
    {
        int number = epoll_wait(epollfd, events, MAX_EVENT_NUMBER, -1);
        if ((number < 0) && (errno != EINTR))
        {
            printf("epoll failure\n" );
            break;
        }

        for (int i = 0; i < number; i++)
        {
           // cout << "i "<< i <<endl;
            int sockfd = events[i].data.fd;
            if(sockfd == listenfd)
            {
                struct sockaddr_in client_address;
                socklen_t client_addrlength = sizeof(client_address);
                int connfd = accept(listenfd, (struct sockaddr* )&client_address, &client_addrlength);
                //cout << "新连接: "<< connfd <<endl;
                if (connfd < 0)
                {
                    printf("errno is: %d\n", errno);
                    continue;
                }
                if(simplekvTask::m_user_count >= MAX_FD)
                {
                    show_error(connfd, "Internal server busy");
                    continue;
                }

                /*初始化客户连接 及状态机的初始化*/
                users[connfd].init(connfd, client_address, engine);
            }

            /*异常事件 直接关闭 不做过多处理*/
            else if(events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))
            {
                cout << "错误::"<<endl;
                users[sockfd].close_conn();
            }

            /*有读的数据了　向队列中添加处理任务*/
            else if(events[i].events & EPOLLIN)
            {
                //if(users[sockfd].read_line())
                //{
                //    pool->append(users + sockfd);
                //}

                pool->append(users + sockfd);
               // else
                //{
                    //users[sockfd].close_conn();
                //}
            }

            /*内核缓冲区中发送空间不足 等待EPOLLOUT时间*/
           // else if(events[i].events & EPOLLOUT)
           // {
           //     if(!users[sockfd].write_line())
           //     {
           //         users[sockfd].close_conn();
           //     }
           // }
        }
    }

    close(epollfd );
    close(listenfd );
    delete [] users;
    delete pool;
    return 0;
}

