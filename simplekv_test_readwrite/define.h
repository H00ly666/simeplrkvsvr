/**
 * Created by 刘嘉辉 on 11/22/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente some arguments.
 */

#pragma once

#include <string>
#include <mutex>

using namespace std;

enum RetCode {
  kSucc = 0,
  kNotFound = 1,
  kCorruption = 2,
  kNotSupported = 3,
  kInvalidArgument = 4,
  kIOError = 5,
  kIncomplete = 6,
  kTimedOut = 7,
  kFull = 8,
  kOutOfMemory = 9,
};

/*value文件的大小 64M 废弃*/
const size_t VALUE_LOG_SIZE = 0;
/*key文件大小 64MB*/
const size_t KEY_LOG_SIZE = 1024 << 16;
/*文件数*/
const int FILE_NUM = 64;
/*并发恢复线程数量*/
const int RECOVER_THREAD = 64;


const size_t CACHE_SIZE = VALUE_LOG_SIZE;
/*缓冲区容量*/
const int CACHE_NUM = 16;
/*可替换缓冲区单元*/
const int ACTIVE_CACHE_NUM = 8;
/*保留缓冲区单元*/

/*value cache容量*/
const int PAGE_PER_BLOCK = 4;
/**/
const size_t BLOCK_SIZE = PAGE_PER_BLOCK << 12;


struct PMutex {
    pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

    void lock() { pthread_mutex_lock(&m); }

    void unlock() { pthread_mutex_unlock(&m); }
};

struct PCond {
    PMutex pm;
    pthread_cond_t c = PTHREAD_COND_INITIALIZER;

    void lock() { pm.lock(); }

    void unlock() { pm.unlock(); }

    void wait() {
        pthread_cond_wait(&c, &pm.m);
    }

    void notify_all() {
        pthread_cond_broadcast(&c);
    }

    void notify_one() {
        pthread_cond_signal(&c);
    }
};
