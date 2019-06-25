/**
 * Created by 刘嘉辉 on 11/22/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente PEngine
 */
#ifndef ENGINE_RACE_PENGINE_H
#define ENGINE_RACE_PENGINE_H

#include <malloc.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <string.h>
#include <mutex>
#include <thread>
#include <atomic>
#include <map>
#include <set>
#include <condition_variable>
#include <google/dense_hash_map>

#include "KVFiles.h"
#include "KeyValueLog.h"
#include "KVMapEntry.h"
#include "LRUCache11.h"
#include "define.h"

using namespace std;
//测时间
using namespace std::chrono;
using namespace simplekvsvr;

namespace simplekvsvr {

    //static thread_local std::unique_ptr<char> readBuffer(static_cast<char *> (memalign((size_t) getpagesize(), 4096)));

    /**
     * LRU Cache size为200个  由于pread的原因这里要用char* 　
     * 因为string提供的那个字符串不能改变
     * 但是这个LRU锁在内部封装　所以不需要在这里加锁了
     */

    class PEngine {
    private:

        KVFiles *kvFiles[64];
        KeyValueLog *keyValueLogs[64];
        lru11::Cache<std::string, std::string, std::mutex> LRUCache[64];
        google::dense_hash_map<std::string, KVMapEntry> KVMap[64];

        PMutex MapMutex[64];
        int totalNum = 0;
        bool unEnlarge = true;

    public:
        //默认本地路径就好了
        explicit PEngine(const string &path)  {
            //必须设置empty key,delete key, 否则断言失败
            for (int i = 0; i< FILE_NUM; i++) {
                KVMap[i].set_empty_key("!");
                KVMap[i].set_deleted_key("@");
            }

            std::ostringstream ss;
            ss << path << "/value-0";
            string filePath = ss.str();

            if (access(filePath.data(), 0) != -1) {

                //初始化 map , value  存储文件 64个文件
                for (int fileId = 0; fileId < FILE_NUM; fileId++) {
                    //文件路径 文件id 是否存在 value文件大小  索引文件大小  缓存块的大小
                    kvFiles[fileId] = new KVFiles(path, fileId,
                                                  true,
                                                  VALUE_LOG_SIZE,
                                                  KEY_LOG_SIZE,
                                                  BLOCK_SIZE);

                    //建立索引与内存的关系
                    //路径 文件id  fileid 全局偏移 写缓存buffer 索引文件buffer
                    keyValueLogs[fileId] = new KeyValueLog(path,  fileId,
                                                          kvFiles[fileId]->getValueFd(),
                                                          kvFiles[fileId]->getvalueFileGlobalOffset(),
                                                          kvFiles[fileId]->getBlockBuffer(),
                                                          kvFiles[fileId]->getKeyBuffer(),
                                                          kvFiles[fileId]->getMapFileSize());
                }

                //64线程读key 加载google_hash_map索引
                std::thread t[RECOVER_THREAD];
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, this] {
                        KVMapEntry entry;
                        while (int keylen = keyValueLogs[i]->getMapEntry(entry)) {
            //cout <<"传过来的-keylength与keylen" << -keylen<< "  " <<keylen <<endl;
                            //再来一个字符串
                            char *str = (char *) malloc (abs(keylen)+1);

                            keyValueLogs[i]->getKey(str, abs(keylen));
                            str[abs(keylen)] = '\0';
                            std::string key(str);
                            //这条key 需要删除
                            if (keylen < 0) {
                                KVMap[i].erase(key);
                            } else {
                                KVMap[i][key] = entry;
                            }
                            free(str);
                        }
                    });
                }

                for (auto &i : t) {
                    i.join();
                }
                cout <<"索引加载完毕" <<endl;
            } else {  //文件不存在的情况

                for (int fileId = 0; fileId < FILE_NUM; fileId++) {

                    kvFiles[fileId] = new KVFiles(path, fileId,
                                                  false,
                                                  VALUE_LOG_SIZE,
                                                  KEY_LOG_SIZE,
                                                  BLOCK_SIZE);

                    keyValueLogs[fileId] = new KeyValueLog(path, fileId,
                                                          kvFiles[fileId]->getValueFd(),
                                                          kvFiles[fileId]->getvalueFileGlobalOffset(),
                                                          kvFiles[fileId]->getBlockBuffer(),
                                                          kvFiles[fileId]->getKeyBuffer(),
                                                          kvFiles[fileId]->getMapFileSize());
                }
            }
        }

        ~PEngine() {
            for (auto kvFilesi : kvFiles)
                delete kvFilesi;

            for (auto keyvl : keyValueLogs)
                delete keyvl;
        }

        inline int getLogId(const string &key) {
            //std::string str = "MEtttt";
            std::hash<string> hash_fun;
            size_t strhashnum = hash_fun(key);
            return strhashnum%64;
        }


        void put(const std::string &key, const std::string &value) {
            auto logId = getLogId(key);
            auto entry = keyValueLogs[logId]->putValueKey(key, value);

            //当key的数据量 >= 70％，进行更新并扩容两倍
            MapMutex[logId].lock();
                LRUCache[logId].insert(key, value);
                bool ret = keyValueLogs[logId]->isNeedEnlarge();
                if(ret)
                    keyValueLogs[logId]->KeyValueLogEnlarge();
            MapMutex[logId].unlock();

            if (ret) {
                //cout <<"进行扩容" << endl;
                std::thread t;
                t = std::thread([logId, this] {
                    //遍历此id的整个哈希表
                    auto ite = KVMap[logId].begin();
                    for (ite; ite != KVMap[logId].end(); ite++) {
                        //转移数据
                        moveKeyValue(ite->first, ite->second, logId);
                    }

                    MapMutex[logId].lock();
                    keyValueLogs[logId]->modifyKeyValueFile(kvFiles[logId]->getMapFd() , kvFiles[logId]->getValueFd());
                    MapMutex[logId].unlock();
                });

                t.join();
            }
        }

        //文件存储重整功能
        void moveKeyValue(const std::string &key, KVMapEntry &entry, int logId) {
            //char value[1000];
            string value;
            read(key, value);
            //更新索引，并写入新数据
            entry = keyValueLogs[logId]->putValueKey(key, value);
        }

        RetCode read(const std::string &key, std::string &value) {
            auto logId = getLogId(key);

            if(LRUCache[logId].tryGet(key, value)) {
                return kSucc;
            } else {
                auto ite = KVMap[logId].find(key);
                if(ite != KVMap[logId].end()) {
                    char v_str[100];

                    keyValueLogs[logId]->readValue(ite->second.getoffset(), v_str,
                                                   ite->second.getvaluelength(),
                                                   ite->second.getvaluefd());
                    value = string(v_str);
                    //cout << "value  " << value << endl;
                    return kSucc;
                } else {
                    return kNotFound;
                }
            }
        }

        //这个流程与put类似 但是得有才能删
        RetCode Delete(const std::string &key) {
            auto logId = getLogId(key);
            LRUCache[logId].remove(key);
                cout <<"进入删除" << endl;

            auto ite = KVMap[logId].find(key);
            if(ite != KVMap[logId].end()) {
                //首先在索引中找到对应的项 然后锁住，delete掉
                MapMutex[logId].lock();

                KVMap[logId].erase(key);
                cout <<"删除成功" << endl;
                //锁住对应的文件 然后追加一条key 就好了， value就没必要再写了
                //<length,fileID,offset,key> <length,     0,     0,key> 加入这条
                keyValueLogs[logId]->putDeleteKey(key);
                MapMutex[logId].unlock();

                return kSucc;
            } else {
                cout <<"删除不存在" << endl;
                return kNotFound;
            }
        }

        /*
         * 因为这个返回的数据是要传给用户的
         * 所以可以直接在传进来的字符串指针上
         * 对数据进行书写就可以了吧
         * key-count mem-size file-size hit-count miss-count
         */
        void status(char *m_write_buf) {
            //遍历所有的hash_table统计count
            unsigned long key_counts = 0;
            unsigned long hit_counts = 0;
            unsigned long miss_counts = 0;
            unsigned long mapfile_size = 0;
            unsigned long valuefile_size = 0;

            for(auto i : KVMap) {
                key_counts += i.size();
            }

            //遍历所有lru
            for (int j = 0 ; j < 64; j++) {
                hit_counts += LRUCache[j].hits;
                miss_counts += LRUCache[j].misses;
            }

            //遍历所有的KeyValueLogs得到文件的大小
            for (auto k : keyValueLogs) {
                mapfile_size += k->getMapBufferPosition();
                valuefile_size += k->getvalueFileGlobalOffset();
            }

            sprintf(m_write_buf,"count <%ld>, mem <%ld>, file <%ld>, hits <%ld>, misses <%ld>",
                                 key_counts, mapfile_size, mapfile_size+valuefile_size, hit_counts, miss_counts);
            cout << "m_write_buf "  << m_write_buf  << endl;
        }
    };
}
#endif //ENGINE_RACE_PENGINE_H
