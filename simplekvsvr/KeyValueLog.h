/**
 * Created by 刘嘉辉 on 11/22/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente mmap keyFiles and get fd of valueFiles.
 */

#ifndef ENGINE_KEYVALUELOG_H
#define ENGINE_KEYVALUELOG_H

//#include <stdin.h>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>

#include "define.h"
#include "KVMapEntry.h"

class KeyValueLog {
private:
    //key-value文件Id
    int id;
    string path;

    //value的fd
    int current_value_Fd;
    size_t filePosition;
    //当前记录在value文件内的便宜
    size_t globalOffset;

    //记录当前map文件的大小 初始化为define中的大小　当扩容时自动增大
    size_t currentKeyFileSize;

    size_t cacheBufferPosition;

    char *cacheBuffer;

    size_t MapBufferPosition;
    char *MapBuffer;

    //是否正在enlarge
    bool enlarge;

    PMutex WriteMutex;

public:
    KeyValueLog() {}
    KeyValueLog(const std::string &path, const int &id, const int &fd, const size_t &globalOffset, char *cacheBuffer, char *MapBuffer, const size_t mapFileSize) :
        id(id), path(path), current_value_Fd(fd) ,filePosition(0), globalOffset(globalOffset), cacheBufferPosition(0),
        cacheBuffer(cacheBuffer), MapBufferPosition(0), MapBuffer(MapBuffer), enlarge(false), currentKeyFileSize(mapFileSize) {
    }

    ~KeyValueLog() {
        /*
        if (this->cacheBufferPosition != 0) {
            auto remainSize = cacheBufferPosition << 12;
            pwrite(this->fd, cacheBuffer, remainSize, globalOffset + filePosition);
        }
        if (this->enlarge) {
            munmap(MapBuffer, KEY_ENLARGE_SIZE);
            close(this->fd);
        }
        */
    }

    void KeyValueLogEnlarge() {
        std::ostringstream mapfp, valuefp;
        mapfp << path << "/enlarge-map-" << id;
        valuefp << path << "/enlarge-value-" << id;
        string mapfilePath = mapfp.str();
        string valuefilePath = valuefp.str();

        //标志为正在扩容
        this->enlarge = true;
        int mapfd = open(mapfp.str().data(), O_CREAT | O_RDWR , 0666);
        //当前大小扩容双倍
        currentKeyFileSize <<= 1;
        //打文件空洞
        fallocate(mapfd, 0, 0, this->currentKeyFileSize);
        this->MapBuffer = static_cast<char *>(mmap(nullptr, currentKeyFileSize, PROT_READ | PROT_WRITE,
                                                        MAP_SHARED | MAP_POPULATE, mapfd, 0));

        //更新value文件 文件fd此时似乎还是不能立即改变　因为当前还有查询所以得等到文件全部更新之后才能切换
        this->current_value_Fd = open(valuefp.str().data(), O_CREAT | O_RDWR | O_APPEND, 0666);
        //当前value文件的偏移置为0
        this->globalOffset = 0;
        //Map-mmap偏移切换
        this->MapBufferPosition = 0;
    }

    //扩容完毕 切换文件 更改文件名
    void modifyKeyValueFile(int mapfd, int valuefd) {
        char enlarge_mapname[30];
        char mapname[30];
        char enlarge_valuename[30];
        char valuename[30];

        //     close(mapfd);
        //     perror("close mapfd: ");
        //     close(valuefd);
        //     perror("close valuefd: ");
        sprintf(enlarge_mapname ,"./log/enlarge-map-%d",id);
        sprintf(mapname ,"./log/map-%d",id);
        int a = rename(enlarge_mapname, mapname);
        //    perror("rename mapfd: ");
       // cout << "成功是为a=0" << a << mapname <<"__" << enlarge_mapname <<endl;


        sprintf(enlarge_valuename ,"./log/enlarge-value-%d",id);
        sprintf(valuename ,"./log/value-%d",id);
        int b = rename(enlarge_valuename, valuename);

       // cout << "成功是为b=0" << b << valuename <<"__" << enlarge_valuename <<endl;

        //表示没有在扩容
        enlarge = false;
    }

    size_t getMapBufferPosition() const {
        return MapBufferPosition;
    }

    size_t getvalueFileGlobalOffset() const {
        return globalOffset;
    }

    //当数据大于70%,且enlarge为false,进行扩容
    bool isNeedEnlarge() const {
        double a =  (MapBufferPosition*1.00000000)/currentKeyFileSize;
       // cout <<"占比为 "<< a <<endl;
        return  (a > 0.7) && !enlarge;
    }

    //WriteMutex
    //TODO 需要考虑锁的粒度 已解决
    inline KVMapEntry putValueKey(const std::string &key, const std::string &value) {
        //将 value 数据复制到 value cache，并更新缓存的数据个数
        int value_len = value.length();

        WriteMutex.lock();
 //       int fd = this->current_value_Fd;
        char *Buffer = MapBuffer;
        size_t valueOffset = globalOffset;
        size_t mapOffset = MapBufferPosition;
        globalOffset += value_len;
        //globalOffset += value.length();
        MapBufferPosition += (key.length() + 20);
        WriteMutex.unlock();

        pwrite(this->current_value_Fd, value.c_str(), value.length(), valueOffset);

        int len = (int)key.length();
        //写key_len
        memcpy(Buffer + mapOffset, &len, 4);
        //写value_len
        memcpy(Buffer + mapOffset + 4, &value_len, 4);
        //写value_fileid
        memcpy(Buffer + mapOffset + 8, &id, 4);
        //写value_offset
        memcpy(Buffer + mapOffset + 12, &valueOffset, 8);
        //写key_string
        memcpy(Buffer + mapOffset + 20, key.c_str(), key.length());

        KVMapEntry mapentry;
        mapentry.setvaluelength(value_len);
        mapentry.setfileID(id);
        //设置entry中的value偏移
        mapentry.setoffset(valueOffset);
        mapentry.setvaluefd(current_value_Fd);

        //返回拷贝
        return mapentry;
    }

    //追加删除的Key记录
    inline void putDeleteKey(const std::string &key) {
        int len = (int)key.length();
        //key_len
        memcpy(MapBuffer + MapBufferPosition, &len, 4);
        //value_len = 0,标记这是一条删除的记录
        len = 0;
        memcpy(MapBuffer + MapBufferPosition + 4, &len, 4);
        //value_fileid
        memcpy(MapBuffer + MapBufferPosition +8, &id, 4);
        //value_offset
        size_t offset = 0;
        memcpy(MapBuffer + MapBufferPosition + 12, &offset, 8);
        //key_string
        memcpy(MapBuffer + MapBufferPosition + 20, key.c_str(), key.length());
        MapBufferPosition += (key.length() + 20);
    }

    /**
     * 从mapEntry中读取fd,此时旧的value文件还没有关闭,所以readValue_fd需要从上层获得
     */
    inline void readValue(size_t offset, char *value, size_t size, int readValue_fd) {
        pread(readValue_fd, value, size, offset);
    }

    /**
     * TODO
     * 由于value的持久化只做一半 这里预留了一部分
     * 这个其实是将value的cache打进file中去
     */
    void recover(u_int32_t sum) {
        this->cacheBufferPosition = sum % PAGE_PER_BLOCK;
        auto offset = (size_t) cacheBufferPosition << 12;
        filePosition -= offset;
        pwrite(this->current_value_Fd, cacheBuffer, offset, globalOffset + filePosition);
    }


    //获取MAPentry
    inline int getMapEntry(KVMapEntry& mapentry) {
        char str[4];
        //keylength
        memcpy(str, MapBuffer+ MapBufferPosition, 4);
        int keylength = *(int *)(str);
        //Key长度为0 代表读到末尾了 立即返回
        if (!keylength) {
            return 0;
        }
    //cout << keylength << " ";

        //value_len
        memcpy(str, MapBuffer+ MapBufferPosition + 4, 4);
        int value_len = *(int *)(str);
        /**
         * valuelen为0时,代表此条数据已删除
         * 解析时跳过此条数据 然后加上数据偏移
         * 返回-keylen 代表这是一条被删除的数据
         * 应该在hash_map中删除它
         */
        if (!value_len) {
            MapBufferPosition += 20;
      //      cout <<"-keylength" << -keylength <<endl;
            return -keylength;
        }
        mapentry.setvaluelength(value_len);
   // cout<<value_len << " ";

        //value_name
        memcpy(str, MapBuffer + MapBufferPosition + 8, 4);
        int valuefileID = *(int *)(str);
        mapentry.setfileID(valuefileID);
   // cout << valuefileID << " ";

        //value_offset
        char ptr[8];
        memcpy(ptr, MapBuffer + MapBufferPosition + 12, 8);
        size_t value_offset = *(size_t* )(ptr);
        mapentry.setoffset(value_offset);
   // cout << value_offset << " " ;

        //valuefd
        mapentry.setvaluefd(current_value_Fd);

        MapBufferPosition += 20;
        return keylength;
    }

    //key_cstr
    inline void getKey(char *key_cstr, const int length) {
        memcpy(key_cstr, MapBuffer + MapBufferPosition, length);
        key_cstr[length] = '\0';
        MapBufferPosition += length;
  //  cout << key_cstr <<endl;
    }

};

#endif //ENGINE_KEYVALUELOG_H
