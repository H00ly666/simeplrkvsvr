/**
 * Created by 刘嘉辉 on 11/22/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente open KVFiles.
 */

#ifndef ENGINE_VALUELOGFILE_H
#define ENGINE_VALUELOGFILE_H

#include <stdint.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
using namespace std;

namespace simplekvsvr {

    class KVFiles {
    private:
        int valueFd;
        size_t valueFileSize;
        size_t valueFileGlobalOffset;

        int mapFd;
        char * MapBuffer;
        size_t keyFileSize;
        //写的块   但我想这次可能不需要 因为不用D I/O的话 这个工作系统可能会帮我做
        char * blockBuffer;
        size_t blockFileSize;

    public:
        KVFiles() {}
        KVFiles(const std::string &path, const int &id, const bool exist, const size_t &valueFileSize, const size_t &key_FileSize, const size_t &blockFileSize)
            : valueFileSize(valueFileSize), keyFileSize(key_FileSize), blockFileSize(blockFileSize){
            //Value Log
            std::ostringstream fp;
            fp << path << "/value-" << id;
            this->valueFd = open(fp.str().data(), O_CREAT | O_RDWR | O_APPEND, 0666);

            //求value文件的长度
            struct stat fpstatbuf;
            stat(fp.str().data(), &fpstatbuf);
            valueFileGlobalOffset = fpstatbuf.st_size;
   // cout << "value文件长度为" << valueFileGlobalOffset << endl;

            //Map Log
            std::ostringstream mp;
            mp << path << "/map-" << id;
            //关掉direct 可能会更好一些
            this->mapFd = open(mp.str().data(), O_CREAT | O_RDWR, 0666);

            //求map文件的长度
            struct stat mpstatbuf;
            stat(mp.str().data(), &mpstatbuf);
            //keyFileSize = mpstatbuf.st_size;

    //cout << "define内map文件长度为" << keyFileSize <<"文件长度" << mpstatbuf.st_size << endl;
            mpstatbuf.st_size > key_FileSize ? keyFileSize = mpstatbuf.st_size : keyFileSize = key_FileSize;
    //cout << "map文件长度为" << keyFileSize << endl;


            ftruncate(this->mapFd, keyFileSize);
            //从mapfd 文件头开始映射 大小为keyFileSize = KEY_LOG_SIZE
            this->MapBuffer = static_cast<char *>(mmap(nullptr, keyFileSize, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE | MAP_NONBLOCK, this->mapFd,
                                                            0));
            //关闭value写缓存
            //ftruncate(this->mapFd, keyFileSize + blockFileSize);
            //从keyFileSize处开始映射 blockFileSize = BLOCK_SIZE * num_log_per_file = 16KB*64
            //this->blockBuffer = static_cast<char *>(mmap(nullptr, blockFileSize, PROT_READ | PROT_WRITE,
            //                                                MAP_PRIVATE | MAP_POPULATE | MAP_NONBLOCK, this->mapFd,
            //                                                keyFileSize));
        }

        ~KVFiles() {
            //munmap(blockBuffer, this->blockFileSize);
            munmap(MapBuffer, this->keyFileSize);
            close(this->mapFd);
            close(this->valueFd);
        }


        int getValueFd() const {
            return valueFd;
        }

        int getMapFd() const {
            return mapFd;
        }

        size_t getvalueFileGlobalOffset() const {
            return valueFileGlobalOffset;
        }

        size_t getMapFileSize () const {
            return keyFileSize;
        }

        char *getKeyBuffer() const {
            return MapBuffer;
        }

        char *getBlockBuffer() const {
            return blockBuffer;
        }
    };
}


#endif //ENGINE_VALUELOGFILE_H
