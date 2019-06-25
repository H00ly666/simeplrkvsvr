/**
 * Created by 刘嘉辉 on 11/22/18.
 * Copyright (c) 2018 刘嘉辉 All rights reserved.
 * @brief To immplmente mmap keyFiles and get fd of valueFiles.
 */

#ifndef ENGINE_KVMAPENTRY_H
#define ENGINE_KVMAPENTRY_H

class KVMapEntry {

    public :
        //无惨默认构造函数必须显式的声明出来
        KVMapEntry() {
        }

        KVMapEntry(int fileID, size_t offset) : fileID(fileID), offset(offset) {
        }

        int getvaluelength() const {
            return valuelength;
        }

        void setvaluelength (const int value_length) {
            valuelength = value_length;
        }

        int getfileID() const {
            return fileID;
        }

        void setfileID  (const int file_ID) {
            fileID = file_ID;
        }

        int getvaluefd() const {
            return valuefd;
        }

        void setvaluefd  (const int value_fd) {
            valuefd = value_fd;
        }

        size_t& getoffset() {
            return offset;
        }

        void setoffset(const size_t& off_set) {
            offset = off_set;
        }

    private :
        //value的长度
        int valuelength;

        //value的文件id
        int fileID;

        //value的文件fd  从kvfiles来
        int  valuefd;

        //value的偏移量
        size_t offset;
};

#endif
