/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.persistence;

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Interface for reading transaction logs.
 * 
 * 操作事务日志文件log.xxxx的接口
 *
 */
public interface TxnLog {
    
    /**
     * roll the current
     * log being appended to
     * @throws IOException 
     * 切换日志
     * 
     */
    void rollLog() throws IOException;
    /**
     * Append a request to the transaction log
     * @param hdr the transaction header
     * @param r the transaction itself
     * returns true iff something appended, otw false 
     * @throws IOException
     * 
     * append一条事务
     * 
     */
    boolean append(TxnHeader hdr, Record r) throws IOException;

    /**
     * Start reading the transaction logs
     * from a given zxid
     * @param zxid
     * @return returns an iterator to read the 
     * next transaction in the logs.
     * @throws IOException
     * 
     * 读一条事务记录--根据事务id
     * 
     */
    TxnIterator read(long zxid) throws IOException;
    
    /**
     * the last zxid of the logged transactions.
     * @return the last zxid of the logged transactions.
     * @throws IOException
     * 获取该文件中最后的一条事务id
     * 
     */
    long getLastLoggedZxid() throws IOException;
    
    /**
     * truncate the log to get in sync with the 
     * leader.
     * @param zxid the zxid to truncate at.
     * @throws IOException 
     * 截断到zxid这条事务---包括zxid吗？ TODO
     * 
     */
    boolean truncate(long zxid) throws IOException;
    
    /**
     * the dbid for this transaction log. 
     * @return the dbid for this transaction log.
     * @throws IOException
     * 
     * 什么？---TODO
     */
    long getDbId() throws IOException;
    
    /**
     * commmit the trasaction and make sure
     * they are persisted
     * @throws IOException
     * 
     * commit这个事务日志、确保持久化。
     * 
     */
    void commit() throws IOException;
   
    /** 
     * close the transactions logs
     */
    void close() throws IOException;
    
    
    
    /**
     * an iterating interface for reading 
     * transaction logs.
     * 
     * 代表一条事务的迭代器
     *  
     */
    public interface TxnIterator {
        /**
         * return the transaction header.
         * @return return the transaction header.
         * 
         * 事务header
         */
        TxnHeader getHeader();
        
        /**
         * return the transaction record.
         * @return return the transaction record.
         * 
         * 事务正体
         * 
         */
        Record getTxn();
     
        /**
         * go to the next transaction record.
         * @throws IOException
         * 
         * 指向下一条事务的指针
         */
        boolean next() throws IOException;
        
        /**
         * close files and release the 
         * resources
         * @throws IOException
         */
        void close() throws IOException;
    }
}

