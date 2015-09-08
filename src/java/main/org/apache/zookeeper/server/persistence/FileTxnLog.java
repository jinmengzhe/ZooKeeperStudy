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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This class implements the TxnLog interface. It provides api's
 * to access the txnlogs and add entries to it.
 * <p>
 * The format of a Transactional log is as follows:
 * <blockquote><pre>
 * 
 * 1 log.xxx文件由以下三部分组成
 * LogFile:
 *     FileHeader TxnList ZeroPad
 * 
 * 2 FileHeader的格式
 * FileHeader: {
 *     magic 4bytes (ZKLG)
 *     version 4bytes
 *     dbid 8bytes
 *   }
 * 
 * 3 TxnList的格式:由若干行Txn组成、即每一行是一条日志
 * TxnList:
 *     Txn || Txn TxnList
 * 
 * 
 * 3.1 解释每一行Txn的格式
 * Txn:
 *     checksum Txnlen TxnHeader Record 0x42
 * 
 * 3.2 Txn每个字段详解
 * checksum: 8bytes Adler32 is currently used
 *   calculated across payload -- Txnlen, TxnHeader, Record and 0x42
 * 
 * Txnlen:
 *     len 4bytes
 * 
 * TxnHeader: {
 *     sessionid 8bytes
 *     cxid 4bytes
 *     zxid 8bytes
 *     time 8bytes
 *     type 4bytes
 *   }
 *     
 * Record:
 *     See Jute definition file for details on the various record types
 * 
 * 0x42:
 * 	    写死的、每条记录后面都以0x42->EOR结束
 * 
 * 4 文件结束0 padded
 * ZeroPad:
 *     0 padded to EOF (filled during preallocation stage)
 * </pre></blockquote> 
 */
public class FileTxnLog implements TxnLog {
    private static final Logger LOG;

    // 2^26字节 = 2^6MB = 64MB
    static long preAllocSize =  65536 * 1024; 

    // FileHeader里面的魔幻数
    public final static int TXNLOG_MAGIC =
        ByteBuffer.wrap("ZKLG".getBytes()).getInt();

    // FileHeader里面的版本号
    public final static int VERSION = 2;

    /** Maximum time we allow for elapsed fsync before WARNing */
    // 多久做一次fsync
    private final static long fsyncWarningThresholdMS;

    static {
        LOG = Logger.getLogger(FileTxnLog.class);

        String size = System.getProperty("zookeeper.preAllocSize");
        if (size != null) {
            try {
                preAllocSize = Long.parseLong(size) * 1024;
            } catch (NumberFormatException e) {
                LOG.warn(size + " is not a valid value for preAllocSize");
            }
        }
        fsyncWarningThresholdMS = Long.getLong("fsync.warningthresholdms", 1000);
    }

    // 最后的zxid
    long lastZxidSeen;
    // 注意下面三个volatile变量
    // 带缓冲的文件流--与fos对应
    volatile BufferedOutputStream logStream = null;
    // jute定义的输出机构--与logSream对应
    volatile OutputArchive oa;
    // 正在写的文件输出流、与下面的变量File logFileWrite对应
    volatile FileOutputStream fos = null;

    
    // 存放目录
    File logDir;
    // 是否强制刷新 TODO
    private final boolean forceSync = !System.getProperty("zookeeper.forceSync", "yes").equals("no");;
    // FileHeader里面的dbid 没啥用 一直为0
    long dbId;
    // 尚未flush的log文件流
    private LinkedList<FileOutputStream> streamsToFlush =
        new LinkedList<FileOutputStream>();
    // 跟踪当前文件大小--终于明白了pad是干什么的 
    // 去zk目录下看日志大小会发现所有日志都是65MB 因为一开始就对日志进行了pad 分配了空间--没有写的部分是空的
    // 这样提高写的效率??? 
    long currentSize;
    // 正在写的log文件
    File logFileWrite = null;

    /**
     * constructor for FileTxnLog. Take the directory
     * where the txnlogs are stored
     * @param logDir the directory where the txnlogs are stored
     * 
     * 只需要目录名就行 FileTxnLog代表目录下的所有事务日志文件
     * 
     */
    public FileTxnLog(File logDir) {
        this.logDir = logDir;
    }

    /**
     * method to allow setting preallocate size
     * of log file to pad the file.
     * @param size the size to set to in bytes
     */
    public static void setPreallocSize(long size) {
        preAllocSize = size;
    }

    /**
     * creates a checksum alogrithm to be used
     * @return the checksum used for this txnlog
     * 
     * 使用Adler32()作为校验算法
     * 
     */
    protected Checksum makeChecksumAlgorithm(){
        return new Adler32();
    }


    /**
     * rollover the current log file to a new one.
     * @throws IOException
     * 
     * 切换到下一个要写的日志文件
     * 
     */
    public synchronized void rollLog() throws IOException {
        if (logStream != null) {
            this.logStream.flush();
            this.logStream = null;
            oa = null;
        }
    }

    /**
     * close all the open file handles
     * @throws IOException
     * 关闭所有打开的log文件
     * 
     */
    public synchronized void close() throws IOException {
        if (logStream != null) {
            logStream.close();
        }
        for (FileOutputStream log : streamsToFlush) {
            log.close();
        }
    }
    
    /**
     * append an entry to the transaction log
     * @param hdr the header of the transaction
     * @param txn the transaction part of the entry
     * returns true iff something appended, otw false 
     * 
     * 写一条事务日志 返回写成功or失败
     * 
     */
    public synchronized boolean append(TxnHeader hdr, Record txn)
        throws IOException
    {
        if (hdr != null) {
        	// 可能会重复写过去的zxid
            if (hdr.getZxid() <= lastZxidSeen) {
                LOG.warn("Current zxid " + hdr.getZxid()
                        + " is <= " + lastZxidSeen + " for "
                        + hdr.getType());
            }
            // 刚开始或者做了rollLog 开启一个新的日志文件
            if (logStream==null) {
               if(LOG.isInfoEnabled()){
                    LOG.info("Creating new log file: log." +  
                            Long.toHexString(hdr.getZxid()));
               }
               
               logFileWrite = new File(logDir, ("log." + 
                       Long.toHexString(hdr.getZxid())));
               fos = new FileOutputStream(logFileWrite);
               logStream=new BufferedOutputStream(fos);
               oa = BinaryOutputArchive.getArchive(logStream);
               // 写FileHeader
               FileHeader fhdr = new FileHeader(TXNLOG_MAGIC,VERSION, dbId);
               fhdr.serialize(oa, "fileheader");
               // Make sure that the magic number is written before padding.
               logStream.flush();
               currentSize = fos.getChannel().position();
               streamsToFlush.add(fos);
            }
            // 下面开始写入一条新的事务记录
            
            // TODO  提高写事务日志的效率 将大小pad到 +64MB
            padFile(fos);
            // “hdr” + “txn”
            byte[] buf = Util.marshallTxnEntry(hdr, txn);
            if (buf == null || buf.length == 0) {
                throw new IOException("Faulty serialization for header " +
                        "and txn");
            }
            // txnEntryCRC
            Checksum crc = makeChecksumAlgorithm();
            crc.update(buf, 0, buf.length);
            // 先写校验、再写内容、最后写结束符:“txnEntryCRC” + "txnEntry"(“hdr” + “txn”) + "EOR"—>0x42(B)
            oa.writeLong(crc.getValue(), "txnEntryCRC");
            Util.writeTxnBytes(oa, buf);
            
            return true;
        }
        return false;
    }

    /**
     * pad the current file to increase its size
     * @param out the outputstream to be padded
     * @throws IOException
     * 
     * 为什么要pad 已经解释过了
     * 
     */
    private void padFile(FileOutputStream out) throws IOException {
        currentSize = Util.padLogFile(out, currentSize, preAllocSize);
    }

    /**
     * Find the log file that starts at, or just before, the snapshot. Return
     * this and all subsequent logs. Results are ordered by zxid of file,
     * ascending order.
     * @param logDirList array of files
     * @param snapshotZxid return files at, or before this zxid
     * @return
     * 
     * 获取snapshotZxid之前(或者包括snapshotZxid)的所有log.xxx文件
     * 
     */
    public static File[] getLogFiles(File[] logDirList,long snapshotZxid) {
        List<File> files = Util.sortDataDir(logDirList, "log", true);
        long logZxid = 0;
        // Find the log file that starts before or at the same time as the
        // zxid of the snapshot
        for (File f : files) {
            long fzxid = Util.getZxidFromName(f.getName(), "log");
            if (fzxid > snapshotZxid) {
                continue;
            }
            // the files
            // are sorted with zxid's
            if (fzxid > logZxid) {
                logZxid = fzxid;
            }
        }
        List<File> v=new ArrayList<File>(5);
        for (File f : files) {
            long fzxid = Util.getZxidFromName(f.getName(), "log");
            if (fzxid < logZxid) {
                continue;
            }
            v.add(f);
        }
        return v.toArray(new File[0]);

    }

    /**
     * get the last zxid that was logged in the transaction logs
     * @return the last zxid logged in the transaction logs
     * 
     * 找到log.xxx里面最新的一个zxid
     * 
     */
    public long getLastLoggedZxid() {
    	// 首先找到最后的一个log.xxx文件
        File[] files = getLogFiles(logDir.listFiles(), 0);
        long maxLog=files.length>0?
                Util.getZxidFromName(files[files.length-1].getName(),"log"):-1;

        // 然后遍历该文件 找到最后一个zxid记录
        // if a log file is more recent we must scan it to find
        // the highest zxid
        long zxid = maxLog;
        try {
            FileTxnLog txn = new FileTxnLog(logDir);
            TxnIterator itr = txn.read(maxLog);
            while (true) {
                if(!itr.next())
                    break;
                TxnHeader hdr = itr.getHeader();
                zxid = hdr.getZxid();
            }
        } catch (IOException e) {
            LOG.warn("Unexpected exception", e);
        }
        return zxid;
    }

    /**
     * commit the logs. make sure that evertyhing hits the
     * disk
     * 将事务日志提交到磁盘
     * make sure!!!  确保所有带缓冲的事务内容都提交到磁盘
     * 
     */
    public synchronized void commit() throws IOException {
    	// flush当前
        if (logStream != null) {
            logStream.flush();
        }
        // flush缓冲的
        for (FileOutputStream log : streamsToFlush) {
            log.flush();
            if (forceSync) {
                long startSyncNS = System.nanoTime();

                log.getChannel().force(false);

                long syncElapsedMS =
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startSyncNS);
                if (syncElapsedMS > fsyncWarningThresholdMS) {
                    LOG.warn("fsync-ing the write ahead log in "
                            + Thread.currentThread().getName()
                            + " took " + syncElapsedMS
                            + "ms which will adversely effect operation latency. "
                            + "See the ZooKeeper troubleshooting guide");
                }
            }
        }
        // 清空缓存的stream
        while (streamsToFlush.size() > 1) {
            streamsToFlush.removeFirst().close();
        }
    }

    /**
     * start reading all the transactions from the given zxid
     * @param zxid the zxid to start reading transactions from
     * @return returns an iterator to iterate through the transaction
     * logs
     * 
     * 从zxid实现事务日志的迭代--参见FileTxnIterator的实现
     * 
     */
    public TxnIterator read(long zxid) throws IOException {
        return new FileTxnIterator(logDir, zxid);
    }

    /**
     * truncate the current transaction logs
     * @param zxid the zxid to truncate the logs to
     * @return true if successful false if not
     * 
     * 截断后不包括zxid
     * 
     */
    public boolean truncate(long zxid) throws IOException {
        FileTxnIterator itr = new FileTxnIterator(this.logDir, zxid);
        PositionInputStream input = itr.inputStream;
        // 要截断的位置--从这截断就不包括zxid这一条
        long pos = input.getPosition();
        // now, truncate at the current position
        // 从zxid截断--不包括
        RandomAccessFile raf=new RandomAccessFile(itr.logFile,"rw");
        raf.setLength(pos);
        raf.close();
        // 将后面的事务文件删除
        while(itr.goToNextLog()) {
            if (!itr.logFile.delete()) {
                LOG.warn("Unable to truncate " + itr.logFile);
            }
        }
        return true;
    }

    /**
     * read the header of the transaction file
     * @param file the transaction file to read
     * @return header that was read fomr the file
     * @throws IOException
     * 
     * 从日志文件读出FileHeader 没用到
     * 
     */
    private static FileHeader readHeader(File file) throws IOException {
        InputStream is =null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            InputArchive ia=BinaryInputArchive.getArchive(is);
            FileHeader hdr = new FileHeader();
            hdr.deserialize(ia, "fileheader");
            return hdr;
         } finally {
             try {
                 if (is != null) is.close();
             } catch (IOException e) {
                 LOG.warn("Ignoring exception during close", e);
             }
         }
    }

    /**
     * the dbid of this transaction database
     * @return the dbid of this database
     * 
     * 没用 dbid没用到
     * 
     */
    public long getDbId() throws IOException {
        FileTxnIterator itr = new FileTxnIterator(logDir, 0);
        FileHeader fh=readHeader(itr.logFile);
        itr.close();
        if(fh==null)
            throw new IOException("Unsupported Format.");
        return fh.getDbid();
    }

    /**
     * the forceSync value. true if forceSync is enabled, false otherwise.
     * @return the forceSync value
     */
    public boolean isForceSync() {
        return forceSync;
    }

    /**
     * a class that keeps track of the position 
     * in the input stream. The position points to offset
     * that has been consumed by the applications. It can 
     * wrap buffered input streams to provide the right offset 
     * for the application.
     * 
     * 带position跟踪的输入流
     * 可以getPosition()返回当前读了多少字节---在truncated里面有用
     * 其他使用方式跟InputStream一致
     * 
     */
    static class PositionInputStream extends FilterInputStream {
        long position;
        protected PositionInputStream(InputStream in) {
            super(in);
            position = 0;
        }
        
        @Override
        public int read() throws IOException {
            int rc = super.read();
            if (rc > -1) {
                position++;
            }
            return rc;
        }

        public int read(byte[] b) throws IOException {
            int rc = super.read(b);
            if (rc > 0) {
                position += rc;
            }
            return rc;            
        }
        
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int rc = super.read(b, off, len);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        
        @Override
        public long skip(long n) throws IOException {
            long rc = super.skip(n);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        public long getPosition() {
            return position;
        }

        // 不支持mark和reset 一次性读完
        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readLimit) {
            throw new UnsupportedOperationException("mark");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("reset");
        }
    }
    
    /**
     * this class implements the txnlog iterator interface
     * which is used for reading the transaction logs
     * 
     * 迭代器 事务日志的迭代器 --跨越多个日志文件
     * 
     */
    public static class FileTxnIterator implements TxnLog.TxnIterator {
        // 日志目录
    	File logDir;
    	// 从zxid开始迭代
        long zxid;
        // 正在迭代的那条事务的hdr
        TxnHeader hdr;
        // 正在迭代的那条事务的record
        Record record;
        // 正在迭代的那个日志文件
        File logFile;
        // 正在进行迭代的日志文件对应的ia
        InputArchive ia;
        // 校验失败
        static final String CRC_ERROR="CRC check failed";
        // 正在进行迭代的带position跟踪的输入流
        PositionInputStream inputStream=null;
        //stored files is the list of files greater than
        //the zxid we are looking for.
        // zxid之后的事务文件--后续迭代会用到
        private ArrayList<File> storedFiles;

        /**
         * create an iterator over a transaction database directory
         * @param logDir the transaction database directory
         * @param zxid the zxid to start reading from
         * @throws IOException
         * 在事务数据库logDir下面迭代 从zxid开始--包含zxid
         * 
         */
        public FileTxnIterator(File logDir, long zxid) throws IOException {
          this.logDir = logDir;
          this.zxid = zxid;
          init();
        }

        /**
         * initialize to the zxid specified
         * this is inclusive of the zxid
         * @throws IOException
         */
        void init() throws IOException {
            storedFiles = new ArrayList<File>();
            // 降序排列
            List<File> files = Util.sortDataDir(FileTxnLog.getLogFiles(logDir.listFiles(), 0), "log", false);
            for (File f: files) {
            	// >= zxid的
                if (Util.getZxidFromName(f.getName(), "log") >= zxid) {
                    storedFiles.add(f);
                }
                // add the last logfile that is less than the zxid
                // 只要第一个比zxid小的文件
                else if (Util.getZxidFromName(f.getName(), "log") < zxid) {
                    storedFiles.add(f);
                    break;
                }
            }
            // 
            goToNextLog();
            if (!next())
                return;
            while (hdr.getZxid() < zxid) {
                if (!next())
                    return;
            }
        }

        /**
         * go to the next logfile
         * @return true if there is one and false if there is no
         * new file to be read
         * @throws IOException
         * 
         * 从storedFiles取出下一个迭代的文件
         */
        private boolean goToNextLog() throws IOException {
            if (storedFiles.size() > 0) {
                this.logFile = storedFiles.remove(storedFiles.size()-1);
                ia = createInputArchive(this.logFile);
                return true;
            }
            return false;
        }

        /**
         * read the header from the inputarchive
         * @param ia the inputarchive to be read from
         * @param is the inputstream
         * @throws IOException
         * 
         * 当从文件创建is和ia的时候 检查文件头的合法性
         * 
         */
        protected void inStreamCreated(InputArchive ia, InputStream is)
            throws IOException{
            FileHeader header= new FileHeader();
            header.deserialize(ia, "fileheader");
            if (header.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
                throw new IOException("Transaction log: " + this.logFile + " has invalid magic number " 
                        + header.getMagic()
                        + " != " + FileTxnLog.TXNLOG_MAGIC);
            }
        }

        /**
         * Invoked to indicate that the input stream has been created.
         * @param ia input archive
         * @param is file input stream associated with the input archive.
         * @throws IOException
         * 
         * 创建logFile对应的inputStream和ia
         * 这里处理的很丑陋啊 读完一个inputStream后会将它置为null 这样才会执行if(inputStream==null)
         * 参见next()方法里面 在异常处理中来做这个逻辑 
         * 
         **/
        protected InputArchive createInputArchive(File logFile) throws IOException {
            if(inputStream==null){
                inputStream= new PositionInputStream(new BufferedInputStream(new FileInputStream(logFile)));
                LOG.debug("Created new input stream " + logFile);
                ia  = BinaryInputArchive.getArchive(inputStream);
                inStreamCreated(ia,inputStream);
                LOG.debug("Created new input archive " + logFile);
            }
            return ia;
        }

        /**
         * create a checksum algorithm
         * @return the checksum algorithm
         */
        protected Checksum makeChecksumAlgorithm(){
            return new Adler32();
        }

        /**
         * the iterator that moves to the next transaction
         * @return true if there is more transactions to be read
         * false if not.
         * 
         * 迭代到下一条事务日志--注意这里如何维护跨日志文件 
         * 个人认为实现比较丑陋
         * 
         */
        public boolean next() throws IOException {
            if (ia == null) {
                return false;
            }
            try {
            	// TODO: "crcvalue"??? 写的时候不是以“txnEntryCRC”写的吗？？？
            	// 明白了 这里写和读的时候的tag确实是写错了 不一致
            	// 不过参见ia的实现 发现读基本类型的时候直接安装数据长度去读的 并没有使用标签
            	// 事实上标签在这里没有用
                long crcValue = ia.readLong("crcvalue");
                byte[] bytes = Util.readTxnBytes(ia);
                // Since we preallocate, we define EOF to be an
                if (bytes == null || bytes.length==0)
                   throw new EOFException("Failed to read");
                // EOF or corrupted record
                // validate CRC
                Checksum crc = makeChecksumAlgorithm();
                crc.update(bytes, 0, bytes.length);
                if (crcValue != crc.getValue())
                    throw new IOException(CRC_ERROR);
                if (bytes == null || bytes.length == 0)
                    return false;
                InputArchive iab = BinaryInputArchive
                                    .getArchive(new ByteArrayInputStream(bytes));
                hdr = new TxnHeader();
                record = SerializeUtils.deserializeTxn(iab, hdr);
            } catch (EOFException e) {
            	// 无论是发现损坏的记录还是到达文件末尾
            	// 都以抛出异常的形式来进入到下一个文件
            	// TODO  这里是否有些问题？？？？文件损坏怎么办？？？
                LOG.debug("EOF excepton " + e);
                inputStream.close();
                inputStream = null;
                ia = null;
                hdr = null;
                // 进入下一个文件
                // thsi means that the file has ended
                // we shoud go to the next file
                if (!goToNextLog()) {
                    return false;
                }
                // 进入下一条记录
                // if we went to the next log file, we should call next() again
                return next();
            }
            return true;
        }

        /**
         * reutrn the current header
         * @return the current header that
         * is read
         */
        public TxnHeader getHeader() {
            return hdr;
        }

        /**
         * return the current transaction
         * @return the current transaction
         * that is read
         */
        public Record getTxn() {
            return record;
        }

        /**
         * close the iterator
         * and release the resources.
         */
        public void close() throws IOException {
            inputStream.close();
        }
    }
}
