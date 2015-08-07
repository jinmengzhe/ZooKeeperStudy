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

package org.apache.zookeeper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ZooTrace;

/**
 * This class manages the socket i/o for the client. ClientCnxn maintains a list
 * of available servers to connect to and "transparently" switches servers it is
 * connected to as needed.
 * 
 * 1 客户端上下文：管理可连接的servers scoket i/o
 * 2 实际的、或者说更为底层的客户端操作的请求、都在这个类里完成。更为上层的Zookeeper类(也就是用户直接使用的入口)都是调用这个类
 * 3 这个类很重要 所有的实现都在ClientCnxn和Zookeeper这两个类里面---代码比较多、详细注释在实现中。
 * 
 * 
 */
public class ClientCnxn {
    private static final Logger LOG = Logger.getLogger(ClientCnxn.class);

    /** This controls whether automatic watch resetting is enabled.
     * Clients automatically reset watches during session reconnect, this
     * option allows the client to turn off this behavior by setting
     * the environment variable "zookeeper.disableAutoWatchReset" to "true"
     * 
     *  是否关闭自动watch reset---当session重连时、客户端会自动重置这些watche(这些watch在客户端缓存)
     *  默认是false、即支持自动恢复watch。可以通过配置设置为不支持。
     *  
     *  */
    private static boolean disableAutoWatchReset;

    /**
     * packet包的大小、限制为4096 * 1024(2^22)--这个值已经足够大
     * 
     * */
    public static final int packetLen;
    static {
        // this var should not be public, but otw there is no easy way
        // to test
        disableAutoWatchReset =
            Boolean.getBoolean("zookeeper.disableAutoWatchReset");
        if (LOG.isDebugEnabled()) {
            LOG.debug("zookeeper.disableAutoWatchReset is "
                    + disableAutoWatchReset);
        }
        packetLen = Integer.getInteger("jute.maxbuffer", 4096 * 1024);
    }

    /**
     * 可以连接的zk server socket 列表
     * 容易理解
     * */
    private final ArrayList<InetSocketAddress> serverAddrs =
        new ArrayList<InetSocketAddress>();

    /**
     * 安全验证方式
     * */
    static class AuthData {
        AuthData(String scheme, byte data[]) {
            this.scheme = scheme;
            this.data = data;
        }

        String scheme;

        byte data[];
    }

    /**
     * 支持多种验证方式
     * 
     * */
    private final ArrayList<AuthData> authInfo = new ArrayList<AuthData>();

    /**
     * These are the packets that have been sent and are waiting for a response.
     * 已发送等待响应的packet队列
     * 
     */
    private final LinkedList<Packet> pendingQueue = new LinkedList<Packet>();

    /**
     * These are the packets that need to be sent.
     * 
     * 等待发送的packet队列
     */
    private final LinkedList<Packet> outgoingQueue = new LinkedList<Packet>();

    /**
     * 维护下一个应该连接的zk实例下标--后面的代码里我们来分析选择server的策略
     * 
     * */
    private int nextAddrToTry = 0;

    /**
     * connectTimeout起始时设置为sessionTimeout/server个数
     * 后面如果连接成功设置为实际的negotiatedSessionTimeout/server个数
     * 
     * */
    private int connectTimeout;

    /** The timeout in ms the client negotiated with the server. This is the 
     *  "real" timeout, not the timeout request by the client (which may
     *  have been increased/decreased by the server which applies bounds
     *  to this value.
     *  
     *  客户端与server之间协商的超时时间、这是实际的超时时间
     *  它是由连接成功后的ConnectResponse返回的timeOut--由server端记载的
     *  注意它是volatile的--后续解释
     */
    private volatile int negotiatedSessionTimeout;

    /**
     * 策略同connectTimeout、初始时设置为sessionTimeout的2/3
     * 后面设置为实际的negotiatedSessionTimeout的2/3
     * ---
     * 
     * */
    private int readTimeout;

    /**
     * 所有连接的超时时间 这个值是用户从Zookeeper类入口直接设置的
     * 
     * */
    private final int sessionTimeout;

    /**
     * 持有该上下文的Zookeeper实例、它们之间互相引用以进行一些内部操作
     * 参考Zookeeper的构造方法
     * 
     * */
    private final ZooKeeper zooKeeper;

    /**
     * ClientWatchManager接口、用于挑选需要处理的watcher集合
     * 该接口的实现在Zookeeper类中、在Zookeeper的构造方法中传入ClientCnxn
     * 
     * */
    private final ClientWatchManager watcher;

    /**
     * 此次连接的sessionId
     * */
    private long sessionId;

    /**
     * 此次连接的验证信息
     * 
     * */
    private byte sessionPasswd[] = new byte[16];

    /**
     * 支持以rootPath的方式去连
     * 
     * */
    final String chrootPath;

    /**
     * 发送请求和接受响应、维持心跳的线程
     * 核心代码
     * 
     * */
    final SendThread sendThread;

    /**
     * 事件处理的线程--处理到server的通知、监听事件、以及api调用的回调事件
     * 核心代码
     * 
     * */
    final EventThread eventThread;

    /**
     * Set to true when close is called. Latches the connection such that we
     * don't attempt to re-connect to the server if in the middle of closing the
     * connection (client sends session disconnect to server as part of close
     * operation)
     * 
     * 用于状态维护、当正在进行close操作时置为true、锁存re-connect操作
     * 注意volatile
     */
    volatile boolean closing = false;

    public long getSessionId() {
        return sessionId;
    }

    public byte[] getSessionPasswd() {
        return sessionPasswd;
    }

    public int getSessionTimeout() {
        return negotiatedSessionTimeout;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        SocketAddress local = getLocalSocketAddress();
        SocketAddress remote = getRemoteSocketAddress();
        sb
            .append("sessionid:0x").append(Long.toHexString(getSessionId()))
            .append(" local:").append(local)
            .append(" remoteserver:").append(remote)
            .append(" lastZxid:").append(lastZxid)
            .append(" xid:").append(xid)
            .append(" sent:").append(sendThread.sentCount)
            .append(" recv:").append(sendThread.recvCount)
            .append(" queuedpkts:").append(outgoingQueue.size())
            .append(" pendingresp:").append(pendingQueue.size())
            .append(" queuedevents:").append(eventThread.waitingEvents.size());

        return sb.toString();
    }
    
    /**
     * Returns the address to which the socket is connected.
     * @return ip address of the remote side of the connection or null if
     *         not connected
     *         返回当前连接上的server的socket地址
     */
    SocketAddress getRemoteSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel)sendThread.sockKey.channel())
                .socket().getRemoteSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    /** 
     * Returns the local address to which the socket is bound.
     * @return ip address of the remote side of the connection or null if
     *         not connected
     *         当前连接的本地scoket地址
     */
    SocketAddress getLocalSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel)sendThread.sockKey.channel())
                .socket().getLocalSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * This class allows us to pass the headers and the relevant records around.
     * 
     * 传输的数据包：
     * 
     * 
     */
    static class Packet {
    	/**  
    	 * 请求header
    	 * */
        RequestHeader header;

        /**
         * 数据包实际读写的缓冲区 会将请求包序列化到ByteBuffer中
         * 
         * */
        ByteBuffer bb;

        /** Client's view of the path (may differ due to chroot)
         * 	客户端看到的path 注意chroot
         *  
         *  **/
        String clientPath;
        /** Servers's view of the path (may differ due to chroot) 
         * 	server端看到的path
         * 
         * **/
        String serverPath;

        /**
         * 响应的header
         * */
        ReplyHeader replyHeader;

        /**
         * 请求体正文
         * 
         * */
        Record request;

        /**
         * 响应体正文
         * 
         * */
        Record response;

        /**
         * 数据包是否处理完毕--
         * 数据包packet的处理包括几个阶段：
         * 1 提交请求包数据给服务端--参见submitRequest
         * 2 将packet中的watcher注册到本地维护(通过watcherManager) + 将packet放入EventThread队列、等待api回调处理。--参见finishPacket()
         * 
         * 注意1和2之间的关系、在submitRequest中定义了提交数据包的过程、然后packet.wait()来等待finishPacket()的notifyAll
         * 
         * */
        boolean finished;

        /**
         * 异步回调 用于处理客户端api中定义的 当api操作完后 对服务端的响应做回调处理
         * 
         * */
        AsyncCallback cb;

        /**
         * 擦 终于搞明白这个参数、ctx是回调的上下文、由用户定义。
         * 在Zookeeper类的api里传入、用户在回调处理结果的时候可能需要传入一些自己的东西作为上下文。
         * 
         * */
        Object ctx;

        /**
         * watch注册对象 在Zookeeper里定义的
         * 用于指定该packet定义的操作要添加的注册对象
         * 
         * */
        WatchRegistration watchRegistration;

        /**
         * 1 packet是一个完整的数据包：请求 + 响应 + watch注册
         * 	 @header   请求头
         * 	 @record   请求体
         * 	 @bb       len + header + record序列化到bb中
         *   以上定义了发给服务端的完整请求
         * 
         *   @relayHeader  响应头
         *   @response     响应体
         *   以上定义了服务端响应的完整内容
         *   
         *   @watchRegistration
         *   本次请求附带注册哪些watch--这个是在客户端自己维护的 根据请求的执行结果来决定是否添加watch
         *   
         *   这种packet模式是网络编程里最最常见的packet结构、很多开源软件里都是这样定义的。不了解的同学可以仔细看下。
         *   
         *   
         * 2 bb定义请求包、并将完整的请求序列化到字节缓冲区bb中
         *   请求包格式（即bb的格式）为：
         * 							len(int型、4字节、表示header + request的长度) + header(请求header) + request(请求体)
         *   note1：len表示后面两个段的长度、不包括自己。这是非常常见的一种处理 在网络编程中带来很多好处。
         *   note2：len在实现上为延迟填充、这个也是极为常用的一种技术。
         * 
         * 
         * 以上分析、见packet代码。--理解packet的这种定义方式对于学习很多网络编程相关的东西都是有益的。
         * 
         * */
        Packet(RequestHeader header, ReplyHeader replyHeader, Record record,
                Record response, ByteBuffer bb,
                WatchRegistration watchRegistration) {
            this.header = header;
            this.replyHeader = replyHeader;
            this.request = record;
            this.response = response;
            if (bb != null) {
                this.bb = bb;
            } else {
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    BinaryOutputArchive boa = BinaryOutputArchive
                            .getArchive(baos);
                    boa.writeInt(-1, "len"); // We'll fill this in later
                    header.serialize(boa, "header");
                    if (record != null) {
                        record.serialize(boa, "request");
                    }
                    baos.close();
                    this.bb = ByteBuffer.wrap(baos.toByteArray());
                    this.bb.putInt(this.bb.capacity() - 4);
                    this.bb.rewind();
                } catch (IOException e) {
                    LOG.warn("Ignoring unexpected exception", e);
                }
            }
            this.watchRegistration = watchRegistration;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("clientPath:" + clientPath);
            sb.append(" serverPath:" + serverPath);
            sb.append(" finished:" + finished);

            sb.append(" header:: " + header);
            sb.append(" replyHeader:: " + replyHeader);
            sb.append(" request:: " + request);
            sb.append(" response:: " + response);

            // jute toString is horrible, remove unnecessary newlines
            return sb.toString().replaceAll("\r*\n+", " ");
        }
    }


    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param hosts
     *                a comma separated list of hosts that can be connected to.
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @throws IOException
     */
    public ClientCnxn(String hosts, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher)
        throws IOException
    {
        this(hosts, sessionTimeout, zooKeeper, watcher, 0, new byte[16]);
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param hosts
     *                a comma separated list of hosts that can be connected to.
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param sessionId session id if re-establishing session
     * @param sessionPasswd session passwd if re-establishing session
     * @throws IOException
     */
    public ClientCnxn(String hosts, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, long sessionId, byte[] sessionPasswd)
        throws IOException
    {
        this.zooKeeper = zooKeeper;
        this.watcher = watcher;
        this.sessionId = sessionId;
        this.sessionPasswd = sessionPasswd;

        // chroot方式处理
        // parse out chroot, if any
        int off = hosts.indexOf('/');
        if (off >= 0) {
            String chrootPath = hosts.substring(off);
            // ignore "/" chroot spec, same as null
            if (chrootPath.length() == 1) {
                this.chrootPath = null;
            } else {
                PathUtils.validatePath(chrootPath);
                this.chrootPath = chrootPath;
            }
            hosts = hosts.substring(0,  off);
        } else {
            this.chrootPath = null;
        }

        // serverAddrs列表
        String hostsList[] = hosts.split(",");
        for (String host : hostsList) {
            int port = 2181;
            int pidx = host.lastIndexOf(':');
            if (pidx >= 0) {
                // otherwise : is at the end of the string, ignore
                if (pidx < host.length() - 1) {
                    port = Integer.parseInt(host.substring(pidx + 1));
                }
                host = host.substring(0, pidx);
            }
            InetAddress addrs[] = InetAddress.getAllByName(host);
            for (InetAddress addr : addrs) {
                serverAddrs.add(new InetSocketAddress(addr, port));
            }
        }
        // 注意一系列的timeout是如何定义的 
        this.sessionTimeout = sessionTimeout;
        connectTimeout = sessionTimeout / hostsList.length;
        readTimeout = sessionTimeout * 2 / 3;
        Collections.shuffle(serverAddrs);// 乱序
        sendThread = new SendThread();
        eventThread = new EventThread();
    }

    /**
     * tests use this to check on reset of watches
     * @return if the auto reset of watches are disabled
     */
    public static boolean getDisableAutoResetWatch() {
        return disableAutoWatchReset;
    }
    /**
     * tests use this to set the auto reset
     * @param b the vaued to set disable watches to
     */
    public static void setDisableAutoResetWatch(boolean b) {
        disableAutoWatchReset = b;
    }
    
    /**启动该上下文实际上就是启动这两个主体线程**/
    public void start() {
        sendThread.start();
        eventThread.start();
    }

    /**
     * 上下文死掉的事件 特殊定义的通知关闭客户端的事件
     * 
     * */
    Object eventOfDeath = new Object();

    /**
     * 在sendThread和eventThread定义未catch异常的处理
     * 
     * */
    final static UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            LOG.error("from " + t.getName(), e);
        }
    };

    /**
     * @event  客户端监听到的事件：server状态+事件类型+事件路径
     * @watchers  与此事件相关的客户端维护的watchers---这个由watcherManager挑选出来的
     * 
     * */
    private static class WatcherSetEventPair {
        private final Set<Watcher> watchers;
        private final WatchedEvent event;

        public WatcherSetEventPair(Set<Watcher> watchers, WatchedEvent event) {
            this.watchers = watchers;
            this.event = event;
        }
    }

    /**
     * Guard against creating "-EventThread-EventThread-EventThread-..." thread
     * names when ZooKeeper object is being created from within a watcher.
     * See ZOOKEEPER-795 for details.
     */
    private static String makeThreadName(String suffix) {
        String name = Thread.currentThread().getName().
            replaceAll("-EventThread", "");
        return name + suffix;
    }

    /**
     * 客户端事件处理的后台线程
     * 
     * */
    class EventThread extends Thread {
    	/**
    	 * 客户端自己维护的等待处理的事件阻塞队列：---当没有事件时、出队一直阻塞
    	 * Object类型为了支持队列中支持两种类型的事件：TODO
    	 * 			WatcherSetEventPair:  用于处理watcher
    	 * 			Packet: 注意这种方式我们平时使用还是蛮少的、在Zookeeper的api里有这个CallBack的参数。
    	 * 				    用于处理在这个api执行完后、对服务端的响应执行怎样的回调---区别于watcher
    	 * 
    	 * */
        private final LinkedBlockingQueue<Object> waitingEvents =
            new LinkedBlockingQueue<Object>();

        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        private volatile KeeperState sessionState = KeeperState.Disconnected;

        /**
         * 是否接受到了kill通知、当发现队列中有eventOfDeath事件(调用disconnect时将该事件入队)、表示接收到了kill通知。
         * 它只表示接受到了kill事件 作为一种状态的来通知 线程此时还没有停止运行---为了将队列中的剩余事件执行完再关闭
         * 
         * */
       private volatile boolean wasKilled = false;
       /**
        * 实际的表示此线程是否还在run的标志、当检测到wasKilled以后、确认事件队列为空后、就停止运行。
        * 
        * */
       private volatile boolean isRunning = false;

        EventThread() {
            super(makeThreadName("-EventThread"));
            setUncaughtExceptionHandler(uncaughtExceptionHandler);
            setDaemon(true);
        }

        /**
         * 观察到事件WatchedEvent、挑选出在此事件上add的watchers。
         * 即将WatcherSetEventPair加入队列
         * 
         * */
        public void queueEvent(WatchedEvent event) {
            if (event.getType() == EventType.None
                    && sessionState == event.getState()) {
                return;
            }
            sessionState = event.getState();

            // materialize the watchers based on the event
            WatcherSetEventPair pair = new WatcherSetEventPair(
                    watcher.materialize(event.getState(), event.getType(),
                            event.getPath()),
                            event);
            // queue the pair (watch set & event) for later processing
            waitingEvents.add(pair);
        }

        /**
         * 将请求包Packet加入事件处理队列--api的事件回调
         * 
         * */
       public void queuePacket(Packet packet) {
          if (wasKilled) {
             synchronized (waitingEvents) {
                if (isRunning) waitingEvents.add(packet);
                else processEvent(packet);
             }
          } else {
             waitingEvents.add(packet);
          }
       }

       /**
        * 特殊的事件 断开连接时加入队列
        * 
        * */
        public void queueEventOfDeath() {
            waitingEvents.add(eventOfDeath);
        }

        /**
         * 死循环、阻塞式出队、处理事件。
         * waskilled用于标志是否接收到eventOfDeath事件
         * isRunning用于控制在接受到killed事件后、将剩余的队列中的事件执行完、然后退出死循环、即线程执行完毕退出。
         * 
         * */
        @Override
        public void run() {
           try {
              isRunning = true;
              while (true) {
            	 // 阻塞等待
                 Object event = waitingEvents.take();	
                 if (event == eventOfDeath) {
                    wasKilled = true;
                 } else {
                    processEvent(event);
                 }
                 // 当接收到eventOfDeath事件后、还需要将剩余事件执行完然后退出
                 if (wasKilled)
                    synchronized (waitingEvents) {
                       if (waitingEvents.isEmpty()) {
                          isRunning = false;
                          break;
                       }
                    }
              }
           } catch (InterruptedException e) {
              LOG.error("Event thread exiting due to interruption", e);
           }

            LOG.info("EventThread shut down");
        }

        /**
         * 事件处理的具体逻辑：
         * 1 WatcherSetEventPair事件：Watcher是由用户实现的、回调process方法
         * 2 Packet事件：执行api时指定的该api执行后的回调事件
         * 
         * */
       private void processEvent(Object event) {
          try {
              if (event instanceof WatcherSetEventPair) {
            	  /**
            	   * watch事件
            	   * 
            	   * */
                  // each watcher will process the event
                  WatcherSetEventPair pair = (WatcherSetEventPair) event;
                  for (Watcher watcher : pair.watchers) {
                      try {
                          watcher.process(pair.event);
                      } catch (Throwable t) {
                          LOG.error("Error while calling watcher ", t);
                      }
                  }
              } else {
            	  /**
            	   * api回调事件
            	   * 
            	   * */
                  Packet p = (Packet) event;
                  int rc = 0;
                  String clientPath = p.clientPath;
                  if (p.replyHeader.getErr() != 0) {
                      rc = p.replyHeader.getErr();
                  }
                  // 对于没有回调的packet、直接通过。
                  if (p.cb == null) {
                      LOG.warn("Somehow a null cb got to EventThread!");
                  } 
                  /**
                   * 以下都是指定了api回调的packet
                   * 根据packet中的响应类型确定执行哪一个回调
                   * 
                   * */
                  else if (p.response instanceof ExistsResponse
                          || p.response instanceof SetDataResponse
                          || p.response instanceof SetACLResponse) {
                	  /**
                	   * StatCallback:
                	   * 参见Zookeeper的api、这三种操作里面可以传入参数StatCallback
                	   * 
                	   * */
                      StatCallback cb = (StatCallback) p.cb;
                      if (rc == 0) {
                          if (p.response instanceof ExistsResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((ExistsResponse) p.response)
                                              .getStat());
                          } else if (p.response instanceof SetDataResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((SetDataResponse) p.response)
                                              .getStat());
                          } else if (p.response instanceof SetACLResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((SetACLResponse) p.response)
                                              .getStat());
                          }
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof GetDataResponse) {
                	  /**
                	   * DataCallback
                	   * 
                	   * */
                      DataCallback cb = (DataCallback) p.cb;
                      GetDataResponse rsp = (GetDataResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getData(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null,
                                  null);
                      }
                  } else if (p.response instanceof GetACLResponse) {
                	  /**
                	   * ACLCallback
                	   * 
                	   * */
                      ACLCallback cb = (ACLCallback) p.cb;
                      GetACLResponse rsp = (GetACLResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getAcl(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null,
                                  null);
                      }
                  } else if (p.response instanceof GetChildrenResponse) {
                	  /**
                	   * ChildrenCallback
                	   * 
                	   * */
                      ChildrenCallback cb = (ChildrenCallback) p.cb;
                      GetChildrenResponse rsp = (GetChildrenResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getChildren());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof GetChildren2Response) {
                	  /**
                	   * Children2Callback--多一个对stat的关注
                	   * 
                	   * */
                      Children2Callback cb = (Children2Callback) p.cb;
                      GetChildren2Response rsp = (GetChildren2Response) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getChildren(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null, null);
                      }
                  } else if (p.response instanceof CreateResponse) {
                	  /**
                	   * StringCallback 对于创建节点的api回调
                	   * 
                	   * */
                      StringCallback cb = (StringCallback) p.cb;
                      CreateResponse rsp = (CreateResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx,
                                  (chrootPath == null
                                          ? rsp.getPath()
                                          : rsp.getPath()
                                    .substring(chrootPath.length())));
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.cb instanceof VoidCallback) {
                	  /**
                	   * VoidCallback
                	   * 不指定对哪个响应数据类型的回调
                	   * 
                	   * */
                      VoidCallback cb = (VoidCallback) p.cb;
                      cb.processResult(rc, clientPath, p.ctx);
                  }
              }
          } catch (Throwable t) {
              LOG.error("Caught unexpected throwable", t);
          }
       }
    }

    /**
     * packet请求提交的过程--其结束在这里定义
     * 将数据包提交以后、需要等待发送队列发送、读取响应、本地watcher事件注册以及packet的api回调进入事件队列才算对该packet处理完了。
     * 
     * */
    private void finishPacket(Packet p) {
    	// 本地事件注册--watcherManager管理本地登记的监听事件
        if (p.watchRegistration != null) {
            p.watchRegistration.register(p.replyHeader.getErr());
        }

        /**
         * 是否有api回调：必须知道Zookeeper类api的两种使用方式(因为第二种方式以前用的少、导致这里代码一开始没看懂)
         * -----
         * 1 返回数据的同步api：
         * 					  即不带CallBack的api、直接返回数据给用户、这种请求是同步请求、通过submitRequest提交然后wait
         * 
         * 2 不需要返回数据的异步api:
         * 					  带CallBack的异步api、这种请求用户定义了返回数据以后、根据返回数据回调里做什么---而不需要直接将数据返回用户。
         * 					  这种请求直接将packet放入待发送的请求队列、进入后续流程 
         * 
         * */
        if (p.cb == null) {
        	// 没有api回调、直接finish、通知packet上的wait
            synchronized (p) {
                p.finished = true;
                p.notifyAll();
            }
        } else {
        	// api回调入事件处理队列、这种情况下在哪里通知packet上的wait？？？？？TODO
        	// 擦 终于明白 异步CallBack api不是通过submitRequest提交的、而是直接放入请求发送队列的
        	// 
            p.finished = true;
            eventThread.queuePacket(p);
        }
    }

    /**
     * 连接丢失时处理packet
     * 设置响应的错误码
     * 
     * */
    private void conLossPacket(Packet p) {
        if (p.replyHeader == null) {
            return;
        }
        switch(zooKeeper.state) {
        case AUTH_FAILED:
            p.replyHeader.setErr(KeeperException.Code.AUTHFAILED.intValue());
            break;
        case CLOSED:
            p.replyHeader.setErr(KeeperException.Code.SESSIONEXPIRED.intValue());
            break;
        default:
            p.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
        }
        finishPacket(p);
    }

    /**
     * zxid为一64位数字，高32位为leader信息又称为epoch，每次leader转换时递增；低32位为消息编号
     * Leader转换时应该从0重新开始编号
     * 每条消息有这样一个id  详情以后再说  TODO
     * 
     * */
    volatile long lastZxid;

    private static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -5438877188796231422L;

        public EndOfStreamException(String msg) {
            super(msg);
        }
        
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    private static class SessionTimeoutException extends IOException {
        private static final long serialVersionUID = 824482094072071178L;

        public SessionTimeoutException(String msg) {
            super(msg);
        }
    }
    
    private static class SessionExpiredException extends IOException {
        private static final long serialVersionUID = -1388816932076193249L;

        public SessionExpiredException(String msg) {
            super(msg);
        }
    }
    
    /**
     * This class services the outgoing request queue and generates the heart
     * beats. It also spawns the ReadThread.
     */
    class SendThread extends Thread {
    	// 绑定与某一个socketChannel的SelectionKey对象
        SelectionKey sockKey;
        // 管理多个socketChannel
        private final Selector selector = Selector.open();
        // 包长度、4个字节
        final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(4);
        // 请求包缓冲区、大小为从lenBuffer读出的长度
        ByteBuffer incomingBuffer = lenBuffer;

        // 是否初始化--连接上了server
        boolean initialized;

        // 最后一次ping的时间(ns)
        private long lastPingSentNs;

        // 发送和接受请求数量
        long sentCount = 0;
        long recvCount = 0;

        // 读取数据包的len
        void readLength() throws IOException {
            int len = incomingBuffer.getInt();
            if (len < 0 || len >= packetLen) {
                throw new IOException("Packet len" + len + " is out of range!");
            }
            incomingBuffer = ByteBuffer.allocate(len);
        }

        /**
         * 从incomingBuffer中读连接请求的响应
         * 注意readConnectResult和readResponse都只是业务上的逻辑处理
         * 底层的io都在doIO()里 将incomingBuffer准备就绪
         * 
         * 响应的序列化格式：connectResp
         * 
         * */
        void readConnectResult() throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(
                    incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ConnectResponse conRsp = new ConnectResponse();
            conRsp.deserialize(bbia, "connect");
            // 服务端响应的连接时间<=0表示失败
            negotiatedSessionTimeout = conRsp.getTimeOut();
            if (negotiatedSessionTimeout <= 0) {
                zooKeeper.state = States.CLOSED;

                eventThread.queueEvent(new WatchedEvent(
                        Watcher.Event.EventType.None,
                        Watcher.Event.KeeperState.Expired, null));
                eventThread.queueEventOfDeath();
                throw new SessionExpiredException(
                        "Unable to reconnect to ZooKeeper service, session 0x"
                        + Long.toHexString(sessionId) + " has expired");
            }
            // 每次重连都会重置这些timeOut值
            readTimeout = negotiatedSessionTimeout * 2 / 3;
            connectTimeout = negotiatedSessionTimeout / serverAddrs.size();
            sessionId = conRsp.getSessionId();
            sessionPasswd = conRsp.getPasswd();
            // 状态置为conneted
            zooKeeper.state = States.CONNECTED;
            LOG.info("Session establishment complete on server "
                    + ((SocketChannel)sockKey.channel())
                        .socket().getRemoteSocketAddress()
                    + ", sessionid = 0x"
                    + Long.toHexString(sessionId)
                    + ", negotiated timeout = " + negotiatedSessionTimeout);
            // syncConected事件入队
            eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None,
                    Watcher.Event.KeeperState.SyncConnected, null));
        }

        /**
         * 不仅仅是读取响应：
         * 		1 读请求的响应     响应的序列化格式：header + response--有些请求是只有header的、例如ping
         * 		2 读到响应后的事情（本地watch维护+packet api回调进入EventThread事件队列）
         * 
         * */
        void readResponse() throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(
                    incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ReplyHeader replyHdr = new ReplyHeader();

            /**
             * 读取响应header 根据xid判断请求和响应的类型
             * 
             * */
            replyHdr.deserialize(bbia, "header");
            /**
             * 1 ping请求的响应 响应格式：header
             *   有响应就代表成功、直接返回
             * 
             * */
            if (replyHdr.getXid() == -2) {
                // -2 is the xid for pings
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got ping response for sessionid: 0x"
                            + Long.toHexString(sessionId)
                            + " after "
                            + ((System.nanoTime() - lastPingSentNs) / 1000000)
                            + "ms");
                }
                return;
            }
            /**
             * 2 AuthPacket请求的响应 响应格式：header
             * 	 如果验证成功、直接返回。否则验证失败事件事件处理队列
             * 
             * */
            if (replyHdr.getXid() == -4) {
            	 // -4 is the xid for AuthPacket               
                if(replyHdr.getErr() == KeeperException.Code.AUTHFAILED.intValue()) {
                    zooKeeper.state = States.AUTH_FAILED;                    
                    eventThread.queueEvent( new WatchedEvent(Watcher.Event.EventType.None, 
                            Watcher.Event.KeeperState.AuthFailed, null) );            		            		
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got auth sessionid:0x"
                            + Long.toHexString(sessionId));
                }
                return;
            }
            /**
             * 3 server发来的通知、这个不算数一个响应。
             *   格式：header + response(WatcherEvent)
             *   
             *   直接入事件处理线程队列即可
             *   
             * */
            if (replyHdr.getXid() == -1) {
                // -1 means notification
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got notification sessionid:0x"
                        + Long.toHexString(sessionId));
                }
                WatcherEvent event = new WatcherEvent();
                event.deserialize(bbia, "response");

                // convert from a server path to a client path
                if (chrootPath != null) {
                    String serverPath = event.getPath();
                    if(serverPath.compareTo(chrootPath)==0)
                        event.setPath("/");
                    else
                        event.setPath(serverPath.substring(chrootPath.length()));
                }

                WatchedEvent we = new WatchedEvent(event);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got " + we + " for sessionid 0x"
                            + Long.toHexString(sessionId));
                }
                // 通知事件入事件处理线程队列
                eventThread.queueEvent( we );
                return;
            }
            /**
             * 4 其他绝大多数一般的情况下的各种api请求的响应：
             *   将响应填充到packet(即放在请求队列里的请求包)后，将packet扔给EventThread即可
             *   响应为header + response
             *   
             * */
            /**
             * 等待响应的packet队列不应为空、因为此时确实收到了一个响应
             * 
             * */
            if (pendingQueue.size() == 0) {
                throw new IOException("Nothing in the queue, but got "
                        + replyHdr.getXid());
            }
            /**
             * 读取响应、完善等待响应的packet--将响应内容填充进去
             * 
             * */
            Packet packet = null;
            synchronized (pendingQueue) {
                packet = pendingQueue.remove();
            }
            /*
             * Since requests are processed in order, we better get a response
             * to the first request!
             * 
             * 请求是顺序处理的、递增的xid标识了一个请求和响应的一一对应
             * 
             */
            try {
            	// xid标识请求和响应一一对应
                if (packet.header.getXid() != replyHdr.getXid()) {
                    packet.replyHeader.setErr(
                            KeeperException.Code.CONNECTIONLOSS.intValue());
                    throw new IOException("Xid out of order. Got "
                            + replyHdr.getXid() + " expected "
                            + packet.header.getXid());
                }

                // 填充packet的replyHeader
                packet.replyHeader.setXid(replyHdr.getXid());
                packet.replyHeader.setErr(replyHdr.getErr());
                packet.replyHeader.setZxid(replyHdr.getZxid());
                // 服务端返回的最新的事务id
                if (replyHdr.getZxid() > 0) {
                    lastZxid = replyHdr.getZxid();
                }
                // 填充packet的response
                if (packet.response != null && replyHdr.getErr() == 0) {
                    packet.response.deserialize(bbia, "response");
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reading reply sessionid:0x"
                            + Long.toHexString(sessionId) + ", packet:: " + packet);
                }
            } finally {
            	// 最后对packet做finish处理：本地watcher注册维护+packet进入事件处理队列
                finishPacket(packet);
            }
        }

        /**
         * @return true if a packet was received
         * @throws InterruptedException
         * @throws IOException
         * 
         * 1 doIO在sendThread线程死循环里。做的事情:
         * 	  1) socket可读时：去读请求--参见readResponse()----有三种情况、参见注释
         *    2) socket可写时：从待发送队列outgoingQueue取请求发送、这个比较简单
         *    
         * 2 以上各种情况、只有当可读且读到了响应packet时、才返回true
         * 
         */
        boolean doIO() throws InterruptedException, IOException {
            boolean packetReceived = false;
            SocketChannel sock = (SocketChannel) sockKey.channel();
            if (sock == null) {
                throw new IOException("Socket is null!");
            }
            /**
             * 1 socket可读
             * 
             * */
            if (sockKey.isReadable()) {
                int rc = sock.read(incomingBuffer);
                if (rc < 0) {
                    throw new EndOfStreamException(
                            "Unable to read additional data from server sessionid 0x"
                            + Long.toHexString(sessionId)
                            + ", likely server has closed socket");
                }
                if (!incomingBuffer.hasRemaining()) {
                    incomingBuffer.flip();
                    if (incomingBuffer == lenBuffer) {
                        /**
                         * 先读length、这也是常见的一种处理方式
                         * */
                        recvCount++;
                        readLength();
                    } else if (!initialized) {
                    	/**
                    	 * 如果还没有连上server、先去连server
                    	 * */
                        readConnectResult();
                        enableRead();
                        if (!outgoingQueue.isEmpty()) {
                            enableWrite();
                        }
                        lenBuffer.clear();
                        incomingBuffer = lenBuffer;
                        packetReceived = true;
                        initialized = true;
                    } else {
                    	/**
                    	 * 读响应：注意真正的很多逻辑都在readResponse里面(读到响应后做的事情)
                    	 * 
                    	 * */
                        readResponse();
                        lenBuffer.clear();
                        incomingBuffer = lenBuffer;
                        packetReceived = true;
                    }
                }
            }
            /**
             * 2 socket可写
             *   从待发送队列取packet、写socket、将packet放进待响应的队列
             * 
             * */
            if (sockKey.isWritable()) {
                synchronized (outgoingQueue) {
                    if (!outgoingQueue.isEmpty()) {
                        ByteBuffer pbb = outgoingQueue.getFirst().bb;
                        sock.write(pbb);
                        if (!pbb.hasRemaining()) {
                            sentCount++;
                            Packet p = outgoingQueue.removeFirst();
                            if (p.header != null
                                    && p.header.getType() != OpCode.ping
                                    && p.header.getType() != OpCode.auth) {
                                pendingQueue.add(p);
                            }
                        }
                    }
                }
            }
            /**
             * 如果等待发送到packet队列为空、也就意味着现在不需要writable
             * 
             * */
            if (outgoingQueue.isEmpty()) {
                disableWrite();
            } else {
                enableWrite();
            }
            return packetReceived;
        }

        synchronized private void enableWrite() {
            int i = sockKey.interestOps();
            if ((i & SelectionKey.OP_WRITE) == 0) {
                sockKey.interestOps(i | SelectionKey.OP_WRITE);
            }
        }

        synchronized private void disableWrite() {
            int i = sockKey.interestOps();
            if ((i & SelectionKey.OP_WRITE) != 0) {
                sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
            }
        }

        synchronized private void enableRead() {
            int i = sockKey.interestOps();
            if ((i & SelectionKey.OP_READ) == 0) {
                sockKey.interestOps(i | SelectionKey.OP_READ);
            }
        }

        synchronized private void disableRead() {
            int i = sockKey.interestOps();
            if ((i & SelectionKey.OP_READ) != 0) {
                sockKey.interestOps(i & (~SelectionKey.OP_READ));
            }
        }

        /**
         * 后台线程、设置开始connecting
         * 
         * */
        SendThread() throws IOException {
            super(makeThreadName("-SendThread()"));
            zooKeeper.state = States.CONNECTING;
            setUncaughtExceptionHandler(uncaughtExceptionHandler);
            setDaemon(true);
        }

        /**
         * 真正的对server建立连接是在这个函数里：
         * @k  已经选择好了点对应的server的SelectionKey
         * 
         * 1 watch重建
         * 2 验证请求
         * 3 连接请求
         * 
         * 注意这里的连接都只是将相关请求放入发送队列、还并没有得到server端的响应(甚至请求还没有发出去)。
         * 
         * */
        private void primeConnection(SelectionKey k) throws IOException {
            LOG.info("Socket connection established to "
                    + ((SocketChannel)sockKey.channel())
                        .socket().getRemoteSocketAddress()
                    + ", initiating session");
            // 连接下标维护
            lastConnectIndex = currentConnectIndex;
            // 连接请求构造
            ConnectRequest conReq = new ConnectRequest(0, lastZxid,
                    sessionTimeout, sessionId, sessionPasswd);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            boa.writeInt(-1, "len");
            conReq.serialize(boa, "connect");
            baos.close();
            ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
            bb.putInt(bb.capacity() - 4);
            bb.rewind();
            // 1 watch重建、此时outgoingQueue不应该再有新请求进入
            synchronized (outgoingQueue) {
                // We add backwards since we are pushing into the front
                // Only send if there's a pending watch
                if (!disableAutoWatchReset) {
                    List<String> dataWatches = zooKeeper.getDataWatches();
                    List<String> existWatches = zooKeeper.getExistWatches();
                    List<String> childWatches = zooKeeper.getChildWatches();
                    if (!dataWatches.isEmpty()
                                || !existWatches.isEmpty() || !childWatches.isEmpty()) {
                        SetWatches sw = new SetWatches(lastZxid,
                                prependChroot(dataWatches),
                                prependChroot(existWatches),
                                prependChroot(childWatches));
                        RequestHeader h = new RequestHeader();
                        h.setType(ZooDefs.OpCode.setWatches);
                        h.setXid(-8);
                        Packet packet = new Packet(h, new ReplyHeader(), sw, null, null,
                                null);
                        outgoingQueue.addFirst(packet);
                    }
                }
                // 2 验证信息放入发送队列
                for (AuthData id : authInfo) {
                    outgoingQueue.addFirst(new Packet(new RequestHeader(-4,
                            OpCode.auth), null, new AuthPacket(0, id.scheme,
                            id.data), null, null, null));
                }
                // 3 连接请求放入发送对了
                outgoingQueue.addFirst((new Packet(null, null, null, null, bb,
                        null)));
            }
            // 开启读写io的epoll关注
            synchronized (this) {
                k.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Session establishment request sent on "
                        + ((SocketChannel)sockKey.channel())
                            .socket().getRemoteSocketAddress());
            }
        }

        /**
         * 真正的root重新调整
         * 
         * */
        private List<String> prependChroot(List<String> paths) {
            if (chrootPath != null && !paths.isEmpty()) {
                for (int i = 0; i < paths.size(); ++i) {
                    String clientPath = paths.get(i);
                    String serverPath;
                    // handle clientPath = "/"
                    if (clientPath.length() == 1) {
                        serverPath = chrootPath;
                    } else {
                        serverPath = chrootPath + clientPath;
                    }
                    paths.set(i, serverPath);
                }
            }
            return paths;
        }

        /**
         * ping请求
         * 
         * */
        private void sendPing() {
            lastPingSentNs = System.nanoTime();
            RequestHeader h = new RequestHeader(-2, OpCode.ping);
            queuePacket(h, null, null, null, null, null, null, null, null);
        }

        // 维护连接server list的策略
        int lastConnectIndex = -1;

        int currentConnectIndex;

        Random r = new Random(System.nanoTime());

        private void startConnect() throws IOException {
            if (lastConnectIndex == -1) {
                // We don't want to delay the first try at a connect, so we
                // start with -1 the first time around
                lastConnectIndex = 0;
            } else {
                try {
                    Thread.sleep(r.nextInt(1000));
                } catch (InterruptedException e1) {
                    LOG.warn("Unexpected exception", e1);
                }
                if (nextAddrToTry == lastConnectIndex) {
                    try {
                        // Try not to spin too fast!
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
            }
            zooKeeper.state = States.CONNECTING;
            currentConnectIndex = nextAddrToTry;
            InetSocketAddress addr = serverAddrs.get(nextAddrToTry);
            // 环形重试
            nextAddrToTry++;
            if (nextAddrToTry == serverAddrs.size()) {
                nextAddrToTry = 0;
            }
            LOG.info("Opening socket connection to server " + addr);
            SocketChannel sock;
            sock = SocketChannel.open();
            sock.configureBlocking(false);
            sock.socket().setSoLinger(false, -1);
            sock.socket().setTcpNoDelay(true);
            setName(getName().replaceAll("\\(.*\\)",
                    "(" + addr.getHostName() + ":" + addr.getPort() + ")"));
            try {
            	/**
            	 * Note：
            	 * 这里已经建立起了socket连接、但是从用户看到的逻辑层面角度来说、还没有连接成功。
            	 * 只有当primeConnection中做了watch重建、验证信息、发送并接受到了server端的请求才算是用户看到的建立了连接。
            	 * 
            	 * */
                sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
                boolean immediateConnect = sock.connect(addr);
                if (immediateConnect) {
                	// 实际的连接操作在这
                    primeConnection(sockKey);
                }
            } catch (IOException e) {
                LOG.error("Unable to open socket to " + addr);
                sock.close();
                throw e;
            }
            /**
             * 注意为什么这里是false、
             *  primeConnection(sockKey)只是做了将连接需要的请求放到发送队列、还并没有连接成功、甚至还没有发送出去
             *  只有当在diIO里面发现尚未连接、去readConnectResult()、读到了服务端的响应才会将initialized置为true
             * 
             * */
            initialized = false;

            /*
             * Reset incomingBuffer
             */
            lenBuffer.clear();
            incomingBuffer = lenBuffer;
        }

        private static final String RETRY_CONN_MSG =
            ", closing socket connection and attempting reconnect";
        
        
        /**
         * SendThread的死循环在这
         * 
         * */
        @Override
        public void run() {
            long now = System.currentTimeMillis();
            long lastHeard = now;
            long lastSend = now;
            while (zooKeeper.state.isAlive()) {
            	/**
            	 * 所有代码被catch、即使出现异常、也继续执行下一次while
            	 * 
            	 * **/
                try {
                	/**
                	 * 1 初始条件检测
                	 * 
                	 * */
                    if (sockKey == null) {
                        // don't re-establish connection if we are closing
                        if (closing) {
                            break;
                        }
                        startConnect();
                        lastSend = now;
                        lastHeard = now;
                    }
                    
                    /**
                     * 2 SessionTimeout检测
                     * 注意这个是非常非常重要的、zk客户端保证请求和响应的一一对应和顺序、
                     * 那么超时就非常重要、否则如果前面的某个请求一直没有响应就会阻塞在那。
                     * 
                     * 所以每次进入while都要首先检测是否超时、这种超时的时间是由作者定义的！！！！！
                     * 
                     * TODO 这几个时间的含义 以及为什么这样设计超时时间
                     * 
                     * */
                    int idleRecv = (int) (now - lastHeard);
                    int idleSend = (int) (now - lastSend);
                    int to = readTimeout - idleRecv;
                    if (zooKeeper.state != States.CONNECTED) {
                        to = connectTimeout - idleRecv;
                    }
                    if (to <= 0) {
                        throw new SessionTimeoutException(
                                "Client session timed out, have not heard from server in "
                                + idleRecv + "ms"
                                + " for sessionid 0x"
                                + Long.toHexString(sessionId));
                    }
                    
                    /**
                     * 3 发送ping
                     * 这个也很重要、当上一次发送请求的时间间隔 > readTimeout/2时---例如极端情况：可能很久没有正常的api调用
                     * 那么就发送一个ping。这种策略保证如果一直都有很紧凑的正常请求、就不需要发送ping
                     * 
                     * */
                    if (zooKeeper.state == States.CONNECTED) {
                        int timeToNextPing = readTimeout/2 - idleSend;
                        if (timeToNextPing <= 0) {
                            sendPing();
                            lastSend = now;
                            enableWrite();
                        } else {
                            if (timeToNextPing < to) {
                                to = timeToNextPing;
                            }
                        }
                    }

                    /**
                     * 4 正常的IO读写开始
                     * 虽然这里是有一个for循环、但其实只有一个socketKey绑定到了该selector上
                     * 
                     * */
                    // 阻塞式的设定超时时间select
                    selector.select(to);
                    Set<SelectionKey> selected;
                    synchronized (this) {
                        selected = selector.selectedKeys();
                    }
                    // Everything below and until we get back to the select is
                    // non blocking, so time is effectively a constant. That is
                    // Why we just have to do this once, here
                    now = System.currentTimeMillis();
                    for (SelectionKey k : selected) {
                        SocketChannel sc = ((SocketChannel) k.channel());
                        if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                        	// 1 建立连接
                            if (sc.finishConnect()) {
                                lastHeard = now;
                                lastSend = now;
                                primeConnection(k);
                            }
                        } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                        	// 2 正常读写
                            if (outgoingQueue.size() > 0) {
                                // We have something to send so it's the same
                                // as if we do the send now.
                                lastSend = now;
                            }
                            if (doIO()) {
                                lastHeard = now;
                            }
                        }
                    }
                    /**
                     * socket状态维护
                     * 
                     * */
                    if (zooKeeper.state == States.CONNECTED) {
                        if (outgoingQueue.size() > 0) {
                            enableWrite();
                        } else {
                            disableWrite();
                        }
                    }
                    selected.clear();
                } catch (Throwable e) {
                	/**
                	 * 在这处理异常、这个是非常非常重要的、注意客户端的请求投递保证：
                	 * 请求按顺序发送、响应按顺序接受(tcp/ip保证顺序)、由outgoingQueue和pendingQueue两个队列来保证这种顺序
                	 * 如果发现不一致、说明出错了或者超时---所以这里超时的维护也非常非常重要、zk是这样实现超时的--每次执行while都要判断超时时间
                	 * 那么就进入异常处理、断开连接、清空之前的待发送队列和待响应队列、重新开始。
                	 * 
                	 * */
                    if (closing) {
                        if (LOG.isDebugEnabled()) {
                            // closing so this is expected
                            LOG.debug("An exception was thrown while closing send thread for session 0x"
                                    + Long.toHexString(getSessionId())
                                    + " : " + e.getMessage());
                        }
                        break;
                    } else {
                        // this is ugly, you have a better way speak up
                        if (e instanceof SessionExpiredException) {
                            LOG.info(e.getMessage() + ", closing socket connection");
                        } else if (e instanceof SessionTimeoutException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof EndOfStreamException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else {
                            LOG.warn("Session 0x"
                                    + Long.toHexString(getSessionId())
                                    + " for server "
                                    + ((SocketChannel)sockKey.channel())
                                        .socket().getRemoteSocketAddress()
                                    + ", unexpected error"
                                    + RETRY_CONN_MSG,
                                    e);
                        }
                        cleanup();
                        if (zooKeeper.state.isAlive()) {
                            eventThread.queueEvent(new WatchedEvent(
                                    Event.EventType.None,
                                    Event.KeeperState.Disconnected,
                                    null));
                        }

                        now = System.currentTimeMillis();
                        lastHeard = now;
                        lastSend = now;
                    }
                }
            }
            
            /**
             * 如果收到了关闭客户端的命令、退出while循环
             * 关闭socket和epoll退出
             * 
             * */
            cleanup();
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("Ignoring exception during selector close", e);
            }
            if (zooKeeper.state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(
                        Event.EventType.None,
                        Event.KeeperState.Disconnected,
                        null));
            }
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                                     "SendThread exitedloop.");
        }

        /**
         * 1 关闭当前与server连接的socket
         * 2 清空待发送队列和待响应队列
         * 注意这个是很重要的、请求和响应是顺序的、一一对应的、发送出去的请求必须要有一个响应回来、再考虑下一个请求的响应
         * 如果没有或者响应的不是这个请求、就说明超时或者出错了、那么进入异常处理过程、直接cleanup然后重新连接
         * 
         * */
        private void cleanup() {
            if (sockKey != null) {
                SocketChannel sock = (SocketChannel) sockKey.channel();
                sockKey.cancel();
                try {
                    sock.socket().shutdownInput();
                } catch (IOException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring exception during shutdown input", e);
                    }
                }
                try {
                    sock.socket().shutdownOutput();
                } catch (IOException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring exception during shutdown output", e);
                    }
                }
                try {
                    sock.socket().close();
                } catch (IOException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring exception during socket close", e);
                    }
                }
                try {
                    sock.close();
                } catch (IOException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring exception during channel close", e);
                    }
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SendThread interrupted during sleep, ignoring");
                }
            }
            sockKey = null;
            synchronized (pendingQueue) {
                for (Packet p : pendingQueue) {
                    conLossPacket(p);
                }
                pendingQueue.clear();
            }
            synchronized (outgoingQueue) {
                for (Packet p : outgoingQueue) {
                    conLossPacket(p);
                }
                outgoingQueue.clear();
            }
        }

        public void close() {
            synchronized (this) {
               zooKeeper.state = States.CLOSED;
               selector.wakeup();
            }
        }

        synchronized private void wakeup() {
            selector.wakeup();
        }
    }

    /**
     * Shutdown the send/event threads. This method should not be called
     * directly - rather it should be called as part of close operation. This
     * method is primarily here to allow the tests to verify disconnection
     * behavior.
     * 
     * 不直接调用：关闭sendThread和eventThread
     * 
     */
    public void disconnect() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Disconnecting client for session: 0x"
                      + Long.toHexString(getSessionId()));
        }

        sendThread.close();
        eventThread.queueEventOfDeath();
    }

    /**
     * Close the connection, which includes; send session disconnect to the
     * server, shutdown the send/event threads.
     *
     * @throws IOException
     * 
     * 暴漏给Zookeeper直接使用、发送给server关闭命令、再调研disconnect()
     * 
     */
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing client for session: 0x"
                      + Long.toHexString(getSessionId()));
        }

        try {
            RequestHeader h = new RequestHeader();
            h.setType(ZooDefs.OpCode.closeSession);

            submitRequest(h, null, null, null);
        } catch (InterruptedException e) {
            // ignore, close the send/event threads
        } finally {
            disconnect();
        }
    }

    /**
     * 此次请求id的唯一标示 原子自增
     * 在请求header里 参考RequestHeader
     * 
     * */
    private int xid = 1;

    synchronized private int getXid() {
        return xid++;
    }

    /**
     * 组装和提交request、调用queuePacket()放进发送队列、此时不会退出submitRequest。
     * 等待packet完成、即finishPacket--submitRequest才会退出。
     * 
     * 所以这里是一个异步等待的方式、理解这一流程:
     * 1 packet请求提交到发送队列、等待packet finish
     * 2 SendThread线程：发送请求、读取响应-->
     * 3 读到响应后、finishPacket(本地watch维护+packet进入api事件回调处理队列+唤醒packet上的wait)
     * 
     * 为返回数据的同步请求而设计、Zookeeper里面的同步api、要求返回数据、使用该方法提交请求、
     * 等待直到返回数据。
     * ----
     * 而下面的方法queuePacket为异步的CallBack而设计、这种在Zookeeper的api里不需要给用户返回数据、而是直接放入发送队列
     * 后续拿到结果以后执行响应的回调即可。
     * -------
     * fuck 终于理解了finishPacket里面的为什么else里面没有notify了--参见finishPacket
     * 
     * */
    public ReplyHeader submitRequest(RequestHeader h, Record request,
            Record response, WatchRegistration watchRegistration)
            throws InterruptedException {
        ReplyHeader r = new ReplyHeader();
        Packet packet = queuePacket(h, r, request, response, null, null, null,
                    null, watchRegistration);
        synchronized (packet) {
            while (!packet.finished) {
                packet.wait();
            }
        }
        return r;
    }

    /**
     * 请求组装+放入待发送队列
     * 该方法公开给Zookeeper类调用、该方法的参数都是用来组装Packet的
     * 
     * @return 组装后的packet、后续处理完后还要从该packet中去response
     * 
     * */
    Packet queuePacket(RequestHeader h, ReplyHeader r, Record request,
            Record response, AsyncCallback cb, String clientPath,
            String serverPath, Object ctx, WatchRegistration watchRegistration)
    {
        Packet packet = null;
        synchronized (outgoingQueue) {
            if (h.getType() != OpCode.ping && h.getType() != OpCode.auth) {
                h.setXid(getXid());
            }
            packet = new Packet(h, r, request, response, null,
                    watchRegistration);
            packet.cb = cb;
            packet.ctx = ctx;
            packet.clientPath = clientPath;
            packet.serverPath = serverPath;
            if (!zooKeeper.state.isAlive() || closing) {
                conLossPacket(packet);
            } else {
                // If the client is asking to close the session then
                // mark as closing
                if (h.getType() == OpCode.closeSession) {
                    closing = true;
                }
                outgoingQueue.add(packet);
            }
        }

        sendThread.wakeup();
        return packet;
    }

    /**
     * 验证信息：添加到authInfo + 发给服务端
     * xid=-4
     * 
     * */
    public void addAuthInfo(String scheme, byte auth[]) {
        if (!zooKeeper.state.isAlive()) {
            return;
        }
        authInfo.add(new AuthData(scheme, auth));
        queuePacket(new RequestHeader(-4, OpCode.auth), null,
                new AuthPacket(0, scheme, auth), null, null, null, null,
                null, null);
    }
}
