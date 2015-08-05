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

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
/**
 * 异步回调的接口
 * 参数及含义后续补充
 * 
 * 这些回调定义了客户端在操作完以后、对服务端返回的结果执行怎样的回调。
 * 这个和watch的事件处理是两码事 
 * 
 * 这种方法平时我们使用zk时使用的还是蛮少的、需要注意一下。
 * 这个接口由用户实现以决定如何执行StatCallback
 * 
 * @rc   客户端api执行的结果  用户可以根据该操作执行的成功或失败以决定做什么
 * @path api指定的path
 * @ctx  注意 这个是用户自己定义的上下文、回调函数可能依赖于用户自己定义的一些东西
 * 
 * 其他就容易理解了：下面都是api执行返回的结果
 * @stat
 * @data
 * @children
 * ....
 * 
 * 
 * */
public interface AsyncCallback {
	// 一些api执行完后、得到了stat数据，定义拿到响应后对stat的回调
    interface StatCallback extends AsyncCallback {
        public void processResult(int rc, String path, Object ctx, Stat stat);
    }

    // 对data和stat的回调
    interface DataCallback extends AsyncCallback {
        public void processResult(int rc, String path, Object ctx, byte data[],
                Stat stat);
    }

    // acl + stat
    interface ACLCallback extends AsyncCallback {
        public void processResult(int rc, String path, Object ctx,
                List<ACL> acl, Stat stat);
    }

    // children
    interface ChildrenCallback extends AsyncCallback {
        public void processResult(int rc, String path, Object ctx,
                List<String> children);
    }

    // children + stat
    interface Children2Callback extends AsyncCallback {
        public void processResult(int rc, String path, Object ctx,
                List<String> children, Stat stat);
    }

    // 创建节点时、获得创建成功的路径名name
    interface StringCallback extends AsyncCallback {
        public void processResult(int rc, String path, Object ctx, String name);
    }

    // 不指定对哪个响应结果进行回调、可以用来做一般化的回调、当所有操作执行完后都执行该回调
    interface VoidCallback extends AsyncCallback {
        public void processResult(int rc, String path, Object ctx);
    }
}
