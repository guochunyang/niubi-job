/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zuoxiaolong.niubi.job.api.curator;

import com.zuoxiaolong.niubi.job.api.MasterSlavePathApi;

/**
 * 这个class主要是规定zookeeper中job和node的路径
 * 所有的job都挂在 /jobs/xxx下面 而且创建的都是有序的持久化结点
 * 所有的node挂在 /nodes/child下面 创建的是临时的有序结点
 * @author Xiaolong Zuo
 * @since 0.9.3
 */
public class MasterSlavePathApiImpl implements MasterSlavePathApi {

    public static final MasterSlavePathApi INSTANCE = new MasterSlavePathApiImpl();

    private static final String ROOT_PATH = "/job-root";

    private static final String MASTER_SLAVE_NODE_PATH = ROOT_PATH + "/master-slave-node";

    private MasterSlavePathApiImpl() {}

    @Override
    public String getSelectorPath() {
        return MASTER_SLAVE_NODE_PATH + "/selector";
    }

    @Override
    public String getInitLockPath() {
        return MASTER_SLAVE_NODE_PATH + "/initLock";
    }

    @Override
    public String getNodePath() {
        return MASTER_SLAVE_NODE_PATH + "/nodes/child";
    }

    @Override
    public String getJobPath() {
        return MASTER_SLAVE_NODE_PATH + "/jobs";
    }

}
