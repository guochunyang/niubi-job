/*
 * Copyright 2002-2015 the original author or authors.
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
import com.zuoxiaolong.niubi.job.api.StandbyPathApi;
import com.zuoxiaolong.niubi.job.core.exception.NiubiException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * AbstractCurdApiImpl 主要是封装了zookeeper客户端curator的一些常见操作
 * 例如创建结点、删除结点、获取子节点数据等，目的是是操作更加简化
 * @author Xiaolong Zuo
 * @since 0.9.3
 */
public abstract class AbstractCurdApiImpl {

    private static final Stat EMPTY_STAT = new Stat();

    private CuratorFramework client;

    private StandbyPathApi standbyPathApi = StandbyPathApiImpl.INSTANCE;

    private MasterSlavePathApi masterSlavePathApi = MasterSlavePathApiImpl.INSTANCE;

    public AbstractCurdApiImpl(CuratorFramework client) {
        this.client = client;
    }

    protected CuratorFramework getClient() {
        return client;
    }

    protected StandbyPathApi getStandbyPathApi() {
        return standbyPathApi;
    }

    protected MasterSlavePathApi getMasterSlavePathApi() {
        return masterSlavePathApi;
    }

    /**
     * 返回path 的子节点信息 转化成 ChildData 格式
     * @param path
     * @return
     */
    protected List<ChildData> getChildren(String path) {
        try {
            List<ChildData> childDataList = new ArrayList<>();
            List<String> children = client.getChildren().forPath(path);
            childDataList.addAll(children.stream().map(child -> getData(path + "/" + child)).collect(Collectors.toList()));
            return childDataList;
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    /**
     * 返回path的信息，转化为ChildData格式
     * @param path
     * @return
     */
    protected ChildData getData(String path) {
        try {
            return new ChildData(path, EMPTY_STAT, client.getData().forPath(path));
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected boolean checkExists(String path) {
        try {
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected String create(String path, byte[] data) {
        try {
            return getClient().create().creatingParentsIfNeeded().forPath(path, data);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected String createWithProtection(String path, byte[] data) {
        try {
            return getClient().create().creatingParentsIfNeeded().withProtection().forPath(path, data);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected String createPersistentWithProtection(String path, byte[] data) {
        try {
            return getClient().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected String createPersistentSequentialWithProtection(String path, byte[] data) {
        try {
            return getClient().create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, data);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected String createPersistentSequential(String path, byte[] data) {
        try {
            return getClient().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, data);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected String createEphemeralSequential(String path, byte[] data) {
        try {
            return getClient().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, data);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected void delete(String path) {
        try {
            getClient().delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

    protected Stat setData(String path, byte[] data) {
        try {
            return getClient().setData().forPath(path, data);
        } catch (Exception e) {
            throw new NiubiException(e);
        }
    }

}
