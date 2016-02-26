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

import com.zuoxiaolong.niubi.job.api.MasterSlaveJobApi;
import com.zuoxiaolong.niubi.job.api.data.MasterSlaveJobData;
import com.zuoxiaolong.niubi.job.api.helper.PathHelper;
import com.zuoxiaolong.niubi.job.core.helper.JsonHelper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Xiaolong Zuo
 * @since 0.9.3
 */
public class MasterSlaveJobApiImpl extends AbstractCurdApiImpl implements MasterSlaveJobApi {

    public MasterSlaveJobApiImpl(CuratorFramework client) {
        super(client);
    }

    /**
     * 获取所有的job 转化为MasterSlaveJobData格式
     * @return
     */
    @Override
    public List<MasterSlaveJobData> getAllJobs() {
        List<ChildData> childDataList = getChildren(getMasterSlavePathApi().getJobPath());
        List<MasterSlaveJobData> masterSlaveJobDataList = childDataList.stream().map(MasterSlaveJobData::new).collect(Collectors.toList());
        return masterSlaveJobDataList;
    }

    /**
     * 将该job保存到zookeeper的相应结点中
     * @param group
     * @param name
     * @param data job的bytes数据
     */
    @Override
    public void saveJob(String group, String name, MasterSlaveJobData.Data data) {
        data.prepareOperation();
        // 拼接path，组装成MasterSlaveJobData
        MasterSlaveJobData masterSlaveJobData = new MasterSlaveJobData(PathHelper.getJobPath(getMasterSlavePathApi().getJobPath(), group, name), data);
        // 提升一个版本号
        masterSlaveJobData.getData().incrementVersion();
        // 如果结点存在就设置data，不存在则创建结点
        if (checkExists(masterSlaveJobData.getPath())) {
            setData(masterSlaveJobData.getPath(), masterSlaveJobData.getDataBytes());
        } else {
            create(masterSlaveJobData.getPath(), JsonHelper.toBytes(masterSlaveJobData.getData()));
        }
    }

    @Override
    public void updateJob(String group, String name, MasterSlaveJobData.Data data) {
        MasterSlaveJobData masterSlaveJobData = new MasterSlaveJobData(PathHelper.getJobPath(getMasterSlavePathApi().getJobPath(), group, name), data);
        masterSlaveJobData.getData().incrementVersion();
        setData(masterSlaveJobData.getPath(), masterSlaveJobData.getDataBytes());
    }

    /**
     * 根据group和name 拼接path，然后获取job
     * @param group
     * @param name
     * @return
     */
    @Override
    public MasterSlaveJobData getJob(String group, String name) {
        return getJob(PathHelper.getJobPath(getMasterSlavePathApi().getJobPath(), group, name));
    }

    @Override
    public MasterSlaveJobData getJob(String path) {
        if (!checkExists(path)) {
            return null;
        }
        return new MasterSlaveJobData(getData(path));
    }

}
