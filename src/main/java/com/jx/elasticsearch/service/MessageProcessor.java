package com.jx.elasticsearch.service;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created by hpb on 2018-03-21.
 */
public interface MessageProcessor {
    /**
     * 处理消息的接口
     *
     * @param messageExt
     * @return
     */
    public boolean handleMessage(MessageExt messageExt);
}
