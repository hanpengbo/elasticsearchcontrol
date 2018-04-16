package com.jx.elasticsearch.config;


import com.jx.elasticsearch.exception.RocketMQException;
import com.jx.elasticsearch.service.MessageProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Created by hpb on 2018-03-21.
 */
@SpringBootConfiguration
public class RocketMQConsumerConfiguration {

    public static final Logger LOGGER = LoggerFactory.getLogger(RocketMQConsumerConfiguration.class);

    @Value("${rocketmq.consumer.namesrvAddr}")
    private String namesrvAddr;

    @Value("${rocketmq.consumer.groupName}")
    private String groupName;

    @Autowired
    @Qualifier("messageProcessorImpl")
    private MessageProcessor messageProcessor;

    @Bean
    public DefaultMQPushConsumer getRocketMQConsumer() throws RocketMQException {
        if (StringUtils.isBlank(groupName)) {
            throw new RocketMQException("groupName is null !!!");
        }
        if (StringUtils.isBlank(namesrvAddr)) {
            throw new RocketMQException("namesrvAddr is null !!!");
        }
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(namesrvAddr);
        MessageListener messageListener = new MessageListener();
        messageListener.setMessageProcessor(messageProcessor);
        consumer.registerMessageListener(messageListener);
        try {
            consumer.subscribe("serviceOrder", "*");
//            consumer.subscribe("order", "*");
//            consumer.subscribe("product", "*");
            consumer.start();
            LOGGER.info("consumer is start !!! groupName:{},namesrvAddr:{}", groupName, namesrvAddr);
        } catch (MQClientException e) {
            LOGGER.error("consumer is start !!! groupName:{},namesrvAddr:{}", groupName, namesrvAddr, e);
            throw new RocketMQException(e);
        }
        return consumer;
    }
}
