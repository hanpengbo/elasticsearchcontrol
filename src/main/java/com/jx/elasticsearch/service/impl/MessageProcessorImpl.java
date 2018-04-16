package com.jx.elasticsearch.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.jx.elasticsearch.service.ElasticsearchService;
import com.jx.elasticsearch.service.MessageProcessor;
import com.jx.elasticsearch.utils.elasticsearch.ElasticsearchUtils;
import com.jx.elasticsearch.utils.http.HttpHelper;
import com.jx.elasticsearch.vo.ElasticSearchVo;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hpb on 2018-03-21.
 */
@Component
public class MessageProcessorImpl implements MessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MessageProcessorImpl.class);

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Value("${rocketmq.consumer.serviceordertopic}")
    private String serviceOrderTopic;

    @Value("${rocketmq.consumer.serviceorder.addtags}")
    private String addTags;

    @Value("${rocketmq.consumer.serviceorder.updatetags}")
    private String updateTags;

    @Value("${elasticsearch.serviceorder.index}")
    private String serviceOrderIndex;

    @Value("${elasticsearch.serviceorder.type}")
    private String serviceOrderType;

    @Value("${serviceorder.modal.simpleurl}")
    private String simpleUrl;

    @Value("${serviceorder.model.userurl}")
    private String userUrl;

    /**
     * 监听到MQ消息进行业务处理
     * @param messageExt
     * @return
     */
    public boolean handleMessage(MessageExt messageExt) {
        if (messageExt.getTopic().equals(serviceOrderTopic)) {
            System.out.println("tags:"+messageExt.getTags());
            if(messageExt.getTags().equals("add")||messageExt.getTags().equals("update")){
                String serviceOrderId = new String(messageExt.getBody());
                JSONObject jsonObject = getDataFromModal(serviceOrderId);
                if (jsonObject != null) {
                    // ES更新数据
                    try {
                        elasticsearchService.updateServiceOrder(serviceOrderId);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        e.printStackTrace();
                        return false;
                    }
                }
            }else if(messageExt.getTags().equals("addUser")){
                String userid = new String(messageExt.getBody());
                List serviceOrderids = getServiceOrderidsByUseridFromModel(userid);
                if(serviceOrderids!=null && serviceOrderids.size()>0){
                    try {
                        elasticsearchService.batchUpdateServiceOrderByQuerySize(serviceOrderids);
                    } catch (Exception e) {
                        logger.error(e.getMessage(),e);
                    }
                    logger.info("es人员服务单索引更新完毕=============");
                }
            }else if(messageExt.getTags().equals("deleteUser")){
                String userid = new String(messageExt.getBody());
                Map<String,String> map = new HashMap<>();
                map.put("user.userId",userid);
                List<Map<String, Object>> list = ElasticsearchUtils.searchExact(serviceOrderIndex,serviceOrderType,map);
                if(list!=null){
                    List<String> serviceOrderIds = new ArrayList<>();
                    for (Map<String, Object> objectMap : list) {
                        String serviceOrderId = objectMap.get("serviceOrderId").toString();
                        serviceOrderIds.add(serviceOrderId);
                    }
                    try {
                        elasticsearchService.batchUpdateServiceOrderByQuerySize(serviceOrderIds);
                    } catch (Exception e) {
                        logger.error(e.getMessage(),e);
                    }

                    logger.info("es人员服务单索引删除完毕=============");
                }
            }
        }

        /*if(messageExt.getTopic().equals("order")){

        }

        if(messageExt.getTopic().equals("product")){

        }*/

        return true;
    }

    private JSONObject getDataFromModal(String serviceOrderId){
        try {
            String result = HttpHelper.get(simpleUrl + serviceOrderId);
            Map map = JSONObject.parseObject(result, Map.class);
            Object data = map.get("data");
            if(data!=null) return JSONObject.parseObject(data.toString());
            else return null;
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            e.printStackTrace();
            return null;
        }
    }

    /**
     *
     * @param userid
     * @return
     */
    private List<Long> getServiceOrderidsByUseridFromModel(String userid){
        try {
            String result = HttpHelper.get(userUrl + userid);
            Map map = JSONObject.parseObject(result, Map.class);
            List<Long> data = (List<Long>)map.get("data");
            if(data!=null) return data;
            else return null;
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            e.printStackTrace();
            return null;
        }
    }
}
