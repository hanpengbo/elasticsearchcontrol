package com.jx.elasticsearch.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.jx.elasticsearch.service.ElasticsearchService;
import com.jx.elasticsearch.service.IndexFieldService;
import com.jx.elasticsearch.utils.elasticsearch.ElasticsearchUtils;
import com.jx.elasticsearch.utils.http.HttpHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hpb on 2018-03-13.
 */
@Service
public class IndexFieldServiceImpl implements IndexFieldService {

    private static final Logger logger = LoggerFactory.getLogger(IndexFieldServiceImpl.class);

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Value("${serviceorder.modal.allurl}")
    private String allServiceOrderUrl;

    @Value("${elasticsearch.serviceorder.index}")
    private String serviceOrderIndex;

    @Value("${elasticsearch.serviceorder.type}")
    private String serviceOrderType;

    @Value("${serviceorder.model.userurl}")
    private String userUrl;

    @Value("${serviceorder.model.querysize}")
    private int querySize;




    @Override
    public void addIndexField() throws Exception {
        //1 mysql存值
//        insertIndexAndField(index);
        //创建索引
//        elasticsearchService.createIndex(index.getIndexName());
        new Thread(() -> {
            try {
                long start = System.currentTimeMillis();
                String result1 = HttpHelper.get(userUrl);  //获取全部服务单id
                Map map1 = JSONObject.parseObject(result1, Map.class);
                List data1 = (List) map1.get("data");
                if(data1!=null){
                    int pc = data1.size()%querySize == 0?data1.size()/querySize:(data1.size()/querySize + 1);
                    for(int i=0;i<pc;i++){
                        if(i==(pc-1)){
                            List list = data1.subList(i * querySize, data1.size());
                            List data = elasticsearchService.getDataFromModalByServiceOrderIds(list);
                            ElasticsearchUtils.batchInsertData(serviceOrderIndex,serviceOrderType,data);
                        }else {
                            List list = data1.subList(i * querySize, (i + 1) * querySize);
                            List data = elasticsearchService.getDataFromModalByServiceOrderIds(list);
                            ElasticsearchUtils.batchInsertData(serviceOrderIndex,serviceOrderType,data);
                        }
                    }
                }
                long end = System.currentTimeMillis();
                logger.debug("全量更新用时:"+(end-start)/1000+"s");
                //获取所有服务单id
              /*  String result = HttpHelper.get(allServiceOrderUrl);
                Map map = JSONObject.parseObject(result, Map.class);
                List data = (List) map.get("data");
                ElasticsearchUtils.batchInsertData(serviceOrderIndex,serviceOrderType,data);*/
                logger.info("所有服务单索引创建成功");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}
