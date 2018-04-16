package com.jx.elasticsearch.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jx.elasticsearch.service.ElasticsearchService;
import com.jx.elasticsearch.utils.elasticsearch.ElasticsearchUtils;
import com.jx.elasticsearch.utils.elasticsearch.EsPage;
import com.jx.elasticsearch.utils.http.HttpHelper;
import com.jx.elasticsearch.utils.http.HttpRequest;
import com.jx.elasticsearch.utils.http.HttpResponse;
import com.jx.elasticsearch.vo.ElasticSearchVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Author yangyang【yangyang@lvzheng.com】
 * @Date 2018/3/15 14:06
 * @Remark
 */
@Service
public class ElasticsearchServiceImpl implements ElasticsearchService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchServiceImpl.class);

    /**
     * @Params [indexName]
     * @Return void
     * @Creater yangyang
     * @CreateTime 2018/3/22 13:33
     * @Remark 创建索引 验证是否存在
     */
    public void createIndex(String indexName) throws Exception {
        // 判断index是否存在
        boolean indexExist = ElasticsearchUtils.isIndexExist(indexName);
        // 不存在创建index
        if (!indexExist) {
            boolean index = ElasticsearchUtils.createIndex(indexName);

            if (!index) {
                logger.error("es创建索引失败");
                throw new Exception("es创建索引失败");
            }
        }
    }

    /**
     * @Params [elasticSearchVo]
     * @Return java.lang.String
     * @Creater yangyang
     * @CreateTime 2018/3/22 13:33
     * @Remark es 索引插入数据 并返回id
     */
    @Override
    public String insertData(ElasticSearchVo elasticSearchVo) throws Exception {

        String id = ElasticsearchUtils.addData(
                elasticSearchVo.getJsonData(),
                elasticSearchVo.getIndexName(),
                elasticSearchVo.getTypeName());
        if (id == null) {
            logger.error("es索引数据添加失败,索引:" + elasticSearchVo);
            throw new Exception("索引数据添加失败,索引:" + elasticSearchVo);
        }
        return id;
    }

    @Override
    public List searchExact(ElasticSearchVo elasticSearchVo) throws Exception {
        return ElasticsearchUtils.searchExact(
                elasticSearchVo.getIndexName(),
                elasticSearchVo.getTypeName(),
                elasticSearchVo.getExactMap());
    }

    @Override
    public List searchFuzzy(ElasticSearchVo elasticSearchVo) throws Exception {
        return ElasticsearchUtils.searchFuzzy(
                elasticSearchVo.getIndexName(),
                elasticSearchVo.getTypeName(),
                elasticSearchVo.getFuzzyMap());
    }

    @Override
    public List searchExactAndFuzzy(ElasticSearchVo elasticSearchVo) throws Exception {
        if (elasticSearchVo.getExactMap() == null || elasticSearchVo.getExactMap().size() == 0) {
            return ElasticsearchUtils.searchFuzzy(
                    elasticSearchVo.getIndexName(),
                    elasticSearchVo.getTypeName(),
                    elasticSearchVo.getFuzzyMap());
        }
        if (elasticSearchVo.getFuzzyMap() == null || elasticSearchVo.getFuzzyMap().size() == 0) {
            return ElasticsearchUtils.searchExact(
                    elasticSearchVo.getIndexName(),
                    elasticSearchVo.getTypeName(),
                    elasticSearchVo.getExactMap());
        } else {
            return ElasticsearchUtils.searchExactAndFuzzy(
                    elasticSearchVo.getIndexName(),
                    elasticSearchVo.getTypeName(),
                    elasticSearchVo.getExactMap(),
                    elasticSearchVo.getFuzzyMap());
        }
    }

    /**
     * 分页查询
     * @param elasticSearchVo
     * @return
     * @throws Exception
     */
    @Override
    public EsPage searchPage(ElasticSearchVo elasticSearchVo) throws Exception {

        Map<String, String> exactMap = elasticSearchVo.getExactMap();
        Map<String, String> fuzzyMap = elasticSearchVo.getFuzzyMap();
        StringBuffer matchExactStr = new StringBuffer();
        if (exactMap != null && exactMap.size() != 0) {
            exactMap.forEach((key, value) -> matchExactStr.append(key).append("=").append(value).append(","));
            matchExactStr.deleteCharAt(matchExactStr.lastIndexOf(","));
        }

        StringBuffer matchFuzzyStr = new StringBuffer();
        if (fuzzyMap != null && fuzzyMap.size() != 0) {
            fuzzyMap.forEach((key, value) -> matchFuzzyStr.append(key).append("=").append(value).append(","));
            matchFuzzyStr.deleteCharAt(matchFuzzyStr.lastIndexOf(","));
        }

        EsPage esPage = elasticSearchVo.getEsPage();

        Integer currentPage;

        Integer pageSize;

        if (esPage != null) {
            currentPage = esPage.getCurrentPage();
            pageSize = esPage.getPageSize();
            if (currentPage == null) {
                currentPage = 0;
            }

            if (pageSize == null) {
                pageSize = 30;
            }
        } else {
            currentPage = 0;
            pageSize = 30;
        }

        Long startTime = elasticSearchVo.getStartTime();

        Long endTime = elasticSearchVo.getEndTime();

        return ElasticsearchUtils.searchDataPage(
                elasticSearchVo.getIndexName(),     // 索引名称
                elasticSearchVo.getTypeName(),      // 类型名称,可传入多个type逗号分隔
                currentPage-1,  // 当前页
                pageSize,     // 每页显示多少条
                startTime,
                endTime,
                elasticSearchVo.getFields(),           // 需要显示的字段，逗号分隔（缺省为全部字段）
                elasticSearchVo.getSortField(),   // 排序字段
                null,     // 高亮字段
                matchExactStr.toString(),
                matchFuzzyStr.toString(), // 过滤条件（xxx=111,aaa=222）
                elasticSearchVo.getRangeField());
    }

    /**
     * 更新数据
     * @param elasticSearchVo
     * @return
     * @throws Exception
     */
    @Override
    public boolean updateData(ElasticSearchVo elasticSearchVo) throws Exception {
        String index = elasticSearchVo.getIndexName();
        String type = elasticSearchVo.getTypeName();
        JSONObject jsonData = elasticSearchVo.getJsonData();
        Map<String, String> exactMap = elasticSearchVo.getExactMap();
        return ElasticsearchUtils.updateData(index, type, exactMap, jsonData);
    }

    @Value("${elasticsearch.serviceorder.index}")
    private String serviceOrderIndex;

    @Value("${elasticsearch.serviceorder.type}")
    private String serviceOrderType;

    @Value("${serviceorder.modal.simpleurl}")
    private String simpleUrl;

    @Value("${serviceorder.model.batchurl}")
    private String batchurl;

    @Value("${serviceorder.model.userurl}")
    private String userurl;

    @Value("${serviceorder.model.querysize}")
    private int querySize;

    //全量更新
    @Override
    public void updateAllServiceOrder() throws Exception{
        String result = HttpHelper.get(userurl);
        Map map = JSONObject.parseObject(result, Map.class);
        int code = Integer.parseInt(map.get("code").toString());
        if(code == 200){
            List<String> data = (List<String>)map.get("data");
            if(data!=null){
               batchUpdateServiceOrderByQuerySize(data);
            }
        }
    }

    @Override
    public void batchUpdateServiceOrderByQuerySize(List<String> serviceOrderIds) throws Exception {
        if(serviceOrderIds!=null){
            int pc = serviceOrderIds.size()%querySize == 0?serviceOrderIds.size()/querySize:(serviceOrderIds.size()/querySize + 1);
            for(int i=0;i<pc;i++){
                if(i==(pc-1)){
                    List list = serviceOrderIds.subList(i * querySize, serviceOrderIds.size());
                    batchUpdateServiceOrder(list);
                }else {
                    List list = serviceOrderIds.subList(i * querySize, (i + 1) * querySize);
                    batchUpdateServiceOrder(list);
                }
            }
        }
    }

    @Override
    public void batchUpdateServiceOrder(List<String> serviceOrderIds) throws Exception {
        if(serviceOrderIds!=null){
            long start = System.currentTimeMillis();
            List<Map<String, Object>> serviceOrders = getDataFromModalByServiceOrderIds(serviceOrderIds);
            long end = System.currentTimeMillis();
            logger.debug("单次数据查询用时"+(end-start)/1000+"s");
            if(serviceOrders!=null && serviceOrders.size()>0){
                Map<String,JSONObject> batchData = new HashMap<>();
                List<String> soid = new ArrayList<>();
                for (Map<String, Object> serviceOrder : serviceOrders) {
                    String serviceOrderId = serviceOrder.get("serviceOrderId").toString();
                    soid.add(serviceOrderId);
                }
                if(soid.size()>0) {
                    Map<String, String> map = ElasticsearchUtils.searchDataByIn(serviceOrderIndex, serviceOrderType, soid);
                    for (Map<String, Object> serviceOrder : serviceOrders) {
                        Object serviceOrderId = serviceOrder.get("serviceOrderId");
                        String id = map.get(serviceOrderId.toString());//索引id
                        if(id!=null) {
                            batchData.put(id,JSONObject.parseObject(serviceOrder.toString()));
                        }else {
                            logger.info("原服务单在ES中不存在新添加，改服务单id为："+serviceOrder.get("serviceOrderId").toString());
                            batchData.put(UUID.randomUUID().toString().replaceAll("-", "").toUpperCase(),JSONObject.parseObject(serviceOrder.toString()));
                        }
                    }
                }
                logger.info("es控制器组合数据成功 时间为"+ new Date());
                ElasticsearchUtils.batchUpdateData(serviceOrderIndex,serviceOrderType,batchData);
                logger.info("es控制器批量更新结束 时间为"+ new Date());
            }
        }
    }

    @Override
    public void updateServiceOrder(String serviceOrderId) throws Exception {
        HashMap<String, String> exactMap = new HashMap<>();
        exactMap.put("serviceOrderId", serviceOrderId);
        JSONObject jsonObject = getDataFromModal(serviceOrderId);
        if(jsonObject!=null){
            // ES更新数据
            ElasticSearchVo elasticSearchVo = new ElasticSearchVo();
            elasticSearchVo.setIndexName(serviceOrderIndex);
            elasticSearchVo.setTypeName(serviceOrderType);
            elasticSearchVo.setJsonData(jsonObject);
            Map<String, String> map = new HashMap<>();
            map.put("serviceOrderId",serviceOrderId);
            elasticSearchVo.setExactMap(map);
            boolean b1 = updateData(elasticSearchVo);
            if (!b1) {
                throw new RuntimeException("更新失败");
            }
        }
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

    //多服务单获取数据
    public List<Map<String,Object>> getDataFromModalByServiceOrderIds(List<String> serviceOrderIds){
        try {
            HttpRequest httpRequest = new HttpRequest(batchurl);
            httpRequest.addHeader("Content-Type", "application/json; charset=UTF-8");
            httpRequest.setContent(JSON.toJSONString(serviceOrderIds));
            HttpResponse response = HttpHelper.post(httpRequest);
            String content1 = response.getContent();
            JSONObject jsonObject = JSONObject.parseObject(content1);
            int code = Integer.parseInt(jsonObject.get("code").toString());
            if(code == 200) {
               List<Map<String,Object>> list = (List<Map<String, Object>>)jsonObject.get("data");
                logger.info("es控制器获取全部数据成功 时间为："+new Date());
                if(list!=null && list.size()>0) return list;
                else return null;
            }else{
                Object msg = jsonObject.get("msg");
                logger.info(msg.toString());
                return null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            e.printStackTrace();
            return null;
        }
    }
}
