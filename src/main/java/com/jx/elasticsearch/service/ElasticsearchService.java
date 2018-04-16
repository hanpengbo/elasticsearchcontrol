package com.jx.elasticsearch.service;

import com.jx.elasticsearch.utils.elasticsearch.EsPage;
import com.jx.elasticsearch.vo.ElasticSearchVo;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @Author yangyang【yangyang@lvzheng.com】
 * @Date 2018/3/15 12:28
 * @Remark
 */
@Service
public interface ElasticsearchService {

    void createIndex(String indexName) throws Exception;

    String insertData(ElasticSearchVo elasticSearchVo)throws Exception;

    List searchExact(ElasticSearchVo elasticSearchVo) throws Exception;

    List searchFuzzy(ElasticSearchVo elasticSearchVo) throws Exception;

    List searchExactAndFuzzy(ElasticSearchVo elasticSearchVo) throws Exception;

    EsPage searchPage(ElasticSearchVo elasticSearchVo) throws Exception;

    boolean updateData(ElasticSearchVo elasticSearchVo) throws Exception;

    void updateAllServiceOrder() throws Exception;

    void updateServiceOrder(String serviceOrderId) throws Exception;

    void batchUpdateServiceOrder(List<String> serviceOrderIds)throws Exception;

    List<Map<String,Object>> getDataFromModalByServiceOrderIds(List<String> serviceOrderIds);

    void batchUpdateServiceOrderByQuerySize(List<String> serviceOrderIds)throws Exception;
}
