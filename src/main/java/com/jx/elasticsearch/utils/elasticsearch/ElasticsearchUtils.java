package com.jx.elasticsearch.utils.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

import static sun.security.krb5.internal.LoginOptions.MAX;

/**
 * Created by hpb on 2018-03-12.
 */
@Component
public class ElasticsearchUtils {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchUtils.class);

    @Autowired
    private TransportClient transportClient;

    private static TransportClient client;

    @PostConstruct
    public void init() {
        client = this.transportClient;
    }

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) {
        if (!isIndexExist(index)) {
            logger.info("Index is not exits!");
        }
        CreateIndexResponse indexresponse = client.admin()
                .indices()
                .prepareCreate(index)
                .execute()
                .actionGet();
        logger.info("执行建立成功？" + indexresponse.isAcknowledged());

        return indexresponse.isAcknowledged();
    }

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    public static boolean deleteIndex(String index) {
        if (!isIndexExist(index)) {
            logger.info("Index is not exits!");
        }
        DeleteIndexResponse dResponse = client
                .admin()
                .indices()
                .prepareDelete(index)
                .execute()
                .actionGet();
        if (dResponse.isAcknowledged()) {
            logger.info("delete index " + index + "  successfully!");
        } else {
            logger.info("Fail to delete index " + index);
        }
        return dResponse.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) {
        IndicesExistsResponse inExistsResponse = client
                .admin()
                .indices()
                .exists(new IndicesExistsRequest(index))
                .actionGet();
        if (inExistsResponse.isExists()) {
            logger.info("Index [" + index + "] is exist!");
        } else {
            logger.info("Index [" + index + "] is not exist!");
        }
        return inExistsResponse.isExists();
    }

    /**
     * 数据添加，正定ID
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type, String id) {

        IndexResponse response = client
                .prepareIndex(index, type, id)
                .setSource(jsonObject)
                .get();

        logger.info("addData response status:{},id:{}", response.status().getStatus(), response.getId());

        return response.getId();
    }

    /**
     * 数据添加
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type) {
        return addData(
                jsonObject,
                index,
                type,
                UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    public static void deleteDataById(String index, String type, String id) {

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();

        logger.info("deleteDataById response status:{},id:{}", response.status().getStatus(), response.getId());
    }

    /**
     * 通过ID 更新数据
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static void updateDataById(JSONObject jsonObject, String index, String type, String id) {

        UpdateRequest updateRequest = new UpdateRequest();

        updateRequest.index(index).type(type).id(id).doc(jsonObject);

        client.update(updateRequest);

    }

    /**
     * 批量更新数据
     */
     public static void batchUpdateData(String index, String type, Map<String, JSONObject> data){
         BulkRequestBuilder bulkRequestBuilder = client.prepareBulk() ;
         data.forEach((key,value)->{
            // UpdateRequest updateRequest = new UpdateRequest(index,type,key).doc(value);
             bulkRequestBuilder.add(client.prepareUpdate(index,type,key).setDoc(value).setUpsert(value));
         });
         bulkRequestBuilder.get();
     }

    /**
     * 通过ID获取数据
     *
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    public static Map<String, Object> searchDataById(String index, String type, String id, String fields) {

        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);

        if (StringUtils.isNotEmpty(fields)) {
            getRequestBuilder.setFetchSource(fields.split(","), null);
        }

        GetResponse getResponse = getRequestBuilder.execute().actionGet();

        return getResponse.getSource();
    }

    /**
     * 使用分词查询
     *
     * @param index     索引名称
     * @param type      类型名称,可传入多个type逗号分隔
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @param size      文档大小限制
     * @param matchStr  过滤条件（xxx=111,aaa=222）
     * @return
     */
    public static List<Map<String, Object>> searchListData(String index, String type, long startTime, long endTime, Integer size, String matchStr) {
        return searchListData(
                index,
                type,
                startTime,
                endTime,
                size,
                null,
                null,
                false,
                null,
                matchStr);
    }

    /**
     * 使用分词查询
     *
     * @param index    索引名称
     * @param type     类型名称,可传入多个type逗号分隔
     * @param size     文档大小限制
     * @param fields   需要显示的字段，逗号分隔（缺省为全部字段）
     * @param matchStr 过滤条件（xxx=111,aaa=222）
     * @return
     */
    public static List<Map<String, Object>> searchListData(String index, String type, Integer size, String fields, String matchStr) {
        return searchListData(
                index,
                type,
                0,
                0,
                size,
                fields,
                null,
                false,
                null,
                matchStr);
    }

    /**
     * 使用分词查询
     *
     * @param index       索引名称
     * @param type        类型名称,可传入多个type逗号分隔
     * @param size        文档大小限制
     * @param fields      需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField   排序字段
     * @param matchPhrase true 使用，短语精准匹配
     * @param matchStr    过滤条件（xxx=111,aaa=222）
     * @return
     */
    public static List<Map<String, Object>> searchListData(String index, String type, Integer size, String fields, String sortField, boolean matchPhrase, String matchStr) {
        return searchListData(
                index,
                type,
                0,
                0,
                size,
                fields,
                sortField,
                matchPhrase,
                null,
                matchStr);
    }


    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param matchPhrase    true 使用，短语精准匹配
     * @param highlightField 高亮字段
     * @param matchStr       过滤条件（xxx=111,aaa=222）
     * @return
     */
    public static List<Map<String, Object>> searchListData(String index, String type, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) {
        return searchListData(
                index,
                type,
                0,
                0,
                size,
                fields,
                sortField,
                matchPhrase,
                highlightField,
                matchStr);
    }

    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param startTime      开始时间
     * @param endTime        结束时间
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param matchPhrase    true 使用，短语精准匹配
     * @param highlightField 高亮字段
     * @param matchStr       过滤条件（xxx=111,aaa=222）
     * @return
     */
    public static List<Map<String, Object>> searchListData(String index, String type, long startTime, long endTime, Integer size, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) {

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (startTime > 0 && endTime > 0) {
            boolQuery.must(QueryBuilders.rangeQuery("timestamp")
                    .format("epoch_millis")
                    .from(startTime)
                    .to(endTime)
                    .includeLower(true)
                    .includeUpper(true));
        }

        //搜索的的字段
        if (StringUtils.isNotEmpty(matchStr)) {
            Arrays.stream(matchStr.split(",")).forEach(s -> {
                String[] ss = s.split("=");
                if (ss.length > 1) {
                    if (matchPhrase == Boolean.TRUE) {
                        boolQuery.must(QueryBuilders.matchPhraseQuery(s.split("=")[0], s.split("=")[1]));
                    } else {
                        boolQuery.must(QueryBuilders.wildcardQuery(s.split("=")[0], s.split("=")[1]));
                    }
                }
            });
        }

        // 高亮（xxx=111,aaa=222）
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            //highlightBuilder.preTags("<span style='color:red' >");//设置前缀
            //highlightBuilder.postTags("</span>");//设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }


        searchRequestBuilder.setQuery(boolQuery);

        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }
        searchRequestBuilder.setFetchSource(true);

        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        if (size != null && size > 0) {
            searchRequestBuilder.setSize(size);
        }

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        logger.info("\n{}", searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        logger.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            return setSearchResponse(searchResponse, highlightField);
        }

        return null;

    }

    /**
     * 使用分词查询,并分页
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param currentPage    当前页
     * @param pageSize       每页显示条数
     * @param startTime      开始时间
     * @param endTime        结束时间
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param matchPhrase    true 使用，短语精准匹配
     * @param highlightField 高亮字段
     * @param matchStr       过滤条件（xxx=111,aaa=222）
     * @return
     */
    public static EsPage searchDataPage(String index, String type, int currentPage, int pageSize, long startTime, long endTime, String fields, String sortField, boolean matchPhrase, String highlightField, String matchStr) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        //排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (startTime > 0 && endTime > 0) {
            boolQuery.must(QueryBuilders.rangeQuery("timestamp")
                    .format("epoch_millis")
                    .from(startTime)
                    .to(endTime)
                    .includeLower(true)
                    .includeUpper(true));
        }

        // 查询字段
        if (StringUtils.isNotEmpty(matchStr)) {
            Arrays.stream(matchStr.split(",")).forEach(s -> {
                String[] ss = s.split("=");
                if (matchPhrase == Boolean.TRUE) {
                    boolQuery.must(QueryBuilders.matchPhraseQuery(ss[0], ss[1]));
                } else {
                    boolQuery.must(QueryBuilders.wildcardQuery(ss[0], ss[1]));
                }
            });
        }

        // 高亮（xxx=111,aaa=222）
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            //highlightBuilder.preTags("<span style='color:red' >");//设置前缀
            //highlightBuilder.postTags("</span>");//设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(boolQuery);

        // 分页应用
        searchRequestBuilder.setFrom(currentPage).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        logger.info("\n{}", searchRequestBuilder);

        // 执行搜索,返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        logger.debug("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

            return new EsPage(currentPage, pageSize, (int) totalHits, sourceList);
        }

        return null;

    }

    /**
     * 高亮结果集 特殊处理
     *
     * @param searchResponse
     * @param highlightField
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        StringBuffer stringBuffer = new StringBuffer();

        //遍历 高亮结果集，覆盖 正常结果集
        for (SearchHit searchHit : searchResponse.getHits()) {
            searchHit.getSource().put("id", searchHit.getId());
            if (StringUtils.isNotEmpty(highlightField)) {

                logger.info("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSource());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    //遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSource().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSource());
        }

        return sourceList;
    }

    /**
     * 功能描述：批量插入数据
     *
     * @param index 索引名
     * @param type  类型
     * @param data  (_id 主键, json 数据)
     */
    public static void batchInsertData(String index, String type, Map<String, Object> data) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        data.forEach((param1, param2) -> bulkRequest.add(client.prepareIndex(index, type, param1)
                .setSource(param2)
        ));
        BulkResponse bulkResponse = bulkRequest.get();
    }

    /**
     * 功能描述：批量插入数据
     *
     * @param index    索引名
     * @param type     类型
     * @param jsonList 批量数据
     */
    public static void batchInsertData(String index, String type, List<JSONObject> jsonList) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (JSONObject item : jsonList) {
            bulkRequest.add(client.prepareIndex(index, type)
                    .setSource(item)
            );
        }
        BulkResponse bulkResponse = bulkRequest.get();
    }


    /**
     * 根据文档名、字段名、字段值查询某一条记录的详细信息；query查询
     *
     * @param type  文档名，相当于oracle中的表名，例如：ql_xz；
     * @param key   字段名，例如：bdcqzh
     * @param value 字段值，如：“”
     * @return List
     * @author yangyang
     */
    public static List searchExact(String index, String type, String key, String value) {
        QueryBuilder qb = QueryBuilders.termQuery(key, value);
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(qb)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();
        return responseToList(client, response);
    }


    /**
     * 多条件  文档名、字段名、字段值，查询某一条记录的详细信息
     *
     * @param type 文档名，相当于oracle中的表名，例如：ql_xz
     * @param map  字段名：字段值 的map
     * @return List
     * @author yangyang
     */
    public static List searchExact(String index, String type, Map<String, String> map) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        map.keySet().forEach(in -> {
            String str = map.get(in);//得到每个key多对用value的值
            boolQueryBuilder.must(QueryBuilders.termQuery(in, str));
        });
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(boolQueryBuilder)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();
        return responseToList(client, response);
    }

   /* public static List searchExactToFileds(String index, String type, Map<String, String> map) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        map.keySet().forEach(in -> {
            String str = map.get(in);//得到每个key多对用value的值
            boolQueryBuilder.must(QueryBuilders.termQuery(in, str));
        });
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(boolQueryBuilder)
                .setFetchSource("serviceOrderId",null)
                .setFetchSource(true)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();
        Set<String> set = new HashSet<>();
        for (SearchHit searchHit : response.getHits()) {

        }


        return responseToList(client, response);
    }*/

    /**
     * 单条件 模糊查询
     *
     * @param type  文档名，相当于oracle中的表名，例如：ql_xz
     * @param key   字段名，例如：bdcqzh
     * @param value 字段名模糊值：如 *123* ;?123*;?123?;*123?;
     * @return List
     * @author yangyang
     */
    public static List searchFuzzy(String index, String type, String key, String value) {
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery(key, "*" + value + "*");
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(wildcardQueryBuilder)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();
        return responseToList(client, response);
    }

    /**
     * 多条件 模糊查询
     *
     * @param type type 文档名，相当于oracle中的表名，例如：ql_xz
     * @param map  包含key:value 模糊值键值对
     * @return List
     * @author yangyang
     */
    public static List searchFuzzy(String index, String type, Map<String, String> map) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        map.keySet().forEach(in -> {
            String str = map.get(in);
            boolQueryBuilder.must(QueryBuilders.wildcardQuery(in, "*"+str+"*"));
        });
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(boolQueryBuilder)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();

        return responseToList(client, response);
    }

    /**
     * 多条件  精确和模糊查询
     *
     * @param index    index 索引 相当于oracle中的数据库
     * @param type     type 文档名，相当于oracle中的表名，例如：ql_xz
     * @param exactMap 需要精确查询的字段和值
     * @param fuzzyMap 需要模糊查询的字段和值
     * @return
     */
    public static List searchExactAndFuzzy(String index, String type, Map<String, String> exactMap, Map<String, String> fuzzyMap) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        exactMap.keySet().forEach(exact -> {
            String str = exactMap.get(exact);
            boolQueryBuilder.must(QueryBuilders.termQuery(exact, str));
        });
        fuzzyMap.keySet().forEach(fuzzy -> {
            String str = fuzzyMap.get(fuzzy);
            boolQueryBuilder.must(QueryBuilders.wildcardQuery(fuzzy, "*"+str+"*"));
        });
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(boolQueryBuilder)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();
        return responseToList(client, response);
    }

    /**
     * or 查询
     * @param index
     * @param type
     * @param exactMap
     * @return
     */
    public static List searchDataByOr(String index, String type, Map<String,String> exactMap){

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        exactMap.keySet().forEach(exact -> {
            String str = exactMap.get(exact);
            boolQueryBuilder.should(QueryBuilders.termQuery(exact, str));
        });
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(boolQueryBuilder)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();
        return responseToList(client,response);
    }

    /**
     * es in 查询
     */
     public static Map<String, String> searchDataByIn(String index,String type,List<String> serviceorderids){
         SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);
         searchRequestBuilder.setQuery(QueryBuilders.termsQuery("serviceOrderId", serviceorderids));
         searchRequestBuilder.setFrom(0).setSize(10000);
         SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
         long totalHits = searchResponse.getHits().totalHits;
         long length = searchResponse.getHits().getHits().length;
         Map<String,String> map = new HashMap<>();
         for (SearchHit searchHit : searchResponse.getHits()) {
             Object serviceOrderId = searchHit.getSource().get("serviceOrderId");
             String id = searchHit.getId();
             map.put(serviceOrderId.toString(),id);
//             logger.info("查询到的服务单id 为："+serviceOrderId+"id 为："+id);
         }

         logger.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        return map;
     }

    /**
     * 将查询后获得的response转成list
     *
     * @param client
     * @param response
     * @return
     */
    private static List responseToList(TransportClient client, SearchResponse response) {
        SearchHits hits = response.getHits();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < hits.getHits().length; i++) {
            Map<String, Object> map = hits.getAt(i).getSource();
            map.put("id", hits.getAt(i).getId());
            list.add(map);
        }
//        client.close();
        return list;
    }

    /**
     * 分页查询
     * @param index 索引
     * @param type  类型
     * @param currentPage 当前页
     * @param pageSize    每页多少条
     * @param startTime   开始时间
     * @param endTime     结束时间
     * @param fields      查询字段  ","号分隔
     * @param sortField   排序字段
     * @param highlightField   高亮字段
     * @param matchExactStr    精确查询条件
     * @param matchFuzzyStr    模糊查询条件
     * @return
     */
    public static EsPage searchDataPage(String index, String type, Integer currentPage, Integer pageSize, Long startTime, Long endTime, String fields, String sortField, String highlightField, String matchExactStr, String matchFuzzyStr,String rangeField) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        //排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if(rangeField!=null && !rangeField.trim().equals("")){
            boolQuery.must(QueryBuilders.rangeQuery(rangeField)
                    .format("epoch_millis")
                    .gte(startTime).lte(endTime));
        }

        // 查询字段
        if (StringUtils.isNotEmpty(matchExactStr)) {
            Arrays.stream(matchExactStr.split(",")).forEach(s -> {
                String[] ss = s.split("=");
                boolQuery.must(QueryBuilders.matchPhraseQuery(ss[0], ss[1]));
            });
        }

        if (StringUtils.isNotEmpty(matchFuzzyStr)) {
            Arrays.stream(matchFuzzyStr.split(",")).forEach(s -> {
                String[] ss = s.split("=");
                boolQuery.must(QueryBuilders.wildcardQuery(ss[0], "*"+ss[1]+"*"));
            });
        }

        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(boolQuery);

        // 分页应用
        searchRequestBuilder.setFrom(currentPage*pageSize).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        logger.info("\n{}", searchRequestBuilder);

        // 执行搜索,返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        logger.debug("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

            return new EsPage(currentPage, pageSize, (int) totalHits, sourceList);
        }

        return null;

    }



    /**
     *
     * @param index      索引
     * @param type       类型
     * @param exactMap   搜索条件
     * @return
     * @throws Exception
     */
    public static boolean updateData(String index, String type, Map<String,String> exactMap, JSONObject jsonData) throws Exception {
        QueryBuilder qb = null;
        for (Map.Entry<String, String> entry : exactMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            qb = QueryBuilders.termQuery(key, value);
        }

        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(qb)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        if (hits.totalHits != 0) {
            for (SearchHit hit : hits) {
                String indexId = hit.getId();//这里获取到对应的文档ID
                JSONObject jsonObject = JSONObject.parseObject(hit.getSourceAsString());

                updateDataById(jsonData,index,type,indexId);
            }
        } else {
            addData(jsonData,index,type);
        }
        return true;
    }

    /**
     * 根据条件查询 数据是否存在
     * @param index 索引
     * @param type  类型
     * @param exactMap 查询条件
     * @return
     */
    public static boolean dataExist(String index, String type, Map<String, String> exactMap) {
        QueryBuilder qb = null;
        for (Map.Entry<String, String> entry : exactMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            qb = QueryBuilders.termQuery(key, value);
        }

        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(qb)
                .setFrom(0).setSize(10000).setExplain(true)
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        return hits.totalHits != 0;
    }

}
