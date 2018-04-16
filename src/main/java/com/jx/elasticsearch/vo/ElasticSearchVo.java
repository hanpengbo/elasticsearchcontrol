package com.jx.elasticsearch.vo;

import com.alibaba.fastjson.JSONObject;
import com.jx.elasticsearch.utils.elasticsearch.EsPage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author yangyang【yangyang@lvzheng.com】
 * @Date 2018/3/7 10:13
 * @Remark es 索引实体类
 */
public class ElasticSearchVo implements Serializable {

    /**
     * 索引名
     */
    private String indexName;

    /**
     * 类型名
     */
    private String typeName;

    /**
     * 索引id
     */
    private String id;

    /**
     * 索引数据
     */
    private JSONObject jsonData;

    /**
     * 精确查询条件
     */
    private Map<String,String> exactMap = new HashMap<>();

    /**
     * 模糊查询条件
     */
    private Map<String,String> fuzzyMap = new HashMap<>();

    /**
     * 分页参数
     */
    private EsPage esPage;

    private Long startTime;

    private Long endTime;

    private String rangeField;
    /**
     * 查询字段
     * @return 多个逗号分隔
     */
    private String fields;

    /**
     * 排序字段
     * @return
     */
    private String sortField;

    public String getSortField() {
        return sortField;
    }

    public void setSortField(String sortField) {
        this.sortField = sortField;
    }

    public EsPage getEsPage() {
        return esPage;
    }

    public void setEsPage(EsPage esPage) {
        this.esPage = esPage;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JSONObject getJsonData() {
        return jsonData;
    }

    public void setJsonData(JSONObject jsonData) {
        this.jsonData = jsonData;
    }

    public Map<String, String> getExactMap() {
        return exactMap;
    }

    public void setExactMap(Map<String, String> exactMap) {
        this.exactMap = exactMap;
    }

    public Map<String, String> getFuzzyMap() {
        return fuzzyMap;
    }

    public void setFuzzyMap(Map<String, String> fuzzyMap) {
        this.fuzzyMap = fuzzyMap;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getRangeField() {
        return rangeField;
    }

    public void setRangeField(String rangeField) {
        this.rangeField = rangeField;
    }

    @Override
    public String toString() {
        return "ElasticSearchVo{" +
                "indexName='" + indexName + '\'' +
                ", typeName='" + typeName + '\'' +
                ", id='" + id + '\'' +
                ", jsonData=" + jsonData +
                ", exactMap=" + exactMap +
                ", fuzzyMap=" + fuzzyMap +
                ", esPage=" + esPage +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", rangeField='" + rangeField + '\'' +
                ", fields='" + fields + '\'' +
                ", sortField='" + sortField + '\'' +
                '}';
    }
}
