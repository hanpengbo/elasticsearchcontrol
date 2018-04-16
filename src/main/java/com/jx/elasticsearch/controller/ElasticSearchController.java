package com.jx.elasticsearch.controller;

import com.jx.elasticsearch.service.ElasticsearchService;
import com.jx.elasticsearch.service.IndexFieldService;
import com.jx.elasticsearch.utils.JSONResult;
import com.jx.elasticsearch.utils.ResultUtil;
import com.jx.elasticsearch.utils.elasticsearch.EsPage;
import com.jx.elasticsearch.vo.ElasticSearchVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

/**
 * Created by hpb on 2018-03-15.
 */
@RestController
@RequestMapping("/es")
public class ElasticSearchController {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchController.class);

    @Autowired
    private IndexFieldService indexFieldService;

    @Autowired
    private ElasticsearchService elasticsearchService;

    /**
     * @Params [index]
     * @Return com.jx.elasticsearch.utils.JSONResult
     * @Creater yangyang
     * @CreateTime 2018/3/19 17:33
     * @Remark 创建ES索引
     */
    @RequestMapping("/createIndex")
    public JSONResult createIndex() {
        try {
            indexFieldService.addIndexField();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return ResultUtil.error(-1, e.getMessage());
        }
        return ResultUtil.success();
    }

    /**
     * @Params [elasticSearchVo]
     * @Return com.jx.elasticsearch.utils.JSONResult<java.lang.String>
     * @Creater yangyang
     * @CreateTime 2018/3/19 17:33
     * @Remark 插入数据
     */
    @RequestMapping("/insertData")
    public JSONResult insertData(@RequestBody ElasticSearchVo elasticSearchVo) {
        if (elasticSearchVo == null) {
            logger.info("参数为空");
            return ResultUtil.error(-1, "参数为空");
        }
        String id = null;
        try {
            id = elasticsearchService.insertData(elasticSearchVo);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            ResultUtil.error(-1, e.getMessage());
        }
        return ResultUtil.success(id);
    }

    @RequestMapping("/receive")
    public JSONResult receive() {

        return ResultUtil.success();
    }

    /**
     * @Params [elasticSearchVo]
     * @Return com.jx.elasticsearch.utils.JSONResult
     * @Creater yangyang
     * @CreateTime 2018/3/19 17:32
     * @Remark 精确查询
     */
    @RequestMapping("/searchExact")
    public JSONResult searchExact(@RequestBody ElasticSearchVo elasticSearchVo) {
        if (elasticSearchVo == null) {
            logger.info("参数为空");
            return ResultUtil.error(-1, "参数为空");
        }

        List list;
        try {
            list = elasticsearchService.searchExact(elasticSearchVo);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return ResultUtil.error(-1, e.getMessage());
        }
        return ResultUtil.success(list);
    }

    /**
     * @Params [elasticSearchVo]
     * @Return com.jx.elasticsearch.utils.JSONResult
     * @Creater yangyang
     * @CreateTime 2018/3/19 17:32
     * @Remark 模糊查询
     */
    @RequestMapping("/searchFuzzy")
    public JSONResult searchFuzzy(@RequestBody ElasticSearchVo elasticSearchVo) {
        if (elasticSearchVo == null) {
            logger.info("参数为空");
            return ResultUtil.error(-1, "参数为空");
        }
        List list;
        try {
            list = elasticsearchService.searchFuzzy(elasticSearchVo);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return ResultUtil.error(-1, e.getMessage());
        }
        return ResultUtil.success(list);
    }

    /**
     * @Params [elasticSearchVo, exactMap 精确查询条件, fuzzyMap 模糊查询条件]
     * @Return com.jx.elasticsearch.utils.JSONResult
     * @Creater yangyang
     * @CreateTime 2018/3/19 17:31
     * @Remark 多条件精确和模糊查询
     */
    @RequestMapping("/searchExactAndFuzzy")
    public JSONResult searchExactAndFuzzy(@RequestBody ElasticSearchVo elasticSearchVo) {
        if (elasticSearchVo == null) {
            logger.info("参数为空");
            return ResultUtil.error(-1, "参数为空");
        }
        List list;
        try {
            list = elasticsearchService.searchExactAndFuzzy(elasticSearchVo);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return ResultUtil.error(-1, e.getMessage());
        }
        return ResultUtil.success(list);
    }

    /**
     * @Params [elasticSearchVo]
     * @Return com.jx.elasticsearch.utils.JSONResult
     * @Creater yangyang
     * @CreateTime 2018/3/23 10:40
     * @Remark 分页搜索
     */
    @RequestMapping("/searchPage")
    public JSONResult searchPage(@RequestBody ElasticSearchVo elasticSearchVo) {
        if (elasticSearchVo == null) {
            logger.info("参数为空");
            return ResultUtil.error(-1, "参数为空");
        }
        EsPage esPage = null;
        try {
            esPage = elasticsearchService.searchPage(elasticSearchVo);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return ResultUtil.error(-1, e.getMessage());
        }
        return ResultUtil.success(esPage);
    }

    /**
     * 全量更新服务单的所有索引数据
     * @return
     */
    @RequestMapping("/updateAllServiceOrder")
    public JSONResult updateAllServiceOrder() {
        try {
            long start = System.currentTimeMillis();
            elasticsearchService.updateAllServiceOrder();
            long end = System.currentTimeMillis();
            logger.debug("全量更新用时:"+(end-start)/1000+"s");
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            return ResultUtil.error(-1,"全量更新服务单的所有索引数据失败");
        }
        return ResultUtil.success();
    }
    /**
     *
     * 批量更新索引
     */
   /* public JSONResult batchUpdate(List<String> serviceOrderIds){

    }*/

    /**
     * 工具箱生成一个新服务单的索引数据
     * @param serviceOrderIds
     * @return
     */
    @RequestMapping("/updateServiceOrders")
    public JSONResult updateServiceOrders(@RequestBody String serviceOrderIds){
        try {
            if(serviceOrderIds!=null){
                String[] split = serviceOrderIds.split("\\n");
                List<String> list = Arrays.asList(split);
                elasticsearchService.batchUpdateServiceOrder(list);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            return ResultUtil.error(-1,e.getMessage());
        }
        return ResultUtil.success();
    }

}
