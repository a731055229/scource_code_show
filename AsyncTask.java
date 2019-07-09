package com.migu.labelsys.es.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.migu.labelsys.aop.AopUtil;
import com.migu.labelsys.common.consts.LabelSysConstants;
import com.migu.labelsys.common.util.ElasticSqlUtil;
import com.migu.labelsys.dao.LabelInfoDao;
import com.migu.labelsys.dao.LabelSysDao;
import com.migu.labelsys.entity.LabelGroup;
import com.migu.labelsys.entity.LabelGroupModule;
import com.migu.labelsys.entity.LabelInfo;
import com.migu.labelsys.entity.ModuleInfo;
import com.migu.labelsys.es.EsConfig;
import com.migu.labelsys.vo.LabelQryVo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.*;

import static com.migu.labelsys.service.impl.LabelGroupServiceImpl.STR_COMMA;

/**
 * 异步方法
 */
 @Component
 public class AsyncTask {
    private static final Logger logger = LoggerFactory.getLogger(AsyncTask.class);
    private final static ExecutorService exec = new ThreadPoolExecutor(8, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new CustomThreadFoctory("labelGroup"));
     static {
        //应用停止，关闭exec
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!exec.isShutdown()) {
                exec.shutdown();
            }
            try {
                if (!exec.awaitTermination(60, TimeUnit.SECONDS)) {
                    exec.shutdownNow();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    @Autowired
    private LabelSysDao labelSysDao;
    @Autowired
    private LabelInfoDao labelInfoDao;
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private EsConfig esConfig;

    @Async
    public Future<Long> calLabelGroupValue(LabelGroup labelGroup) {
        //        异步实现计算
        try {
            if (labelGroup.getGroupId() == null) {
                return null;
            }
//            Thread.sleep(1000000);
            //       获取人群的标签Ids
            LabelGroup updatedGroup = new LabelGroup();
            String labelIds = labelGroup.getLabelIds();
            Long groupVal = 0L;
            StringBuilder whereExpress = concatLabelWhereSql(labelIds);
            if (StringUtils.isNotBlank(whereExpress.toString())) {
                //通过mysql格式获取boolQuery
                String querySql = String.format(" select count(*) from %s/%s ", esConfig.getUser_profile_index(),
                        esConfig.getUser_profile_type());
                BoolQueryBuilder boolQueryBuilder = ElasticSqlUtil.createQueryBuilderByWhere(querySql,
                        whereExpress.toString(), Boolean.FALSE);
                if (boolQueryBuilder != null) {
                    //es-查询对象
                    groupVal = countByQuery(Lists.newArrayList(boolQueryBuilder));
                }
            }
            //更新值以及状态
            updatedGroup.setGroupId(labelGroup.getGroupId()).setGroupValue(groupVal).setStatus(1);
            labelSysDao.updateLabelGroup(updatedGroup);

        } catch (Exception e) {
            logger.error("=====calLabelGroupValue ==>fail to calculate value fro label group .ex:{} ", e);
        }
        return new AsyncResult<>(labelGroup.getGroupId());
    }


    private Long countByQuery(List<QueryBuilder> queryBuilders) {
        //es-查询对象
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
        builder.withIndices(esConfig.getUser_profile_index());
        builder.withTypes(esConfig.getUser_profile_type());
        //添加查询
        if (null != queryBuilders) {
            for (QueryBuilder queryBuilder : queryBuilders) {
                builder.withQuery(queryBuilder);
            }
        }
        //查询工具
        SearchQuery searchQuery = builder.build();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(searchQuery.getQuery()).from(0).size(0);
        logger.info("countByQuery----->DSL=>" + searchSourceBuilder.toString());
        return elasticsearchTemplate.query(searchQuery, searchResponse -> searchResponse.getHits().getTotalHits());
    }

    /**
     * @param labelGroup
     * @return void
     * @desc 人群分模块统计值
     */
    @Async
    public void calPopulationModule(LabelGroup labelGroup) {
        String moduleIdsStr = labelGroup.getModuleIds();
        Long groupId = labelGroup.getGroupId();
        Set<Long> moduleIds = Sets.newLinkedHashSet();
        //验证模块
        if (StringUtils.isBlank(moduleIdsStr) || groupId == null) {
            logger.warn("calPopulationModule ==>no module configured.");
            return;
        }
        //获取模块ID
        String[] moduleIdArr = moduleIdsStr.split(LabelSysConstants.STR_COMMA);
        for (String moduleId : moduleIdArr) {
            moduleIds.add(Long.parseLong(moduleId.trim()));
        }
        //找到相应的模块
        LabelQryVo queryParam = new LabelQryVo();
        queryParam.setModuleIds(Lists.newArrayList(moduleIds));
        List<ModuleInfo> moduleInfos = labelSysDao.selectModuleInfos(queryParam);
        List<LabelGroupModule> labelGroupModules = Lists.newArrayList();
        //Map<id,moduleCode>
        Map<String, LabelGroupModule> moduleDataMap = Maps.newLinkedHashMap();
        if (CollectionUtils.isEmpty(moduleInfos)) {
            logger.warn("calPopulationModule ==>no module found .");
            return;
        }
        for (ModuleInfo moduleInfo : moduleInfos) {
            Long id = moduleInfo.getId();
            //添加落地记录
            LabelGroupModule labelGroupModule = new LabelGroupModule();
            labelGroupModule.setLabelGroupId(groupId).setModuleId(id).setStatus(0);
            labelGroupModules.add(labelGroupModule);
            moduleDataMap.put(moduleInfo.getModuleCode(), labelGroupModule);
        }
//       ((AsyncTask)AopContext.currentProxy()).deleteThenInsertLabelGroupModule(groupId, labelGroupModules);
         (AopUtil.getBean(this.getClass())).deleteThenInsertLabelGroupModule(groupId, labelGroupModules);
        //获取标签定义,获取whereExpress
        String labelIds = labelGroup.getLabelIds();
        StringBuilder whereExpress = concatLabelWhereSql(labelIds);
        if(StringUtils.isBlank(whereExpress.toString())){
            logger.warn("no filter for labelGroupId:[{}]",groupId);
            return ;
        }
        //获取过滤查询
        //通过mysql格式获取boolQuery
        String querySql = String.format(" select * from %s/%s", esConfig.getUser_profile_index(), esConfig.getUser_profile_type());
        BoolQueryBuilder boolQueryBuilder = ElasticSqlUtil.createQueryBuilderByWhere(querySql,
                whereExpress.toString(), Boolean.FALSE);
        //异步统计模块值
        List<ModuleStatTask> tasks = Lists.newArrayList();
        Map<String,String> idMap = new HashMap<>();
        for (String moduleCode : moduleDataMap.keySet()) {
            final ElasticQueryDao elasticQueryDao = new ElasticQueryDao(elasticsearchTemplate, esConfig, Lists
                    .newArrayList(boolQueryBuilder), moduleCode);
            if("M0021".equals(moduleCode) || "M0020".equals(moduleCode)){
                dealSpecialMoudles(labelIds,idMap);
            }
            tasks.add(new ModuleStatTask(elasticQueryDao,idMap));
        }
        //
        List<LabelGroupModule> updatedGroupModule = Lists.newArrayList();
        try {
            List<Future<Map<String, Object>>> futures=exec.invokeAll(tasks);
            ObjectMapper objectMapper = new ObjectMapper();
            for (Future<Map<String, Object>> future : futures) {
                try {
                    final Map<String, Object> dataMap = future.get();
                    for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                        String moduleCode = entry.getKey();
                        Object data = entry.getValue();
                        if (data == null) {
                            continue;
                        }
                        LabelGroupModule groupModule = moduleDataMap.get(moduleCode);
                        if (groupModule == null) continue;
                        groupModule.setStatus(1)
                                .setStaticVal(objectMapper.writeValueAsString(data));
                        updatedGroupModule.add(groupModule);
                    }
                }catch (ExecutionException|InterruptedException e) {
                    future.cancel(true);
                    logger.error("calPopulationModule==>ExecutionException  ex:", e.getCause());
                } catch (JsonProcessingException e) {
                    logger.error("calPopulationModule==>JsonProcessingException  ex:", e);
                }
            }
            //更新数据
            for (LabelGroupModule labelGroupModule : updatedGroupModule) {
                labelSysDao.updateLabelGroupModule(labelGroupModule);
            }
        } catch (InterruptedException e) {
            logger.error("calPopulationModule==>ex:{}", e);
//          Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return;
    }

    /**
     *
     * @desc先删除后插入人群模块统计信息
     * @param groupId
     * @param labelGroupModules
     * @return void
     */
    @Transactional(rollbackFor = {Exception.class,RuntimeException.class})
    public void deleteThenInsertLabelGroupModule(Long groupId, List<LabelGroupModule> labelGroupModules) {
        //入库前删除所有数据
        labelSysDao.deleteGroupModuleByGroupId(groupId);
        //插入模块数据,入库
        for (LabelGroupModule labelGroupModule : labelGroupModules) {
            labelSysDao.insertLabelGroupModule(labelGroupModule);
        }
    }

    /**
     * @param labelIds
     * @return java.lang.StringBuilder
     * @desc 通过标签ID查找相关的where表达式
     */
    private StringBuilder concatLabelWhereSql(String labelIds) {
        StringBuilder whereExpress = new StringBuilder();
        if (StringUtils.isNotBlank(labelIds)) {
            String[] idStrs = labelIds.split(STR_COMMA);
            HashSet<String> hashSet = Sets.newHashSet();
            for(String labelId : idStrs){//涉及到自定义标签，则标签Id去重
                try {
                    LabelInfo labelInfo = labelInfoDao.queryById(Long.valueOf(labelId));
                    if(null != labelInfo){
                        if(StringUtils.isNotBlank(labelInfo.getLabelIds())){
                            hashSet.addAll(Arrays.asList(labelInfo.getLabelIds().split(",")));
                        }else{
                            hashSet.add(labelId);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            List<Long> ids = Lists.newArrayList();
            for(String labelId : hashSet){
                ids.add(Long.parseLong(labelId));
            }
            /*for (String idStr : idStrs) {
                ids.add(Long.parseLong(idStr));
            }*/
            LabelQryVo labelQryVo = new LabelQryVo();
            labelQryVo.setLabelIds(ids);
            List<LabelInfo> labelInfoList = labelInfoDao.selectLabelInfos(labelQryVo);
            //拼接条件语句
            if (CollectionUtils.isNotEmpty(labelInfoList)) {
                for (LabelInfo labelInfo : labelInfoList) {
                    String labelFlag = labelInfo.getLabelFlag();
                    if (StringUtils.isNotBlank(labelFlag)) {
                        labelFlag = labelFlag.trim();
                        if (whereExpress.length() == 0) {
                            whereExpress.append(labelFlag).append("\t");
                        } else {
                            //                            whereExpress.append(" or ( ").append(labelFlag).append
                            // (" ) \t");
                            whereExpress.append(" and ( ").append(labelFlag).append(" ) \t");
                        }
                    }
                }
            }
        }
        return whereExpress;
    }

    /**
     * 处理特殊的模块，比如20，21
     * @param labelIds
     * @param idMap
     */
    private void dealSpecialMoudles(String labelIds, Map<String, String> idMap) {
        if (StringUtils.isNotBlank(labelIds)) {
            String[] idStrs = labelIds.split(STR_COMMA);
            List<Long> ids = Lists.newArrayList();
            for (String idStr : idStrs) {
                ids.add(Long.parseLong(idStr));
            }
            LabelQryVo labelQryVo = new LabelQryVo();
            labelQryVo.setLabelIds(ids);
            List<LabelInfo> labelInfoList = labelInfoDao.selectLabelInfos(labelQryVo);
            //拼接条件语句
            if (org.apache.commons.collections.CollectionUtils.isNotEmpty(labelInfoList)) {
                for (LabelInfo labelInfo : labelInfoList) {
                    String labelFlag = labelInfo.getLabelFlag();
                    String labelName = labelInfo.getLabelName();
                    if (StringUtils.isNotBlank(labelFlag)) {
                        labelFlag = labelFlag.trim();
                        if("location_class_id.keyword in ('1','2')".equals(labelFlag)){
                            idMap.put("M0021Ids1","1,2");
                        }
                        if("location_class_id.keyword in ('3','4')".equals(labelFlag)){
                            idMap.put("M0021Ids3","3");
                            idMap.put("M0021Ids4","4");
                        }
                        if("location_class_id.keyword in ('5','6')".equals(labelFlag)){
                            idMap.put("M0021Ids5","5,6,,-998");
                        }
                        if("gender.keyword = '0'".equals(labelFlag)){
                            idMap.put("M00200","0");
                        }
                        if("gender.keyword = '1'".equals(labelFlag)){
                            idMap.put("M00201","1");
                        }
                    }
                }
            }
        }
    }
}
