#!/usr/bin/env python
__coding__ = "utf-8"

import os

from pyspark.ml.clustering import KMeansModel, KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template
from cn.itcastUser.utils.HDFSUtils import HdfsUtil

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	psm_tags
   Author      :	'钱有霄'
   Date	       :	2023/9/21
-------------------------------------------------
"""


class PSMTags(Tags_basic_Template):
    def get_level4_id(self):
        return 50

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        psmScoreStr = "psm"
        featureStr = "feature"
        predictStr = "predict"

        process_df: DataFrame = es_df.groupBy(es_df['memberid'].alias('userId'))\
            .agg(F.sum(F.when(es_df['couponcodevalue'] > 0, 1).otherwise(0)).alias('tdon'),
                 F.count(es_df['ordersn']).alias('ton'),
                 F.sum(es_df['couponcodevalue']).alias('tda'),
                 F.sum(es_df['orderamount']).alias('tra'))

        project_df: DataFrame = process_df.select(process_df['userId'], ((process_df["tdon"] / process_df["ton"] ) +
             ((process_df["tda"] / process_df["tdon"]) / (process_df["tra"] / process_df["ton"])) +
             (process_df["tda"] / process_df["tra"])).alias(psmScoreStr)).where(f"{psmScoreStr} is not null")
        # project_df.show()

        vector_df: DataFrame = VectorAssembler()\
            .setInputCols([psmScoreStr])\
            .setOutputCol(featureStr)\
            .transform(project_df)
        # vector_df.show()

        model_path = '/up/model/psmmodel'
        hdfs_util = HdfsUtil()
        if hdfs_util.exists(model_path):
            kmeans_model = KMeansModel.load('hdfs://up01:8020'+model_path)
        else:
            kmeans = KMeans().setFeaturesCol(value=featureStr).setPredictionCol(predictStr).setK(5)
            kmeans_model = kmeans.fit(vector_df)
            kmeans_model.save('hdfs://up01:8020'+model_path)

        kmeans_model_df = kmeans_model.transform(vector_df)
        # kmeans_model_df.show()

        import numpy as np
        cluster_list = [float(np.sum(x)) for x in kmeans_model.clusterCenters()]
        print(cluster_list)

        cluster_dict = {}
        for i in range(len(cluster_list)):
            cluster_dict[i] = cluster_list[i]
        print(cluster_dict)

        sort_dict = dict(sorted(cluster_dict.items(), key=lambda tuple: tuple[1], reverse=True))
        print(sort_dict)

        mysql_tags_level5_df_dict = mysql_tags_level5_df.rdd.collectAsMap()
        print(mysql_tags_level5_df_dict)

        merge_dict = dict(zip(sort_dict.keys(), mysql_tags_level5_df_dict.values()))
        print(merge_dict)

        @F.udf
        def get_tags(predict):
            return merge_dict.get(predict)

        new_tags = kmeans_model_df.select(kmeans_model_df['userId'], get_tags(predictStr).alias('tagsId'))
        print(new_tags.count())


if __name__ == '__main__':
    psm_tags = PSMTags()
    psm_tags.executor()
