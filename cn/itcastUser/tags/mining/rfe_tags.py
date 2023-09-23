#!/usr/bin/env python
__coding__ = "utf-8"

import os

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template
from cn.itcastUser.utils.HDFSUtils import HdfsUtil

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	rfe_tags
   Author      :	'钱有霄'
   Date	       :	2023/9/21
-------------------------------------------------
"""


class RFETags(Tags_basic_Template):
    def get_level4_id(self):
        return 45

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        recencyStr = "recency"
        frequencyStr = "frequency"
        engagementsStr = "engagements"
        featureStr = "feature"
        predictStr = "predict"

        process_df: DataFrame = es_df.groupBy(es_df['global_user_id'])\
            .agg(F.datediff(F.date_sub(F.current_date(), 1428),
                            F.substring(F.max(es_df['log_time']), 1, 10)).alias(recencyStr),
                 F.count(es_df['loc_url']).alias(frequencyStr),
                 F.countDistinct(es_df['loc_url']).alias(engagementsStr))
        # process_df.show()

        r_col = (F.when(process_df[recencyStr].between(0, 15), 5)
                 .when(process_df[recencyStr].between(16, 30), 4)
                 .when(process_df[recencyStr].between(31, 80), 3)
                 .when(process_df[recencyStr].between(81, 95), 2)
                 .when(process_df[recencyStr] > 95, 1)
                 .otherwise(0).alias(recencyStr)
                 )

        f_col = (F.when(process_df[frequencyStr] > 560, 5)
                 .when(process_df[frequencyStr].between(530, 560), 4)
                 .when(process_df[frequencyStr].between(400, 529), 3)
                 .when(process_df[frequencyStr].between(250, 399), 2)
                 .when(process_df[frequencyStr] < 250, 1)
                 .otherwise(0).alias(frequencyStr))

        e_col = (F.when(process_df[engagementsStr] > 260, 5)
                 .when(process_df[engagementsStr].between(240, 260), 4)
                 .when(process_df[engagementsStr].between(220, 239), 3)
                 .when(process_df[engagementsStr].between(50, 219), 2)
                 .when(process_df[engagementsStr] < 49, 1)
                 .otherwise(0).alias(engagementsStr))
        project_df = process_df.select(process_df['global_user_id'].alias('userId'), r_col, f_col, e_col)
        # project_df.show()
        vector_df: DataFrame = VectorAssembler().\
            setInputCols([recencyStr, frequencyStr, engagementsStr])\
            .setOutputCol(featureStr).transform(project_df)
        # vector_df.show()

        hdfs_util = HdfsUtil()
        model_path = '/up/model/rfemodel'
        if hdfs_util.exists(model_path):
            kmeans_model = KMeansModel.load('hdfs://up01:8020'+model_path)
        else:
            kmeans = KMeans().setFeaturesCol(featureStr).setPredictionCol(predictStr).setK(4)
            kmeans_model = kmeans.fit(vector_df)
            kmeans_model.save('hdfs://up01:8020'+model_path)

        kmeans_model_df = kmeans_model.transform(vector_df)
        # kmeans_model_df.show()

        import numpy as np
        cluster_list = [float(np.sum(x)) for x in kmeans_model.clusterCenters()]
        # print(cluster_list)
        cluster_dict = {}
        for i in range(len(cluster_list)):
            cluster_dict[i] = cluster_list[i]
        # print(cluster_dict)
        sort_dict = dict(sorted(cluster_dict.items(), key=lambda tuple: tuple[1], reverse=True))
        # print(sort_dict)
        mysql_tags_level5_df_dict = mysql_tags_level5_df.rdd.collectAsMap()
        merge_dict = dict(zip(sort_dict.keys(), mysql_tags_level5_df_dict.values()))
        # print(merge_dict)

        @F.udf
        def get_tags(predict):
            return merge_dict.get(predict)

        new_tags = kmeans_model_df.select(kmeans_model_df['userId'], get_tags(predictStr).alias('tagsId'))
        print(new_tags.count())


if __name__ == '__main__':
    rfe_tags = RFETags()
    rfe_tags.executor()
