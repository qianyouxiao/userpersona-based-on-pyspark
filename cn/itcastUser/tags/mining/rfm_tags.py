#!/usr/bin/env python
__coding__ = "utf-8"

import os

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template
from cn.itcastUser.utils.HDFSUtils import HdfsUtil

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	rfm_tags
   Author      :	'钱有霄'
   Date	       :	2023/9/21
-------------------------------------------------
"""


spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQL APP") \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()


class RFMTags(Tags_basic_Template):
    def get_level4_id(self):
        return 37

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        recencyStr = "recency"
        frequencyStr = "frequency"
        monetaryStr = "monetary"
        featureStr = "feature"
        predictStr = "predict"

        attribute_df = es_df.groupBy(es_df['memberid'].alias('userId')) \
            .agg(F.datediff(F.date_sub(F.current_date(), 1140),
                            F.from_unixtime(F.max(es_df['finishtime']), 'yyyy-MM-dd')).alias(recencyStr),
                 F.count(es_df['ordersn']).alias(frequencyStr), F.sum(es_df['orderamount']).alias(monetaryStr))
        # attribute_df.show()

        r_col = F.when(attribute_df[recencyStr] >= 16, 1) \
            .when(attribute_df[recencyStr] >= 10, 2) \
            .when(attribute_df[recencyStr] >= 7, 3) \
            .when(attribute_df[recencyStr] >= 4, 4) \
            .when(attribute_df[recencyStr] >= 1, 5) \
            .otherwise(0).alias(recencyStr)

        f_col = F.when((attribute_df[frequencyStr] >= 1) & (attribute_df[frequencyStr] <= 49), 1) \
            .when((attribute_df[frequencyStr] >= 50) & (attribute_df[frequencyStr] <= 99), 2) \
            .when((attribute_df[frequencyStr] >= 100) & (attribute_df[frequencyStr] <= 149), 3) \
            .when((attribute_df[frequencyStr] >= 150) & (attribute_df[frequencyStr] <= 199), 4) \
            .when((attribute_df[frequencyStr] >= 200), 5) \
            .otherwise(0) \
            .alias(frequencyStr)

        m_col = F.when((attribute_df[monetaryStr] < 100000), 1) \
            .when((attribute_df[monetaryStr] >= 100000) & (attribute_df[monetaryStr] < 350000), 2) \
            .when((attribute_df[monetaryStr] >= 350000) & (attribute_df[monetaryStr] < 500000), 3) \
            .when((attribute_df[monetaryStr] >= 500000) & (attribute_df[monetaryStr] < 850000), 4) \
            .when((attribute_df[monetaryStr] >= 850000), 5) \
            .otherwise(0) \
            .alias(monetaryStr)
        project_df = attribute_df.select(attribute_df['userId'], r_col, f_col, m_col)
        # project_df.show()

        vector_df = VectorAssembler()\
            .setInputCols([recencyStr, frequencyStr, monetaryStr])\
            .setOutputCol(featureStr).transform(project_df)
        # vector_df.show()

        model_path = "/up/model/rfmmode"
        hdfs_util = HdfsUtil()
        if hdfs_util.exists(model_path):
            kmeans_model = KMeansModel.load('hdfs://up01:8020'+model_path)
        else:
            kmeans = KMeans().setK(7).setFeaturesCol(featureStr).setPredictionCol(predictStr)
            kmeans_model = kmeans.fit(vector_df)
            kmeans_model.save('hdfs://up01:8020'+model_path)

        user_kmeans_df = kmeans_model.transform(vector_df)
        # user_kmeans_df.show()

        import numpy as np
        centers_sum = [float(np.sum(x)) for x in kmeans_model.clusterCenters()]  # 列表中存放每个聚类中心的和
        print(centers_sum)

        cluster_dict = {}
        for i in range(len(centers_sum)):
            cluster_dict[i] = centers_sum[i]
        print(cluster_dict)

        sort_dict = dict(sorted(cluster_dict.items(), key=lambda tuple: tuple[1], reverse=True))
        print(sort_dict)

        mysql_tags_level5_df_dict = mysql_tags_level5_df.rdd.collectAsMap()
        print(mysql_tags_level5_df_dict)

        tags_dict = dict(zip(sort_dict.keys(), mysql_tags_level5_df_dict.values()))
        print(f'tags_dict：{tags_dict}')

        @F.udf
        def get_tags(predict):
            return tags_dict.get(predict)

        new_tags = user_kmeans_df.select(user_kmeans_df['userId'], get_tags(user_kmeans_df['predict']).alias('tagsId'))
        # new_tags.show(n=1000)
        # return new_tags
        print(new_tags.count())


if __name__ == '__main__':
    rfm_tags = RFMTags()
    rfm_tags.executor()
