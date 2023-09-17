from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from pyspark.sql import functions as F
import numpy

from cn.test.Mining.testUse.rule_base_abstract import rule_base


class customerValue_mode(rule_base):
    def get_id(self):
        return 37

    def compute(self, es_data_df: DataFrame, mysql5: DataFrame):
        # es_data_df.show()
        # +----------+--------+-----------+--------------------+
        # |finishtime|memberid|orderamount|             ordersn|
        # +----------+--------+-----------+--------------------+
        # |1563941154|13822773|        1.0|jd_15062716252125282|
        # |1564117286|13822773|     3699.0|jd_15062720080896457|
        # |1563746025| 4035291|     2699.0|jd_15062817103347299|
        # todo : 计算 R M F
        es_rmf: DataFrame = (es_data_df
                             .where(es_data_df['memberid'].isNotNull())
                             .groupBy(es_data_df['memberid'].alias('userId'))
                             .agg(F.datediff(F.date_sub(F.current_date(), 1049),
                                             F.from_unixtime(F.max(es_data_df['finishtime']))[0:10]).alias('recently'),
                                  F.sum(es_data_df['orderamount']).alias('monetary'),
                                  F.count(es_data_df['ordersn']).alias('frequency')))
        # es_rmf.show()
        # +---------+--------+------------------+---------+
        # |   userId|recently|          monetary|frequency|
        # +---------+--------+------------------+---------+
        # | 13823431|       2|180858.21996093728|      122|
        # |       29|       2|249794.74002075195|      137|
        # |  4034729|       2|237923.07005859353|      126|
        # |  4034199|       2|244163.94004394487|      115|
        recently = (F.when(es_rmf['recently'].between(1, 3), 5)
                    .when(es_rmf['recently'].between(4, 6), 4)
                    .when(es_rmf['recently'].between(7, 9), 3)
                    .when(es_rmf['recently'].between(10, 15), 2)
                    .when(es_rmf['recently'] > 16, 1).otherwise(0))
        frequency = (F.when(es_rmf['frequency'].between(1, 49), 1)
                     .when(es_rmf['frequency'].between(50, 99), 2)
                     .when(es_rmf['frequency'].between(100, 149), 3)
                     .when(es_rmf['frequency'].between(150, 199), 4)
                     .when(es_rmf['frequency'] > 200, 5).otherwise(0))
        monetary = (F.when(es_rmf['monetary'] < 10000, 1)
                    .when(es_rmf['monetary'].between(10000, 49999), 2)
                    .when(es_rmf['monetary'].between(50000, 99999), 3)
                    .when(es_rmf['monetary'].between(100000, 199999), 4)
                    .when(es_rmf['monetary'] > 200000, 5).otherwise(0))
        es_rmf_1 = es_rmf.select(es_rmf['userId'],
                                 recently.alias('recently'),
                                 frequency.alias('frequency'),
                                 monetary.alias('monetary'))
        # es_rmf_1.show()
        # +---------+--------+---------+--------+
        # |   userId|recently|frequency|monetary|
        # +---------+--------+---------+--------+
        # | 13823431|       5|        3|       4|
        # |       29|       5|        3|       5|
        # |  4034729|       5|        3|       5|
        v_model = VectorAssembler().setInputCols(['recently', 'monetary', 'frequency']).setOutputCol('featuresCol')
        es_vector: DataFrame = v_model.transform(es_rmf_1)
        # es_vector.show()
        # +---------+--------+---------+--------+-------------+
        # |   userId|recently|frequency|monetary|  featuresCol|
        # +---------+--------+---------+--------+-------------+
        # | 13823431|       5|        3|       4|[5.0,4.0,3.0]|
        # |       29|       5|        3|       5|[5.0,5.0,3.0]|
        kmeans: KMeans = (KMeans()
                          .setFeaturesCol('featuresCol')
                          .setPredictionCol('prediction')
                          .setK(7)
                          .setInitMode('k-means||')
                          .setDistanceMeasure('euclidean')
                          .setMaxIter(20)
                          .setSeed(1))
        k_model: KMeansModel = kmeans.fit(es_vector)
        es_prediction: DataFrame = k_model.transform(es_vector)
        # es_prediction.show()
        # +---------+--------+---------+--------+-------------+----------+
        # |   userId|recently|frequency|monetary|  featuresCol|prediction|
        # +---------+--------+---------+--------+-------------+----------+
        # | 13823431|       5|        3|       4|[5.0,4.0,3.0]|         5|
        # |       29|       5|        3|       5|[5.0,5.0,3.0]|         0|
        # print(k_model.clusterCenters())
        # [array([4.9944212, 5., 2.9958159]),
        # array([0., 0., 0.]),
        # array([0.5, 1. , 1. ]),
        # array([0., 0., 5.]),
        # array([4.25, 1.66666667, 1.]),
        # array([5., 4., 2.96666667]),
        # array([5. 5. 4.03846154])]
        clusterCenters = [numpy.sum(i) for i in k_model.clusterCenters()]
        # print(clusterCenters)
        # [12.990237099023709, 0.0, 2.5, 5.0, 6.916666666666666, 11.966666666666667, 14.038461538461538]
        clusterCenters_dict = {}
        for i in range(len(clusterCenters)):
            clusterCenters_dict[i] = clusterCenters[i]
        # print(clusterCenters_dict)
        # {0: 12.990237099023709, 1: 0.0, 2: 2.5, 3: 5.0, 4: 6.916666666666666, 5: 11.966666666666667, 6: 14.038461538461538}
        # todo 方法一 纯python转换  自己想出来的
        clusterCenters_dict_sorted = dict(sorted(clusterCenters_dict.items(), key=lambda x: x[1], reverse=True))
        # print(clusterCenters_dict_sorted)
        # {6: 14.038461538461538, 0: 12.990237099023709, 5: 11.966666666666667, 4: 6.916666666666666, 3: 5.0, 2: 2.5, 1: 0.0}

        # todo 处理 5及标签
        # mysql5.show()
        # +------+----+
        # |tagsId|rule|
        # +------+----+
        # |    38|   1|
        # |    39|   2|
        # |    40|   3|
        # |    41|   4|
        # |    42|   5|
        # |    43|   6|
        # |    44|   7|
        # +------+----+
        # todo : 和上面的  方法一  配套
        mysql5_dict = mysql5.orderBy(mysql5['rule'].asc()).rdd.collectAsMap()
        # print(mysql5_dict)
        # {38: '1', 39: '2', 40: '3', 41: '4', 42: '5', 43: '6', 44: '7'}
        center_tagsId = dict(zip(clusterCenters_dict_sorted, mysql5_dict))
        # print(center_tagsId)
        # {6: 38, 0: 39, 5: 40, 4: 41, 3: 42, 2: 43, 1: 44}
        # todo ： 和上面的  方法一  配套  打标签
        prediction2tagsId = F.udf(lambda prediction: center_tagsId.get(prediction), StringType())
        re_new: DataFrame = es_prediction.select(es_prediction['userId'],
                                                 prediction2tagsId(es_prediction['prediction']).alias('tagsId'))
        # re_new.show()
        # +---------+------+
        # |   userId|tagsId|
        # +---------+------+
        # | 13823431|    40|
        # |       29|    39|
        return re_new


if __name__ == '__main__':
    re_customerValue_mode = customerValue_mode()
    re_customerValue_mode.execute()
