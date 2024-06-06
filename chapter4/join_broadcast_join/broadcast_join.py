from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession \
    .builder \
    .appName("Broadcast Join Demo") \
    .master("local[3]") \
    .config("spark.sql.shuffle.partitions", 3) \
    .config("spark.sql.adaptive.enabled", False) \
    .getOrCreate()

# 2개의 df 로드
df_large = spark.read.json("large_data/")
df_small = spark.read.json("small_data/")

# join 구문: id 컬럼을 기준으로 조인
join_expr = df_large.id == df_small.id
join_df = df_large.join(broadcast(df_small), join_expr, "inner")
# broadcast join은 큰 데이터프레임의 파티션에 작은 데이터를 보내서 조인을 수행함

# vs 기본 join
# join_df = df_large.join(df_small, join_expr, "inner")

join_df.collect()
input("Waiting ...")
# web UI를 살려두기 위해 input 추가

spark.stop()
