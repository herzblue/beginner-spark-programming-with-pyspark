from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession \
    .builder \
    .appName("Shuffle Join Demo") \
    .master("local[3]") \
    .config("spark.sql.shuffle.partitions", 3) \
    .config("spark.sql.adaptive.enabled", False) \
    .getOrCreate()

# 2개의 df 로드
df_large = spark.read.json("large_data/")
df_small = spark.read.json("small_data/")

# join 구문: id 컬럼을 기준으로 조인
join_expr = df_large.id == df_small.id
join_df = df_large.join(df_small, join_expr, "inner")

join_df.collect()
input("Waiting ...")
# web UI를 살려두기 위해 input 추가

# job은 총 3개의 stage로 구성됨
# stage1: df_large의 데이터를 읽어오는 작업
# stage2: df_small의 데이터를 읽어오는 작업
# stage3: show가 실행될 때 join이 수행되는 작업

# small_data가 훨씬 작다면 기존에 사용되는 shuffle join은 낭비가 될 수 있음
# broadcast join을 사용하여 성능을 향상시킬 수 있음

spark.stop()
