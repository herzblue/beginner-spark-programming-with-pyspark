from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("SparkSchemaDemo") \
    .config("spark.sql.adaptive.enabled", False) \
    .getOrCreate()

# spark.sql.adaptive.enabled: False - 최적화를 위한 쿼리 실행 계획을 동적으로 조정하는 기능을 활성화하거나 비활성화합니다.

spark.conf.set("spark.sql.shuffle.partitions", 3)
# spark.sql.shuffle.partitions: 3 - 셔플이 발생할 때 사용할 파티션 수를 설정합니다.

# load with schema of one column named value
df = spark.read.text("shakespeare.txt")
df.printSchema()

df_count = df.select(explode(split(df.value, " ")).alias("word")).groupBy("word").count()

# schema가 없어 처음 기본으로 설정된 값이 value로 설정됨
# value를 기준으로 " "로 split하여 단어별 list로 만듦
# explode를 통해 단어로 분리 = 별개의 row로 만듦
# alias로 새로운 컬럼이름을 word로 설정
# groupBy를 통해 word를 기준으로 그룹화

df_count.show()
# 이전까지는 action없이 transformation만 수행했기 때문에 실행되지 않음
# show를 통해 action을 수행하여 결과를 출력 = 총 1개의 job이 실행됨
# job은 여러개의 stage로 구성되며, stage는 여러개의 task로 구성됨
# lazy execution: transformation은 action이 호출될 때까지 실행되지 않음

input("stopping ...")
