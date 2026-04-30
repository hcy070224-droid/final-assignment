
#M1 数据处理
#加载数据
import pandas as pd
import pyarrow.parquet as pq

table = pq.read_table('yellow_tripdata_2023-01.parquet')
df = table.to_pandas()

#数据质量报告
missing_rate = df.isnull().mean()#缺失率
df[df['trip_distance'] <= 0]
df[df['fare_amount'] <= 0]
df[df['passenger_count'] <= 0]#异常检查

#清洗数据
df = df[df['trip_distance'] > 0]#删除异常值
df = df[df['fare_amount'] > 0]
df = df[df['passenger_count'] > 0]
df = df.dropna()#删除缺失严重的数据

#特征提取
df['pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['hour'] = df['pickup_datetime'].dt.hour#提取小时、星期特征
df['weekday'] = df['pickup_datetime'].dt.weekday
df['is_peak'] = df['hour'].apply(lambda x: 1 if (7<=x<=9 or 17<=x<=19) else 0)#是否高峰
#自定义特征
#补充1：工作日or周末
df['is_weekend'] = df['weekday'].apply(lambda x: 1 if x >= 5 else 0)
#补充2：行程时长
df['duration'] = (pd.to_datetime(df['tpep_dropoff_datetime']) -pd.to_datetime(df['tpep_pickup_datetime'])).dt.total_seconds() / 60
#补充3：单位距离价格
df['fare_per_km'] = df['fare_amount'] / df['trip_distance']
