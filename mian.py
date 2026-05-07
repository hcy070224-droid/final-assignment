

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
df = df[df['fare_amount'] > 0]#原因：出租车行程距离、费用、旅客数不可能小于等于0，这类数据属于异常值，会影响后续分析
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


#M2:分析可视化
import matplotlib.pyplot as plt

#图1：出行需求时间规律
#按小时
hourly_orders = df.groupby('hour').size() #按小时统计订单
plt.figure(figsize=(10,5))
hourly_orders.plot()

plt.xlabel('Hour')#完善图像名称
plt.ylabel('Number of Trips')
plt.title('Hourly Taxi Demand')

plt.savefig('hourly_demand.png')#保存
plt.show()
plt.close()
#按周末、工作日分
week_data = df.groupby('is_weekend').size()#统计订单
plt.figure(figsize=(6,5))

plt.bar(['Weekday', 'Weekend'],
        week_data.values)

plt.xlabel('Type')
plt.ylabel('Number of Trips')
plt.title('Weekday vs Weekend Taxi Demand')

plt.savefig('weekend_weekday.png')
plt.show()
plt.close()