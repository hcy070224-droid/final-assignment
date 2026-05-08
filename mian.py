

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

#1.出行需求时间规律
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

#2.区域热度分析
#上车
top_pickup = df['PULocationID'].value_counts().head(10)#统计数据
plt.figure(figsize=(10,5))
#画图
top_pickup.plot(kind='bar')
plt.xlabel('Pickup Location')
plt.ylabel('Trips')
plt.title('Top 10 Pickup Locations')

plt.savefig('top_Pickup.png')
plt.show()
plt.close()
#下车
top_dropoff = df['DOLocationID'].value_counts().head(10)
top_dropoff.plot(kind='bar')
plt.xlabel('Dropoff Location')
plt.ylabel('Trips')
plt.title('Top 10 Dropoff Locations')

plt.savefig('top_Dropoff.png')
plt.show()
plt.close()

# 3. 车费影响因素分析
# 距离与车费关系
sample_df = df.sample(10000)#随机采样
#去除异常值
sample_df = sample_df[
    (sample_df['trip_distance'] > 0) &
    (sample_df['trip_distance'] < 30) &
    (sample_df['fare_amount'] > 0) &
    (sample_df['fare_amount'] < 100)
]
#创建画布
plt.figure(figsize=(8,5))

plt.scatter(#散点图
    sample_df['trip_distance'],
    sample_df['fare_amount'],
    alpha=0.3,
    s=3
)
#设置标签
plt.xlabel('Trip Distance')
plt.ylabel('Fare Amount')
plt.title('Distance vs Fare')
plt.savefig('fare_distance.png')#保存图片
plt.show()
plt.close()
#时间段与车费
hour_fare = df.groupby('hour')['fare_amount'].mean()
plt.figure(figsize=(10,5))
#折线图
plt.plot(
    hour_fare.index,
    hour_fare.values
)

plt.xlabel('Hour')
plt.ylabel('Average Fare')
plt.title('Average Fare by Hour')

plt.savefig('hour_fare.png')
plt.show()
plt.close()
#乘车人数与车费
passenger_fare = df.groupby(
    'passenger_count'
)['fare_amount'].mean()#统计平均车费
plt.figure(figsize=(8,5))
#柱状图
plt.bar(
    passenger_fare.index.astype(str),
    passenger_fare.values
)

plt.xlabel('Passenger Count')
plt.ylabel('Average Fare')
plt.title('Passenger Count vs Average Fare')

plt.savefig('passenger_fare.png')
plt.show()
plt.close()
#自选：不同时间段平均速度变化
#研究一天中，什么时候车速最快/最慢
df['speed'] = df['trip_distance'] / (
    df['duration'] / 60
)
#去除异常速度
df = df[
    (df['speed'] > 0) &
    (df['speed'] < 100)
]
speed_by_hour = df.groupby('hour')[
    'speed'
].mean()#统计
plt.figure(figsize=(10,5))

plt.plot(
    speed_by_hour.index,
    speed_by_hour.values
)

plt.xlabel('Hour')
plt.ylabel('Average Speed')
plt.title('Average Speed by Hour')

plt.savefig('speed_hour.png')
plt.show()
plt.close()

#M3:预测模型
import tensorflow as tf
#1.构建数据
#提取时间特征
df['pickup_datetime'] = pd.to_datetime(
    df['tpep_pickup_datetime']
)

df['hour'] = df['pickup_datetime'].dt.hour

df['weekday'] = df['pickup_datetime'].dt.weekday
#统计
demand_df = df.groupby(
    ['PULocationID', 'hour', 'weekday']
).size().reset_index(name='demand')
#设置x和y
X = demand_df[
    ['PULocationID', 'hour', 'weekday']
]
y = demand_df['demand']
X = pd.get_dummies(
    X,
    columns=['PULocationID']
)

X = X.astype('float32')#把区域编号变成模型能理解的数字特征
#2.划分训练测试集
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)#8：2
#3.神经网络
#建立模型
from keras.models import Sequential
from keras.layers import Dense

model = Sequential([
    Dense(64, activation='relu'),
    Dense(32, activation='relu'),
    Dense(1)
])
#编译
model.compile(
    optimizer='adam',
    loss='mse',
    metrics=['mae']
)
#训练
#history 用来画曲线
#validation_split=0.2用于验证集
history = model.fit(
    X_train,
    y_train,
    epochs=20,
    batch_size=32,
    validation_split=0.2
)
#画loss曲线
plt.figure(figsize=(8,5))

plt.plot(history.history['loss'])
plt.plot(history.history['val_loss'])

plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.legend(['Train', 'Validation'])
plt.show()
#测试集预测
y_pred_nn = model.predict(X_test)
#计算MAE与RMSE
#MAE
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
import numpy as np

mae_nn = mean_absolute_error(
    y_test,
    y_pred_nn
)
#RMSE
rmse_nn = np.sqrt(
    mean_squared_error(
        y_test,
        y_pred_nn
    )
)
#4.随机森林进行对比
#训练模型
from sklearn.ensemble import RandomForestRegressor

rf = RandomForestRegressor(
    n_estimators=100,
    random_state=42
)

rf.fit(X_train, y_train)
y_pred_rf = rf.predict(X_test)#预测
#指标计算
mae_rf = mean_absolute_error(
    y_test,
    y_pred_rf
)

rmse_rf = np.sqrt(
    mean_squared_error(
        y_test,
        y_pred_rf
    )
)
#对比图
models = ['RF', 'NN']
mae_values = [mae_rf, mae_nn]
plt.bar(models, mae_values)
plt.ylabel('MAE')
plt.show()
models = ['Random Forest', 'Neural Network']
rmse_values = [rmse_rf, rmse_nn]
plt.figure(figsize=(6,5))
plt.bar(models, rmse_values)
plt.ylabel('RMSE')
plt.title('RMSE')
plt.show()