

#M1 数据处理
#加载数据
#导入数据处理库
import pandas as pd
import pyarrow.parquet as pq#使用parquet：相比csv读取速度更快、占用空间更小

table = pq.read_table('data/yellow_tripdata_2023-01.parquet')
df = table.to_pandas()#转换为pandasDataFrame方便后续处理

#数据质量报告
missing_rate = df.isnull().mean()#缺失率#用于评估数据完整性
df[df['trip_distance'] <= 0]#检查异常行程距离
df[df['fare_amount'] <= 0]#检查异常车费
df[df['passenger_count'] <= 0]#检查异常乘客数

#清洗数据#原因：出租车行程距离、费用、旅客数不可能小于等于0，这类数据属于异常值，会影响后续分析
df = df[df['trip_distance'] > 0]#删除异常距离值
df = df[df['fare_amount'] > 0]#删除异常车费数值 
df = df[df['passenger_count'] > 0]#删除异常乘客数值
df = df.dropna()#删除缺失严重的数据

#特征提取
df['pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])#将时间字段转换为datetime类型
df['hour'] = df['pickup_datetime'].dt.hour#提取小时特征
df['weekday'] = df['pickup_datetime'].dt.weekday#提取星期特征
df['is_peak'] = df['hour'].apply(lambda x: 1 if (7<=x<=9 or 17<=x<=19) else 0)#提取是否高峰
#自定义特征
#补充1：工作日/周末
df['is_weekend'] = df['weekday'].apply(lambda x: 1 if x >= 5 else 0)#构造是否周末特征
#补充2：行程时长
df['duration'] = (pd.to_datetime(df['tpep_dropoff_datetime']) -pd.to_datetime(df['tpep_pickup_datetime'])).dt.total_seconds() / 60#计算行程时长
#补充3：单位距离价格
df['fare_per_km'] = df['fare_amount'] / df['trip_distance']#计算单位距离价格


#M2:分析可视化
import matplotlib.pyplot as plt#导入matplotlib库

#1.出行需求时间规律
#按小时
hourly_orders = df.groupby('hour').size() #按小时统计订单
plt.figure(figsize=(10,5))#设置画布大小
hourly_orders.plot()

plt.xlabel('Hour')#设置横纵轴名称
plt.ylabel('Number of Trips')
plt.title('Hourly Taxi Demand')#标题名称

plt.savefig('outputs/hourly_demand.png')#保存
plt.close()#关闭图像，避免多个图像重叠
#按周末、工作日分
week_data = df.groupby('is_weekend').size()#统计订单
plt.figure(figsize=(6,5))#设置画布大小

plt.bar(['Weekday', 'Weekend'],#选择使用柱状图
        week_data.values)

plt.xlabel('Type')#设置轴名称和标题
plt.ylabel('Number of Trips')
plt.title('Weekday vs Weekend Taxi Demand')

plt.savefig('outputs/weekend_weekday.png')#保存
plt.close()#关闭

#2.区域热度分析
#上车
top_pickup = df['PULocationID'].value_counts().head(10)#统计订单量最多的前10个上车区域
plt.figure(figsize=(10,5))#创建画布
#画图
top_pickup.plot(kind='bar')#绘制柱状图
plt.xlabel('Pickup Location')#设置轴标签
plt.ylabel('Trips')
plt.title('Top 10 Pickup Locations')

plt.savefig('outputs/top_Pickup.png')
plt.close()
#下车
top_dropoff = df['DOLocationID'].value_counts().head(10)#统计订单量最多的前10个下车区域
top_dropoff.plot(kind='bar')#选择柱状图
plt.xlabel('Dropoff Location')#设置轴标签
plt.ylabel('Trips')
plt.title('Top 10 Dropoff Locations')

plt.savefig('outputs/top_Dropoff.png')
plt.close()

# 3. 车费影响因素分析
# 距离与车费关系
sample_df = df.sample(10000)#随机采样#原因：原始数据过大，直接绘制导致绘制速度慢图像重叠严重
#去除极端异常值
sample_df = sample_df[
    (sample_df['trip_distance'] > 0) &
    (sample_df['trip_distance'] < 30) &
    (sample_df['fare_amount'] > 0) &
    (sample_df['fare_amount'] < 100)
]
#创建画布
plt.figure(figsize=(8,5))#绘制画布

plt.scatter(#散点图
    sample_df['trip_distance'],
    sample_df['fare_amount'],
    alpha=0.3,#alpha设置透明度
    s=3#s=3减小点大小
)
#设置标签
plt.xlabel('Trip Distance')
plt.ylabel('Fare Amount')
plt.title('Distance vs Fare')
plt.savefig('outputs/fare_distance.png')#保存图片
plt.close()
#时间段与车费
hour_fare = df.groupby('hour')['fare_amount'].mean()#用于分析不同时间段的收费变化规律
plt.figure(figsize=(10,5))
#折线图
plt.plot(#选择折线图
    hour_fare.index,
    hour_fare.values
)

plt.xlabel('Hour')
plt.ylabel('Average Fare')
plt.title('Average Fare by Hour')

plt.savefig('outputs/hour_fare.png')
plt.close()
#乘车人数与车费
passenger_fare = df.groupby(#按乘客人数统计平均车费#分析多人乘车是否会影响车费水
    'passenger_count'
)['fare_amount'].mean()#统计平均车费
plt.figure(figsize=(8,5))#创建画布
#柱状图
plt.bar(
    passenger_fare.index.astype(str),
    passenger_fare.values
)

plt.xlabel('Passenger Count')
plt.ylabel('Average Fare')
plt.title('Passenger Count vs Average Fare')

plt.savefig('outputs/passenger_fare.png')
plt.close()
#自选：不同时间段平均速度变化
#研究一天中，什么时候车速最快/最慢
df['speed'] = df['trip_distance'] / (#计算出租车平均速度#原因：速度能够反映交通拥堵情况
    df['duration'] / 60
)
#去除异常速度
df = df[
    (df['speed'] > 0) &
    (df['speed'] < 100)
]
speed_by_hour = df.groupby('hour')[#按小时统计平均速度
    'speed'
].mean()#统计
plt.figure(figsize=(10,5))

plt.plot(#选择折线图
    speed_by_hour.index,
    speed_by_hour.values
)

plt.xlabel('Hour')
plt.ylabel('Average Speed')
plt.title('Average Speed by Hour')

plt.savefig('outputs/speed_hour.png')
plt.close()

#M3:预测模型
import tensorflow as tf#导入tensorflow#用于构建神经网络模型
#1.构建数据
#提取时间特征
df['pickup_datetime'] = pd.to_datetime(#将上车时间转换为datetime格式
    df['tpep_pickup_datetime']
)

df['hour'] = df['pickup_datetime'].dt.hour#提取小时统计

df['weekday'] = df['pickup_datetime'].dt.weekday#提取星期统计
#统计
demand_df = df.groupby(#按区域、小时、星期统计订单量
    ['PULocationID', 'hour', 'weekday']
).size().reset_index(name='demand')
#设置x和y
X = demand_df[#设置输入特征X（包括上车区域、小时、星期）
    ['PULocationID', 'hour', 'weekday']
]
y = demand_df['demand']#设置预测目标y为订单需求量
X = pd.get_dummies(#对区域编号进行One-Hot编码#原因：区域ID本身没有大小关系，不能直接作为普通数字输入模型
    X,
    columns=['PULocationID']
)

X = X.astype('float32')#把区域编号变成模型能理解的数字特征#转换为float32类型#原因：提高TensorFlow兼容性与训练效率
#2.划分训练测试集
from sklearn.model_selection import train_test_split#导入数据集划分工具
X_train, X_test, y_train, y_test = train_test_split(#按8:2划分训练集与测试集
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
    Dense(64, activation='relu'),#第一隐藏层#relu激活函数可以增强非线性学习能力
    Dense(32, activation='relu'),#第二隐藏层#进一步提取特征
    Dense(1)#输出层#1个输出值#用于预测订单需求量
])
#编译模型
model.compile(
    optimizer='adam',#adam优化器训练速度较快
    loss='mse',
    metrics=['mae']#用平均绝对误差评估模型效果
)
#训练模型
history = model.fit(#history用来画曲线
    X_train,
    y_train,
    epochs=20,#模型迭代20次
    batch_size=32,#每次训练32条数据
    validation_split=0.2#validation_split=0.2用于验证集
)
#画loss曲线
plt.figure(figsize=(8,5))#创建画布
plt.plot(history.history['loss'])#绘制
plt.plot(history.history['val_loss'])#用于观察模型泛化能力

plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.legend(['Train', 'Validation'])#添加图例
plt.savefig('outputs/loss_curve.png')
plt.close()
#测试集预测
y_pred_nn = model.predict(X_test)#使用测试集进行预测
#计算MAE与RMSE
#MAE（平均绝对误差）
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
import numpy as np

mae_nn = mean_absolute_error(
    y_test,
    y_pred_nn
)
#RMSE（均方根误差）
rmse_nn = np.sqrt(
    mean_squared_error(
        y_test,
        y_pred_nn
    )
)
#4.随机森林进行对比
#训练模型
from sklearn.ensemble import RandomForestRegressor#导入随机森林回归模型

rf = RandomForestRegressor(#建立随机森林模型
    n_estimators=100,#使用100棵决策树进行集成学习
    random_state=42
)

rf.fit(X_train, y_train)
y_pred_rf = rf.predict(X_test)#用测试集预测
#计算
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
models = ['RF', 'NN']#设置模型名称
mae_values = [mae_rf, mae_nn]#设置数据
plt.bar(models, mae_values)#柱状图
plt.ylabel('MAE')
plt.title('MAE')
plt.savefig('outputs/mae_compare.png')
plt.close()

models = ['Random Forest', 'Neural Network']
rmse_values = [rmse_rf, rmse_nn]
plt.figure(figsize=(6,5))
plt.bar(models, rmse_values)
plt.ylabel('RMSE')
plt.title('RMSE')
plt.savefig('outputs/rmse_compare.png')
plt.close()

#M4:问答接口

#1.将M1-M3定义函数
def query_hour_demand(hour):#某小时订单量
    result = df[df['hour'] == hour].shape[0]

    return f'''{hour}点订单量为：{result}
相关图表:hourly_demand.png'''#返回文字结果与对应图表
def top_pickup_locations():#热门区域
    top = df['PULocationID'].value_counts().head(5)#获取订单量最高的前5个区域
    return f'''热门区域：{top}
相关图表:top_Pickup.png'''#返回文字结果与对应图表
def average_fare(hour):#平均车费
    result = df[df['hour'] == hour][
        'fare_amount'
    ].mean()
    return f'''{hour}点平均车费为：{result:.2f}
相关图表:hour_fare.png'''#返回文字结果与对应图表
def peak_hour_analysis():
    hourly_orders = df.groupby('hour').size()
    peak_hour = hourly_orders.idxmax()#获取订单量最大时段
    peak_value = hourly_orders.max()#获取峰值订单量
    return f'''最高峰时段为：{peak_hour}点，订单量为：{peak_value}
相关图表:hourly_demand.png'''#返回文字结果与对应图表

#预测需求
def predict_demand(zone, hour, weekday):
    input_df = pd.DataFrame({#构建输入数据。
        'PULocationID':[zone],
        'hour':[hour],
        'weekday':[weekday]
    })

    input_df = pd.get_dummies(#进行One-Hot编码
        input_df,
        columns=['PULocationID']
    )

    input_df = input_df.reindex(#补齐缺失列#保证预测数据与训练数据结构一致
        columns=X.columns,
        fill_value=0
    )

    pred = model.predict(#模型预测
        input_df.astype('float32')
    )

    return pred[0][0]
#大模型导入
from openai import OpenAI#导入OpenAI SDK
client = OpenAI(#初始化客户端
    api_key='sk-ee0d09f4cb8240f49f029acf4a520330',
    base_url='https://api.deepseek.com'#DeepSeek接口地址
)

def ask_ai(question):
#调用大模型接口
    response = client.chat.completions.create(
        model='deepseek-chat',#使用对话模型

        messages=[#构建系统提示词
            {
                'role':'system',
                'content':'''
你是一个纽约出租车数据分析助手。

你的任务：
1.解释出租车数据分析结果
2.回答用户关于订单量、车费、高峰期的问题
3.用简洁自然语言回答
4.如果用户问题与出租车无关，也礼貌回答
'''
            },

            {
                'role':'user',
                'content':question
            }
        ],

        temperature=0.7
    )

    return response.choices[0].message.content#返回内容
#问答系统
import re#导入正则表达式模块#用于提取用户输入中的数字
while True:#持续运行问答系统
    question = input('请输入问题(q退出):')#获得输入
    if question == 'q':#问题匹配
        break
    elif '订单量' in question:#订单量查询
        nums = re.findall(r'\d+', question)#提取数字
        if len(nums) == 0:#如果没有输入时间如果没有输入则提示
            print('请输入小时，例如：10点订单量')
        else:
            hour = int(nums[0])
            result = query_hour_demand(hour)
            print(result)
    elif '热门区域' in question:#热门区域查询
        print(top_pickup_locations())
    elif '平均车费' in question:#平均车费查询
        nums = re.findall(r'\d+', question)
        if len(nums) == 0:
            print('请输入小时，例如：10点平均车费')
        else:
            hour = int(nums[0])
            result = average_fare(hour)
            print(result)
    elif '预测' in question:#需求预测
        nums = re.findall(r'\d+', question)
        if len(nums) < 3:#输入格式错误提示
            print('请输入：预测 区域 小时 星期')
            print('例如：预测 236 10 2')
        else:
            zone = int(nums[0])
            hour = int(nums[1])
            weekday = int(nums[2])
            result = predict_demand(
                zone,
                hour,
                weekday
            )
            print('预测需求量:', result)
    elif '高峰期' in question:
        result = peak_hour_analysis()
        print(result)
    else:#其他问题交给DeepSeek大模型
        try:
            answer = ask_ai(question)
            print(answer)

        except Exception as e:#异常处理#防止API调用失败导致程序崩溃
            print('大模型调用失败')
            print(e)