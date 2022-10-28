# DataX NebulaGraphWriter

简体中文|[English](./nebulagraphwriter.md)

## 1 快速介绍

NebulaGraphWriter插件实现了写入数据到NebulaGraph数据库图空间目标标签或者边类型的功能。底层实现上，NebulaGraphWriter通过JDBC连接NebulaGraph，按照NebulaGraph的nGql语法，执行insert语句，将数据写入NebulaGraph。

NebulaGraphWriter可以作为数据迁移工具供DBA将关系型数据库的数据导入到NebulaGraph，从而实现离线同步的功能。

## 2 实现原理

NebulaGraphWriter通过DataX框架获取Reader生成的协议数据(Record形式)，通过nebula-jdbc(JDBC driver)连接NebulaGraph，执行insert语句，将数据写入NebulaGraph。

除使用到nebula-jdbc外，还需要通过nebula-java获取NebulaGraph端系统级别元信息，用于同步标签，边类型以及字段的匹配。

## 3 功能说明

### 3.1 配置样例

- 配置一个写入NebulaGraph的作业，首先在NebulaGraph上创建图空间和标签：

```sql
CREATE SPACE IF NOT EXISTS cba(vid_type = FIXED_STRING(30));
CREATE TAG IF NOT EXISTS player(name string, age int);
CREATE EDGE IF NOT EXISTS follow(degree int);
```

- 此处使用从内存(streamreader)产生到NebulaGraph导入的数据。

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "type": "string",
                "value": "zhangsan"
              },
              {
                "type": "long",
                "value": 25
              }
            ],
            "sliceRecordCount": 1
          }
        },
        "writer": {
          "name": "nebulagraphwriter",
          "parameter": {
            "username": "root",
            "password": "nebula",
            "column": [
              "name",
              "age"
            ],
            "connection": [
              {
                "table": [
                  "player"
                ],
                "edgeType": [
                  {
                    "srcTag": "player", "srcPrimaryKey": "srcPlayerName",
                    "dstTag": "player", "dstPrimaryKey": "dstPlayerName"
                  }
                ],
                "jdbcUrl": "jdbc:nebula://cba"
              }
            ],
            "batchSize": 100
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

### 3.2 参数说明

- **jdbcUrl**
  - 描述：目标数据源的JDBC连接信息，NebulaGraph的JDBC信息请参考：[nebula-jdbc连接器的使用](https://github.com/nebula-contrib/nebula-jdbc)
  - 必选：是
  - 默认值：无

- **username**
  - 描述：数据库用户名
  - 必选：是
  - 默认值：无
- **password**
  - 描述：用户名密码
  - 必选：是
  - 默认值：无
- **table**
  - 描述：表名的集合，图数据库NebulaGraph在DataX数据同步语境中的表概念可以理解成标签和边类型，table应当包含column参数重的所有列，注意reader端的主键+table表名会被当作标签中节点的VID使用。reader端需指定主键，否则默认第一列字段为主键。
  - 必选：是
  - 默认值：无
- **edgeType**
  - 描述：当需要同步边类型数据时(即reader端的边表类型时)，需要指定edgeType中的srcTag和dstTag代表边类型中的起始标签类型和终点标签类型，以及这两个类型中的主键，即待同步边表中的起始和终止外键。
  - 必选：否
  - 默认值：无
- **column**
  - 描述：需同步字段的集合，字段的顺序应与record中的column的顺序一致，即需要与reader端的column字段顺序和名称一一对应。
  - 必选：是
  - 默认值：无
- **batchSize**
  - 描述：batchSize为一次record写入的大小规模，主要用于缓冲，防止DataX对NebulaGraph的IO请求次数过多，影响同步性能。
  - 必选：否
  - 默认值：1

### 3.3 类型转换

DataX中的数据类型与NebulaGraph中数据类型的映射转换关系

| DataX内部类型 | NebulaGraph数据类型                        |
| :------------ | :----------------------------------------- |
| LONG          | INT INT64 INT32 INT16 INT8                 |
| DOUBLE        | FLOAT DOUBLE                               |
| STRING        | FIXED_STRING(N) STRING                     |
| BOOLEAN       | BOOL                                       |
| BYTES         | 暂无对应数据类型                           |
| DATE          | DATE TIME DATETIME(暂不支持，后续完善支持) |

### 3.4 关系型数据库到NebulaGraph的参考示例

| 数据迁移示例       | 配置的示例                                                   |
| ------------------ | ------------------------------------------------------------ |
| MySQL到NebulaGraph | [关系型数据库MySQL到NebulaGraph 点表->标签](../src/test/resources/mysql2nebula.json) |
| 待补充             |                                                              |

## 4. 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

建表语句：

单行记录类似于：

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
    1. cpu:
    2. mem:
    3. net: 千兆双网卡
    4. disc: DataX 数据不落磁盘，不统计此项

* NebulaGraph数据库机器参数为:
    1. cpu:
    2. mem:
    3. net: 千兆双网卡
    4. disc:

#### 4.1.3 DataX jvm 参数

```
-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError
```

### 4.2 测试报告

#### 4.2.1 单表测试报告

| 通道数 | DataX速度(Rec/s) | DataX流量(MB/s) | DataX机器网卡流出流量(MB/s) | DataX机器运行负载 | DB网卡进入流量(MB/s) | DB运行负载 | DB TPS |
| ------ | ---------------- | --------------- | --------------------------- | ----------------- | -------------------- | ---------- | ------ |
| 1      |                  |                 |                             |                   |                      |            |        |
| 4      |                  |                 |                             |                   |                      |            |        |
| 8      |                  |                 |                             |                   |                      |            |        |
| 16     |                  |                 |                             |                   |                      |            |        |
| 32     |                  |                 |                             |                   |                      |            |        |

说明：

1. 

#### 4.2.4 性能测试小结



## 5 约束限制



## FAQ