# DataX NebulaGraphReader

## 1 快速介绍

NebulaGraphReader插件实现了从NebulaGraph中读取数据。在底层实现上，NebulaGraphReader通过JDBC连接远程NebulaGraph数据库，并执行nGql语句(默认采用LOOKUP语句，后续可考虑扩展MATCH FETCH GO等更复杂功能强大的nGql查询语句)从NebulaGraph同空间中查询出来。

## 2 实现原理

简而言之，NebulaGraphReader通过JDBC连接器连接到远程的NebulaGraph数据库，并通过用户配置信息生成查询nGql语句，发送给远程NebulaGraph数据库，并将该nGql执行返回结果使用DataX框架中支持的数据类型封装成抽象数据集(record形式)，并传递给下游Writer端处理。

对于用于配置的table, column, where信息，NebulaGraphReader将其拼接为nGql语句发送到NebulaGraph数据库中；而对于用户配置的querySql信息，则经过校验后直接发送给NebulaGraph数据库中。

## 3 功能说明

### 3.1 配置样例

- 配置一个从NebulaGraph数据库同步数据到本地内存的作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "nebulagraphreader",
          "parameter": {
            "username": "root",
            "password": "nebula",
            "connection": [
              {
                "table": [
                  "player"
                ],
                "jdbcUrl": [
                  "jdbc:nebula://cba"
                ]
              }
            ],
            "column": [
              "name",
              "age"
            ],
            "where": "player.age >= 22"
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
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

- 配置一个用户定义的nGql语句从NebulaGraph数据库同步数据到本地内存的作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "nebulagraphreader",
          "parameter": {
            "username": "root",
            "password": "nebula",
            "connection": [
              {
                "querySql": [
                  "lookup on player where player.age >= 22 yield properties(vertex).name, properties(vertex).age"
                ],
                "jdbcUrl": [
                  "jdbc:nebula://cba"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
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
  - 描述：目标数据源的JDBC连接信息，注意，jdbcUrl必须包含在connection配置单元中.NebulaGraph的JDBC信息请参考：[nebula-jdbc连接器的使用](https://github.com/nebula-contrib/nebula-jdbc)
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
  - 描述：表名的集合，图数据库NebulaGraph在DataX数据同步语境中的表概念可以理解成标签和边类型，table应当包含column参数重的所有列，使用 JSON 的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一 schema 结构， NebulaGraphReader不予检查表是否同一逻辑表。注意，table必须包含在 connection 配置单元中。
  - 必选：是(除非使用querySql，否则为必选项)
  - 默认值：无
- **querySql**
  - 描述：在某些业务场景中，where不足以描述所要求筛选的条件，用户可以通过自定义配置querySql中的nGql语句，供插件直接使用，并向NebulaGraph发送查询语句。当用户配置querySql后，table，column和where字段就会被自动忽略。例如需要同时操作多个标签或者边类型中的数据时，则可以使用该配置。
  - 必选：否
  - 默认值：无
- **column**
  - 描述：需同步字段的集合，字段的顺序应与record中的column的顺序一致，即需要与writer端的column字段顺序和名称一一对应。
  - 必选：是
  - 默认值：无
- **where**
  - 描述：筛选条件中的where子句，NebulaGraphReader根据指定的column, table, where条件拼接nGql，并根据该nGql进行筛选数据。
  - 必选：否
  - 默认值：无
  

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
| MySQL到NebulaGraph | [NebulaGraph到关系型数据库 标签->点表](../src/test/resources/n2mysql.json) |
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