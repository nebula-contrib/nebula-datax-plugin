# nebula支持DataX

## 1 方案思路

本项目架构主要参考DataX官方代码库，项目主要分为Writer和Reader两部分进行开发和测试。由于DataX官方代码库中尚未接入其他图数据库插件，所以本项目开发中也考虑到图数据库与其他数据库开发的差异。

***从原理角度解释项目：***

我们以从关系型数据库MySQL中的数据同步插入到NebulaGraph中为例。之所以能够把关系型数据库中的表数据同步到NebulaGraph图数据库中，是因为nebula中存在Tag和Edge Type这两个类表概念，Tag作为标签类型，标识着某一类点的属性，可以近似为关系型数据库的表结构，而Edge Type同理，也可以近似为关系型数据中的关系表结构。因此当我们想同步关系型数据中某些实体表时，则对应的就是nebula中对应的Tag。而某一Tag下会对应多个Vertex，其相当于关系型数据中的表项，而Edge对于Edge Type的关系同样类似Tag和Vertex的关系。因此无论nebula作为Reader还是Writer都可以从结构角度匹配上关系型数据库中的表结构。

**Reader插件开发的实现思路：**

通过nebula nGql的查询语句 match 查询Tag标签下对应的所有Vertex节点，并返回节点集合组装成record包集合，通过DataX发送给关系型数据库，注意关系型数据库的Table表需要和Tag标签的名称完全一致，否则会出现匹配失败。同理，利用match查询语句查询某个制定edge_type类型下的所有边，然后通过关系型数据库的插入语句写入目标数据库中。需要注意的是，当使用match语句时，需要确保Tag和Edge Type已经建立索引。

**Writer插件开发的实现思路：**

在开发Writer插件时，我们需要利用配置文件中的column字段，利用元信息获得哪些字段属于哪些标签和边类型，然后进行匹配。在获取这些信息后，我们利用从reader插件端获取到的record包集合，通过Java8中stream的filter，map等操作组装插入语句。由于我们规定关系型数据库的表名称和图数据库的标签和边类型必须一致，所以我们可以直接利用tag_name和edgeType_name进行字段的匹配，通过insert vertex <tag_name>和insert edge <edge_type>语句插入到nebula中。

综上所述，我们可以采用table来代替tag和edge_type，并作为tableMeta中的tableType字段。

## 2 项目架构

## 代码架构

***以NebulaGraphWriter开发为例，其代码开发主要分为五大部分：***

第一部分为继承Writer抽象类的NebulaGraphWriter的主程序开发，其中需要实现Job和Task的各个方法，完成Writer插件与DataX框架的接入。其最核心的部分为startWrite方法的开发，为降低整个项目的耦合性，将业务逻辑独立出来，归入到DataHandler部分进行实现。

第二部分为DataHandler的开发与实现，也就是整个Writer插件业务逻辑部分的具体实现，其中最核心的为handle方法的实现。handle方法中首先通过DriverManager获取数据库连接对象，连接到NebulaGraph中。然后初始化SchemaManager对象，并加载数据库元信息，主要包括标签元信息TagMeta，边类型元信息EdgeTypeMeta (此处将TagMeta和EdgeTypeMeta均合并到TableMeta中)，以及字段元信息ColumnMeta，完成数据库静态信息的匹配。之后构造一个ArrayList用于记录从reader端通过DataX channel发送到writer端的Record包集合。整个写入逻辑中需要封装到writeBatch方法中，需要根据规定的Batch大小限定每次写入的批量大小，写入方法通过组装拼接nebula nGql的DDL insert语句实现。

第三部分则是对应的各种元信息的定义，需要明确各种元信息对应的属性参数。元信息需要与用户配置的参数对应匹配。在DataHandler的构造函数中需要通过用户配置的json参数文件，读取出其中各项配置Key，尤其是Column字段和Tag以及EdgeType字段。

第四部分为配置信息，主要是Key和Constants，其包括对应的配置文件的各种参数，还有对应的常量信息，如批量大小的size设置。

第五部分则是SchemaManager，用于实现meta元信息的匹配，加载和创建。

## 3 项目数据链路及流程图

用户需要配置的job.json中有很多重要的参数，其中最主要的就是columns字段和tables字段，其中包含数据库对应的字段信息以及需要同步的表信息。

以Mysql为Reader，nebula为Writer：

首先我们需要创建好两个数据库，MySQL则构建多对多型数据库，实体类为论文，作者，类别，连接类为书籍的著作所有权，类别所属情况。nebula则需要构建标签论文，作者以及类别，边类型包括著作所有权和类别所属表，论文和作者的对应关系，论文和类别的对应关系都是多对多关系)，这种情况是需要单独开设额外的表来存储实体类之间的关系的。

![image-20221028141133870](/Users/eldinzhou/Library/Application Support/typora-user-images/image-20221028141133870.png)

## 4 项目文档

项目Reader和Writer插件的详细文档说明请参考如下链接：

***NebulaGraphReader***: [NebulaGraphReader插件说明文档](./nebulagraphreader/doc/nebulagraphreader.md)

***NebulaGraphWriter***: [NebulaGraphWriter插件说明文档](./nebulagraphwriter/doc/nebulagraphwriter-CN.md)

项目GitHub链接：https://github.com/nebula-contrib/nebula-datax-plugin/pull/1

可参考链接：https://github.com/nebula-contrib/DataX



