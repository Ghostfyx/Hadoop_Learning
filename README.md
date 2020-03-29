# Hadoop_Learning
《Hadoop权威指南》书中代码实现
## 1. IDEA配置Hadoop开发环境
### 1.1 导入Jar包
将Hadoop share目录下的jar包导入IDEA工程：
1. ”/hadoop/share/hadoop/common”目录下的hadoop-common-2.7.1.jar和haoop-nfs-2.7.1.jar；
2. "/hadoop/share/hadoop/common/lib”目录下的所有JAR包；
3. “/hadoop/share/hadoop/hdfs”目录下的haoop-hdfs-2.7.1.jar和haoop-hdfs-nfs-2.7.1.jar；
4. “/hadoop/share/hadoop/hdfs/lib”目录下的所有JAR包。

支持整个文件夹导入。
### 1.2 导入配置文件
把集群上的core-site.xml和hdfs-site.xml(这两文件存在/hadoop/etc/hadoop目录下)放到当前工程项目下，在eclipse下面就项目下面的bin目录，idea的话我们放到src目录下，放到其他目录下都没有用的。
## 2. IDEA下连接Hadoop问题与解决
### 2.1 问题一
报错：`org.apache.hadoop.ipc.RemoteException: Server IPC version 9 cannot communicate`

原因：hadoop-core中的ipc 版本太低。

解决方案：在Maven的pom文件中加入：
```html
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-core</artifactId>
    <version>1.2.0</version>
</dependency>
```
具体版本号可根据当前Hadoop版本修改，我的Hadoop版本号是2.7.2



