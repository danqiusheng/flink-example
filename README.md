    -- Flink学习示例, 以下为搭建环境说明

 - 1 系统
      -  Rhel Hat-server-7.3-x86_64
 - 2 软件
      -  jdk-8u131-linux-x64.tar.gz
      -  flink-1.4.0-bin-hadoop28-scala_2.11.tgz
 - 3 安装要求(非HA)
     - 至少两台机器，一台master，其余slave
     - 机器分配如下
        ````
        master 192.168.6.30
        work1  192.168.6.31
        work2  192.168.6.32
        work3  192.168.6.181
        zk     192.168.6.28
 - 4 安装过程
     - 4.1 将软件上传到各个主机.
     - 4.2 配置各个主机的主机名和IP
          - 4.2.1 master上的配置            
            ````bash
            # vi /etc/hosts
            192.168.6.30 master
            192.168.6.31 work1
            192.168.6.31 work2
            192.168.6.181 work3
            192.168.6.28 zk
            ````
           
            `````bash
            # vi /etc/sysconfig/network-scripts/ifcfg-ens33
            `````
            修改如下：
            
            IPADDR=192.168.6.30 -- 不同的机器修改成对应的IP
            GATEWAY=192.168.6.254
            PREFIX=24
            DNS=202.103.24.68
            修改网络自启参数：
            ONBOOT="YES"
            修改动态分配参数：
            BOOTPROTO=NONE
            
            -- 重启网络服务
            ````bash
            # systemctl restart network.service
            ````
         - 4.2.2 配置JDK
            将jdk解压在/opt下
           
            ````bash
            # tar -zxf  jdk-8u131-linux-x64.tar.gz
            # mv jdk1.8_64/ jdk1.8 -- 重命名
            # vi /etc/profile
              加入以下信息：
              export JAVA_HOME=/opt/jdk1.8
              export PATH=$PATH:$JAVA_HOME/bin
            # source /etc/profile
            ````
          
          - 4.2.3 配置master上的flink
             将flink解压到/opt下   
            ````bash
            # tar -zxf flink-1.4.0-bin-hadoop28-scala_2.11.tgz
            # cd flink-1.4.0
            # vi conf/fink-conf.yml
             jobmanager.rpc.address:192.168.6.30
            # vi conf/masters
                  192.168.6.30:8081
            # vi conf/slaves
                  work1
                  work2
                  work3
            ````
          - 4.2.4 关闭防火墙
            ````bash
            # systemctl stop firewalld
            # systemctl disable firewalld
            # systemctl statue firewalld
            ```` 
          - 4.2.5 主机互信
           所有机器的操作：
            ``` bash
            # ssh-keygen -t rsa -P '' --每台机器都生成自己的公钥和密钥
            ```
            其他机器上的操作：
            ```bash
            # scp ~/.ssh/id_rsa.id master:~/.ssh/authorized_1 --将work1的公钥发送给master
            # scp ~/.ssh/id_rsa.id master:~/.ssh/authorized_2 --将work2的公钥发送给master
            ```    
            master操作：
            ```bash
            # cat ~/.ssh/authorized_1 >> ~/.ssh/authorized_keys
            # cat ~/.ssh/authorized_2 >> ~/.ssh/authorized_keys
            scp ~/.ssh/authorized_keys work1:~/.ssh/authorized_keys
            scp ~/.ssh/authorized__keys work2:~/.ssh/authorized_keys
            ```   
         - 4.2.6 slave的配置
             1. 安装JDK参照master
             2. flink解压到/opt
             3. 将master的host拷贝到slaves (在master机器上执行)
                 ````bash
                 # scp /etc/hosts work1:/etc/  
                 # scp /etc/hosts work2:/etc/ 
                 ````
             4. 将master的各种配置文件拷贝到slaves
                 ````bash
                 # scp /opt/flink-1.4.0/conf/flink-conf.yml work1:/opt/flink-1.4.0/conf/
                 # scp /opt/flink-1.4.0/conf/flink-conf.yml work2:/opt/flink-1.4.0/conf/
                 # scp /opt/flink-1.4.0/conf/masters.yml work1:/opt/flink-1.4.0/conf/
                 # scp /opt/flink-1.4.0/conf/masters.yml work2:/opt/flink-1.4.0/conf/
                 # scp /opt/flink-1.4.0/conf/slaves.yml work1:/opt/flink-1.4.0/conf/
                 # scp /opt/flink-1.4.0/conf/slaves.yml work2:/opt/flink-1.4.0/conf/
                 ````
                    
     - 4.3 启动Flink 集群
     
      在master上操作
      ````bash
      # ./bin/start-cluster.sh
      ````
      在浏览器访问：
      http://localhost:8081
      查看JobManager启动日志和TaskManager
      
      关闭集群
      ````bash
      #./bin/stop-cluster.sh
      ````
      
  
 - 5 练习Popular Place（以下所有操作非root用户）
    - 5.1 软件
    
          - 8.1.1 kibana-4.6.4-linux-x86_64.tar.gz
          - 8.1.2 elasticsearch-2.4.3.tar.gz
    - 5.2  解压各个软件,修改elasticsearchIP
    
         - 5.2.1 修改配置文件
             ```bash
             $ vi config/elasticsearch.yml
             ```
             修改如下：
             去掉#并修改IP，这个修改是为了其他机器访问，
             默认可以单机localhost，修改完毕使用ip访问
             network.host : IP
             去掉#
             http:port:9200
     - 5.3 启动elasticsearch
        ````bash
        $ ./bin/elasticsearch &
        `````
     - 8.4 验证 curl http://IP:9200/
             
                能正确显示如下信息：
                {
                  "name" : "Isaiah Bradley",
                  "cluster_name" : "elasticsearch",
                  "cluster_uuid" : "Bjt_O-vZQV2OcEg8pGdNKA",
                  "version" : {
                    "number" : "2.4.3",
                    "build_hash" : "d38a34e7b75af4e17ead16f156feffa432b22be3",
                    "build_timestamp" : "2016-12-07T16:28:56Z",
                    "build_snapshot" : false,
                    "lucene_version" : "5.5.2"
                  },
                  "tagline" : "You Know, for Search"
                }
      - 8.5 查看elasticsearch进程
           ``` bash
            $ ps ef | grep elasticsearch
           ```
      - 8.5 Kibana修改监听的elasticsearch的IP
               
           - 8.5.1 修改配置文件
               ``` bash
               $ vi config/kibana.yml
               ```
               修改如下：
               elasticsearch.url:"http://elasticsearch机器的IP:9200/"
      - 8.6 启动Kibana
        ```bash
        $ ./bin/kibana &
        ````
      - 8.7 查看kibana端口
            fuser -n tcp 5601


##  搭建Flink HA  
    
   1. 基本步骤
        操作如上,但是不启动集群。
   2. 搭建一个单节点zookeeperIP:192.168.6.28 
       ````bash
       # mv conf/zoo-sample.cfg conf/zoo.cfg
       ````
   3. 修改master配置文件
        ````bash
        # vi conf/master
        192.168.6.28:8081
        192.168.6.28:8082
        ````      
   4. 修改flink-conf.yaml   
        ````bash
        #  vi conf/flink-conf.yaml
        #jobmanager.rpc.address: IP --由zookeeper动态选举
        high-availability: zookeeper
        -- metadata临时存储目录，当一个jobManager down了。可以从这里获取信息恢复
        high-availability.storageDir: file:///tmp/zk/flink/ha/
        -- zookeeper的地址，ip1:port1 ip2:port2
        high-availability.zookeeper.quorum: 192.168.6.28:2181
        ````
   5. 修改zoo.cfg(master上的)
      ````bash
      # vi conf/zoo.cfg
      server.1=zk:2888:3888
      ````       
   6. 启动集群
       ````bash
        # ./bin/start-zookeeper-quorum.sh
        # ./bin/start-cluster.sh
       ````
   7. 关闭集群
       ````bash
       # ./bin/stop-cluster.sh
       # ./bin/stop-zookeeper-quorum.sh
       ````   
       
   8. webUI 查看 IP:port
       如果没有显示管理界面，查看日志,看jobManager是否启动失败;
       ````bash
       # cat log/flink-root-jobmanager-0-master.log
       ````
    
   9. 将jobManager加入的集群中
        ````bash
        # ./bin/jobmanager.sh start cluster --webui-port 8082 
        ````
   
   10. 将taskManager加入到集群中
        ````bash
        # ./bin/taskmanager.sh start cluster
        ````   
        
        
   11. savepoint文件存储位置flink-conf.yaml
        新增,用的Fs
        
        state.savepoints.dir: file:///tmp/flink/savepoints
   12. 查看端口号对应的进程号
        ````
        netstat -anp | grep 8081
        ````
         
         
            
