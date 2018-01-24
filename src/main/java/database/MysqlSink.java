package database;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import streaming.model.Count;

import java.sql.*;
import java.util.UUID;


/**
 * 将数据写入到Mysql
 */
public class MysqlSink extends RichSinkFunction<Count> {

    private Connection connection;
    private PreparedStatement preparedStatement;


    @Override
    public void open(Configuration parameters) throws Exception {
        // 在这里先查询，然后更新数据
        connection = getConnection();
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        if(preparedStatement!=null){
            preparedStatement.close();
        }
        if(connection!=null){
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Count value) throws Exception {
        String sql = "SELECT * FROM t_color where color= ?";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.getName());
        ResultSet rs = preparedStatement.executeQuery();
        boolean flag = true;
        String id = null;
        while (rs.next()) {
            id = rs.getString(1);
            flag = false;
        }
        rs.close();

        if (flag) {
            System.out.println("插入数据中");
            sql = "insert into t_color(id,color,count) values(?,?,?)";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, UUID.randomUUID().toString());
            preparedStatement.setString(2, value.getName());
            preparedStatement.setLong(3, value.getCount());
            preparedStatement.execute();
            System.out.println("插入数据结束");
        }else{
            System.out.println("更新数据中");
            sql = "update t_color set count=? where color=?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1,value.getCount());
            preparedStatement.setString(2,value.getName());
            preparedStatement.executeUpdate();
            System.out.println("更新数据结束");
        }
    }


    // 获取gp的连接
    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        System.out.println("获取数据");
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://192.168.6.172:3306/test?characterEncoding=utf8&useSSL=false", "root", "root");
        return con;
    }
}
