package database;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import streaming.model.StaEntity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;


/**
 * 将数据写入到Mysql
 */
public class MySqlSinkDemo extends RichSinkFunction<StaEntity>  {

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
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(StaEntity value, Context context) throws Exception {
        System.out.println("插入数据中");
        String sql = "insert into t_count(id,color,count) values(?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, UUID.randomUUID().toString());
        preparedStatement.setString(2, value.getCreateTime()+"");
        preparedStatement.setLong(3, value.getData());
        preparedStatement.execute();
        System.out.println("插入数据结束");

    }


    // 获取gp的连接
    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://192.168.6.172:3306/test?characterEncoding=utf8&useSSL=false", "root", "root");
        return con;
    }


}
