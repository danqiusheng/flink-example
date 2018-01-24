package database;


import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import streaming.model.Model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class GPSink implements SinkFunction<Model> {
    @Override
    public void invoke(Model value) throws Exception {
        System.out.println("插入数据中");
        String sql = "insert into basic(id,col1,col2) values(?,?,?)";
        Connection con = getConnection();
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1,new Double( Math.random() * 10000).intValue());
        ps.setString(2,value.getObjtype()+"");
        ps.setFloat(3,new Long(value.getCreatetime()).floatValue());
        ps.execute();
        System.out.println("插入数据结束");
        con.close();
    }


    // 获取gp的连接
    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        System.out.println("获取数据");
        Class.forName("org.postgresql.Driver");
        Connection con = DriverManager.getConnection("jdbc:postgresql://192.168.9.234:5432/basic_db?characterEncoding=utf8&useSSL=false", "gpadmin", "pivotal");
        return con;
    }
}
