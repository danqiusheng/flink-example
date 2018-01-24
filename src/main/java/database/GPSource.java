package database;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import streaming.model.Model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class GPSource implements SourceFunction<Model> {

    static int count = 0;
    @Override
    public void run(SourceContext<Model> ctx) throws Exception {
        while(true){
            List<Model> sourceArr = getConnection( (++count) *1000);
            for (Model model : sourceArr){
                ctx.collect(model);
            }
          }
    }

    @Override
    public void cancel() {

    }


    // 获取gp的连接
    private static List<Model> getConnection(Integer count) throws ClassNotFoundException, SQLException {
        System.out.println("获取数据");
        Class.forName("org.postgresql.Driver");
        Connection con = DriverManager.getConnection("jdbc:postgresql://192.168.9.234:5432/jafjdb?characterEncoding=utf8&useSSL=false", "gpadmin", "pivotal");
        String sql = "SELECT objtype,createtime,upcolor_tag_1,lowcolor_tag_1 , license FROM target_lib limit 1000 offset " + count;
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        List<Model> list = new ArrayList<Model>();
        while(rs.next()){
            int objtype = rs.getInt("objtype");
            Timestamp timestamp = rs.getTimestamp("createtime");
            String color1 = rs.getString("upcolor_tag_1");
            String color2 = rs.getString("lowcolor_tag_1");
            String license = rs.getString("license");
            Model model = new Model(objtype,timestamp.getTime(),color1,color2);
            model.setLicense(license);
            list.add(model);
        }
        con.close();
        return list;
    }
}

