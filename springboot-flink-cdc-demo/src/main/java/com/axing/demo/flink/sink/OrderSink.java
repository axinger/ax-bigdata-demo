package com.axing.demo.flink.sink;

import com.alibaba.fastjson2.JSON;
import com.axing.demo.flink.model.CdcType;
import com.axing.demo.flink.model.ResponseModel;
import com.axing.demo.web.domain.MyOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author: xrp
 * @date: 2022/05/24/17:27
 * @description 订单sink
 */
@Slf4j
public class OrderSink extends RichSinkFunction<String> {

    private PreparedStatement ps = null;

    private Connection connection = null;
    String driver = "com.mysql.cj.jdbc.Driver";
    String url = "jdbc:mysql://localhost:3306/flink_b?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC";
    String username = "root";
    String password = "123456";


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConn();
    }

    private Connection getConn() {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
            log.info("数据库连接成功");
        } catch (Exception e) {
            log.error("数据库连接失败");
        }
        return connection;
    }


    @Override
    public void invoke(String value, Context context) throws Exception {
        //TranslateJson  将自定义的返回结果集转为具体实体
        ResponseModel responseModel = JSON.parseObject(value, ResponseModel.class);
        log.info("order获得数据 = {},线程池 = {} , {}" ,responseModel,Thread.currentThread().getName(),Thread.currentThread().getId());

        if (CdcType.insert.equals(responseModel.getType())) {
            MyOrder order = JSON.parseObject(responseModel.getData(), MyOrder.class);
            ps = connection.prepareStatement("insert into flink_b.order (id,no) values (?,?)");
            ps.setLong(1, order.getId());
            ps.setString(2, order.getNo());
            try {
                int executeUpdate = ps.executeUpdate();
                System.out.println("executeUpdate = " + executeUpdate);
            }catch (Exception e){
                log.error("插入数据库失败 = {}",e.getMessage());
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}