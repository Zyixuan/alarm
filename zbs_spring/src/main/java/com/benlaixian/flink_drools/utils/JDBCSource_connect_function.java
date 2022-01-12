package com.benlaixian.flink_drools.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.InputStream;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

public class JDBCSource_connect_function extends RichSourceFunction<String> {
    Connection connection = null;
    PreparedStatement pst = null;
    Properties properties=new  Properties();
    ResultSet resultSet =null;
    private String tableSql;

    public JDBCSource_connect_function(String tableSql) {
        this.tableSql = tableSql;
    }

    public String getTableSql() {
        return tableSql;
    }

    public void setTableSql(String tableSql) {
        this.tableSql = tableSql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        InputStream resourceAsStream = Properties.class.getClassLoader().getResourceAsStream("E:\\java_home_work\\IdeaProjects\\zbs_learn\\zbs_spring\\src\\main\\resources\\mysql.properties");
        properties.load(resourceAsStream);
        System.out.println(properties.getProperty("url"));
        connection=DriverManager.getConnection(properties.getProperty("url"),properties.getProperty("username"),properties.getProperty("password"));
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        pst=connection.prepareStatement("?");
        pst.setString(1,"tableSql");
        resultSet = pst.executeQuery();
        ctx.collect(resultSet);
    }

    @Override
    public void cancel() {

        try {
            resultSet.close();
            pst.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
