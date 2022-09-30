package org.example;

import com.vesoft.nebula.jdbc.impl.NebulaDriver;
import java.sql.*;

/**
 * Hello world for connection with NebulaGraph!
 *
 */
public class App 
{
    public static void main( String[] args ) throws SQLException {
        NebulaDriver defaultDriver = new NebulaDriver();
        // 连接nebula-jdbc 连接到图空间cba上 使用默认的用户名(root)密码(任意)
        Connection connection = DriverManager.getConnection("jdbc:nebula://cba", "root", "nebula");
        Statement stmt = connection.createStatement();
        // 读取nebulaGraph中的节点
        String queryStatementNGql = "match (v:player) return v.player.name as name, v.player.age as age limit 4";
        ResultSet res = stmt.executeQuery(queryStatementNGql);
        // 输出nebulaGraph中的节点信息
        while (res.next()) {
            String name = res.getString("name");
            int age = res.getInt(2);
            // 生成业务信息
            System.out.println(name + " : " + age);
        }
        // 插入节点
        String insertStatementNGql = "insert vertex player(name,age) values \"player104\":(\"Lin Shuhao\", 32)";
        stmt.executeUpdate(insertStatementNGql);
        connection.close();
    }
}
