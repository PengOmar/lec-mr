package com.lec.common.utils;

import org.apache.commons.beanutils.BeanUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcUtil {

    //1.加载驱动
    static {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    //2.获取连接
    public static Connection getConnection() {
        Properties properties = new Properties();
        try {
            InputStream resourceAsStream = JdbcUtil.class.getClassLoader().getResourceAsStream("db.properties");
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String url=properties.getProperty("url");
        String user=properties.getProperty("user");
        String password=properties.getProperty("password");

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url,user,password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    //3.关闭连接
    public static void close(Connection conn, Statement st, ResultSet rs) {
        //关闭连接
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        //关闭statement
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        //关闭结果集
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    //-------------------------------封装sql操作------------------------------
    //查询返回List集合
    public static Map<String, String> getIdxDfnMap() {
        Map<String, String> idxDfnMap = new HashMap<>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //1.获取连接
            conn = getConnection();
            //2.获取预处理对象
            ps = conn.prepareStatement("SELECT  IDX_NO,IDX_RSLT_TGTB_TBLNM FROM IDX_DFN");
            //3.执行SQL语句
            rs = ps.executeQuery();
            //4.遍历结果集

            //开始遍历结果集
            while (rs.next()) {
                idxDfnMap.put(rs.getString("IDX_NO"), rs.getString("IDX_RSLT_TGTB_TBLNM"));
            }
            return idxDfnMap;
            //5.关闭连接
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
        return idxDfnMap;
    }


    //-------------------------------封装sql操作------------------------------
    //查询返回List集合
    public static Map<String, String> getLabDfnMap() {
        Map<String, String> labDfnMap = new HashMap<>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //1.获取连接
            conn = getConnection();
            //2.获取预处理对象
            ps = conn.prepareStatement("SELECT  LBL_NO,CALC_RULE_SQL_CNT FROM LAB_LBL_DFN where  LBL_STUCD!='01' AND ATMLB_FLG='1'");
            //3.执行SQL语句
            rs = ps.executeQuery();
            //4.遍历结果集

            //开始遍历结果集
            while (rs.next()) {
                labDfnMap.put(rs.getString("LBL_NO"), rs.getString("CALC_RULE_SQL_CNT"));
            }
            return labDfnMap;
            //5.关闭连接
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
        return labDfnMap;
    }


    /**
     * 增加、删除、修改
     *
     * @param sql sql语句
     * @param obj 参数
     * @return
     */
    public static boolean getDML(String sql, Object... obj) {

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);

            for (int i = 1; i <= obj.length; i++) {
                ps.setObject(i, obj[i - 1]);
            }
            System.out.println(sql);
            int update = ps.executeUpdate();

            if (update > 0) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(conn, ps, null);
        }
        return false;
    }

    //查询返回单个对象
    public static <T> T getOneObject(Class<T> cls, String sql, Object... obj) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //1.获取连接
            conn = getConnection();
            //2.获取预处理对象
            ps = conn.prepareStatement(sql);
            //循环参数，如果没有就不走这里
            for (int i = 1; i <= obj.length; i++) {
                //注意：数组下标从0开始，预处理参数设置从1开始
                ps.setObject(i, obj[i - 1]);
            }
            //3.执行SQL语句
            System.out.println(sql);
            rs = ps.executeQuery();
            //4.遍历结果集
            //遍历之前准备：因为封装不知道未来会查询多少列，所以我们需要指定有多少列
            ResultSetMetaData date = rs.getMetaData();//获取ResultSet对象的列编号、类型和属性

            int column = date.getColumnCount();//获取列数

            Field[] fields = cls.getDeclaredFields();//获取本类所有的属性

            //开始遍历结果集
            if (rs.next()) {

                //创建类类型实例
                T t = cls.newInstance();

                for (int i = 1; i <= column; i++) {

                    Object value = rs.getObject(i);//每一列的值

                    String columnName = date.getColumnName(i);//获取每一列名称

                    //遍历所有属性对象
                    for (Field field : fields) {
                        //获取属性名
                        String name = field.getName();

                        field.setAccessible(true);//打破封装，忽略对封装修饰符的检测

                        if (name.equals(columnName)) {
                            BeanUtils.copyProperty(t, name, value);
                        }
                    }
                }
                return t;
            }
            //5.关闭连接
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
        return null;
    }

    //查询总记录数
    public static Integer getCount(String sql, Object... obj) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //1.获取连接
            conn = getConnection();
            //2.获取预处理对象
            ps = conn.prepareStatement(sql);
            //循环参数，如果没有就不走这里
            for (int i = 1; i <= obj.length; i++) {
                //注意：数组下标从0开始，预处理参数设置从1开始
                ps.setObject(i, obj[i - 1]);
            }
            //3.执行SQL语句
            System.out.println(sql);
            rs = ps.executeQuery();

            //开始遍历结果集
            if (rs.next()) {

                return rs.getInt(1);
            }
            //5.关闭连接
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
        return null;
    }

}