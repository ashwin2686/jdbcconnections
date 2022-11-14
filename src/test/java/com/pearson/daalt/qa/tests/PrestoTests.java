package com.pearson.daalt.qa.tests;

import com.pearson.daalt.qa.helpers.JDBCPrestoHelper;
import com.pearson.daalt.qa.helpers.JDBCRedShiftHelper;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vamarra on 3/1/15.
 */
public class PrestoTests {


   //  @Test
    public void testSample() throws InterruptedException, SQLException {

        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Presto Driver is missing");
            e.printStackTrace();
        }
        //String sql = "SELECT * FROM sys.node";
        String sql = "SELECT count(1) as count FROM default.adaptive_recommendations";

        String sql1="select  distinct json_extract_scalar(subpubmessage,'$.messageTypeCode') " +
                "as messageTypeCode,count(json_extract_scalar(subpubmessage,'$.messageTypeCode')) " +
                "as count from default.adaptive_recommendations   where dt ='2017-07-27' " +
                "group by json_extract_scalar(subpubmessage,'$.messageTypeCode')";


        //String url = "jdbc:presto://10.201.1.64:8889/hive/defualt";
         String url = "jdbc:presto://10.201.1.64:8889/hive/default?user=test&password=null&SSL=true";
        Connection connection = DriverManager.getConnection(url, "test", null);
         connection.setCatalog("hive");

         Statement statement = connection.createStatement();

        ResultSet rs = statement.executeQuery(sql1);
        while (rs.next()) {
            System.out.println(rs.getInt("count"));
        }


    }


    @Test
    public void samplePrestoTest() throws SQLException, InterruptedException {
        Map<String, String> tableDef = new HashMap<String, String>();
        int count = 0;
        Connection cnct = null;
        Map<String,String> tableDef1 = new HashMap<String, String>();


        JDBCPrestoHelper jdbcPrestoHelper = null;
        try {
            jdbcPrestoHelper = new JDBCPrestoHelper("10.201.1.88", "8889","test",null);
            tableDef = jdbcPrestoHelper.getTableDefinition("immut.users_mathxl_raw");
            for (Map.Entry<String, String> entry : tableDef.entrySet()) {
                System.out.println(entry.getKey() + "\t"+ entry.getValue());
            }
        } catch (Exception e) {
        }

    }



     @Test
    public void samplePrestoTestGetRows1() throws SQLException, InterruptedException {
        Map<String, String> tableDef = new HashMap<String, String>();
        int count = 0;
        Connection cnct = null;
        Map<String,String> tableDef1 = new HashMap<String, String>();
        ResultSet rset = null;


        JDBCPrestoHelper jdbcPrestoHelper = null;
        try {
            jdbcPrestoHelper =  new JDBCPrestoHelper("10.201.1.88", "8889", "test",null);
            String queryStr=" select json_extract_scalar(subpubmessage,'$.Message_Type_Code') , count(*) as count  from subpub_messages\n" +
                    "    where dt='2018-01-03' group by json_extract_scalar( subpubmessage ,'$.Message_Type_Code')";

            ResultSet resultSet = jdbcPrestoHelper.getTableRowsOnQuery(queryStr);
            System.out.println("counts::"+count);
            while(resultSet.next())
            {
                System.out.println(resultSet.getString(1)+"\t"+resultSet.getInt(2));
            }



        } catch (Exception e) {
        }

    }

    @Test
    public void samplePrestoTestGetRows2() throws SQLException, InterruptedException {
        Map<String, String> tableDef = new HashMap<String, String>();
        int count = 0;
        Connection cnct = null;
        Map<String,String> tableDef1 = new HashMap<String, String>();
        ResultSet rset = null;


        JDBCPrestoHelper jdbcPrestoHelper = null;
        try {
            jdbcPrestoHelper =  new JDBCPrestoHelper("10.201.1.88", "8889", "test",null);
            String queryStr="select * from immut.chaptertests_mathxl_raw limit 1";

            ResultSet resultSet = jdbcPrestoHelper.getTableRowsOnQuery(queryStr);
            System.out.println("counts::"+count);
            while(resultSet.next())
            {
                System.out.println(resultSet.getString(1)+"\t"+resultSet.getInt(2));
            }



        } catch (Exception e) {
        }

    }


    // @Test
    public void samplePrestoTestGetRows() throws SQLException, InterruptedException {
        Map<String, String> tableDef = new HashMap<String, String>();
        int count = 0;
        Connection cnct = null;
        Map<String,String> tableDef1 = new HashMap<String, String>();
        ResultSet rset = null;


        JDBCPrestoHelper jdbcPrestoHelper = null;
        try {
            jdbcPrestoHelper =  new JDBCPrestoHelper("10.201.1.88", "8889", "test",null);
            String queryStr="select count(*) from autobahn_prd.ale_prd_ale_adaptivehomeworkrecommendation_1_0 where dt='2017-12-12' ";
             count = jdbcPrestoHelper.getTableRowCountOnQuery(queryStr);
            System.out.println("counts::"+count);



        } catch (Exception e) {
        }

    }
}
