package com.pearson.daalt.qa.tests;

//import com.datastax.driver.core.Row;
import com.pearson.daalt.qa.helpers.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by vamarra on 1/28/15.
 */
public class SampleConnectivityTest {

  //  com.datastax.driver.core.ResultSet resultSet = null;
    JDBCHiveHelper hiveHelper = null;
    JDBCRedShiftHelper  hiveRedshiftHelper = null;
    JDBCSqlServerHelper helper = null;
   // JDBCCassandraHelper cassandraHelper = null;



    // @Test
    public void sampleTest() throws SQLException, InterruptedException {
        Map<String,String> tableDef = new HashMap<String, String>();
         ResultSet rset = null;
         Connection cnct= null;
        try {
            hiveHelper = new JDBCHiveHelper("10.201.1.64", "10000","qa","hadoop","hadoop");


           // String queryStr="select count(*) from subpub_messages where dt='2016-02-14'";

           // String queryStr="select count(*) count from immut.assessment_item_completion_revel_stage where date='2016-03-04'";
            //ArrayList<String> list= hiveHelper.getTableList();

             String queryStr="msck repair table qa.tincan_messages";


              hiveHelper.executeQuery(queryStr)   ;


          /*  for(String str:list)
            {
                System.out.println(str);
            } */




        } catch (Exception e) {
            System.err.print(e);
        }


    }


    // @Test
    public void sampleRedshiftTest() throws SQLException, InterruptedException {
        Map<String,String> tableDef = new HashMap<String, String>();
        ResultSet rset = null;
        Connection cnct= null;
        try {
            hiveRedshiftHelper = new JDBCRedShiftHelper("10.201.1.13", "5439","vamarra","LordRavi1");


            // String queryStr="select count(*) from subpub_messages where dt='2016-02-14'";

            String queryStr="select *  from revel.assessment limit 10;";
            //ArrayList<String> list= hiveHelper.getTableList();

            tableDef = hiveRedshiftHelper.getTableDefinitionByMetaData("revel.course");


            //loop a Map
            for (Map.Entry<String, String> entry : tableDef.entrySet()) {
                System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
            }

             rset=hiveRedshiftHelper.getTableRowsOnQuery(queryStr);
            while(rset.next()){

            }
            rset.beforeFirst();


        } catch (Exception e) {
            System.err.print(e);
        }
    }

     //@Test
    public void sampleRedshiftUpdateTest() throws SQLException, InterruptedException {
        Map<String,String> tableDef = new HashMap<String, String>();
        ResultSet rset = null;
        Connection cnct= null;
        try {
            hiveRedshiftHelper = new JDBCRedShiftHelper("10.201.1.136", "5439","vamarra","LordRavi1");


            // String queryStr="select count(*) from subpub_messages where dt='2016-02-14'";

            String queryStr="update    qa_group.users \n" +
                    " set firstname='Gingerf'\n" +
                    " where user_id=29550857";
            //ArrayList<String> list= hiveHelper.getTableList();

              hiveRedshiftHelper.updateRow( queryStr);







        } catch (Exception e) {
            System.err.print(e);
        }
    }


   // @Test
    public void samplePostgreSqlTestForCRI() throws SQLException, InterruptedException {
        Map<String,String> UserSelfAssessments = new HashMap<String, String>();
        JDBCPostgreSqlHelper jdbcPostgreSqlHelper=null;
        ResultSet userSelfAssessment,questionlist = null;
        Connection cnct= null;
        try {
            jdbcPostgreSqlHelper = new JDBCPostgreSqlHelper("use1a-daalt-mbr-cri-db-rds-prd-01.cqpdxjaydk1j.us-east-1.rds.amazonaws.com",
                    "5432","il_root", "$Trong_password1");


            // String queryStr="select count(*) from subpub_messages where dt='2016-02-14'";

            String queryStr="select * from cri_user.user_completes_self_assessment_stg where user_self_assessment_id in \n" +
                    "(select user_self_Assessment_id from cri_user.user_starts_self_assessment_stg where self_assessment_id = 'urn:udson:pearson.com/CRI/Prod:CRIAssessment/1')\n" +
                    " ";
            //ArrayList<String> list= hiveHelper.getTableList();

            userSelfAssessment = jdbcPostgreSqlHelper.getTableRowsOnQuery(queryStr);


            while (userSelfAssessment.next()) {
                //System.out.println("user_self_assessment_id:"+userSelfAssessment.getString("user_self_assessment_id")+"\tperson_id"+userSelfAssessment.getString("person_id"));
                UserSelfAssessments.put(userSelfAssessment.getString("user_self_assessment_id"),userSelfAssessment.getString("person_id") +"\t"+userSelfAssessment.getString("course_id"));
            }


            for (String key : UserSelfAssessments.keySet()) {
                String secondQuery = "select * from (\n" +
                        "select * from cri_user.self_assessment_item A LEFT JOIN (\n" +
                        "select uas.user_self_assessment_id, uas.self_assessment_item_id, us.person_id from cri_user.user_answered_self_assessment_item_stg uas inner join (\n" +
                        "select * from cri_user.user_completes_self_assessment_stg where user_self_assessment_id in \n" +
                        "(select user_self_Assessment_id from cri_user.user_starts_self_assessment_stg where self_assessment_id = 'urn:udson:pearson.com/CRI/Prod:CRIAssessment/1')\n" +
                        ") us on us.user_self_assessment_id = uas.user_self_assessment_id\n" +
                        ") B on A.self_assessment_item_id = B.self_assessment_item_id\n" +
                        "AND B.user_self_assessment_id ='" + key + "') \n" +
                "aaa where aaa.user_self_assessment_id is null";

                questionlist = jdbcPostgreSqlHelper.getTableRowsOnQuery(secondQuery);


                while (questionlist.next()) {
                    System.out.println(key+"\t"+UserSelfAssessments.get(key)+"\t"+questionlist.getInt("self_assessment_item_id")+"\t"+questionlist.getString("self_assessment_item_text"));
                }
            }

            jdbcPostgreSqlHelper.closeConnectionPool();
        } catch (Exception e) {
            System.err.print(e);
        }
    }




    //@Test
    public void tableRowsOnQuery() throws SQLException, InterruptedException {
        Map<String,String> tableDef = new HashMap<String, String>();
        ResultSet resultSet1=null;
        String query="select count(*) from paftdx.tdx_message_stage";
        try {
            hiveHelper = new JDBCHiveHelper("10.201.1.72", "10000","default","hadoop","hadoop");
            resultSet1 = hiveHelper.getTableRowsOnQuery(query);

            while (resultSet1 != null && resultSet1.next()) {
                String rowCount = resultSet1.getString(1);
                System.out.println("rowCount:"+rowCount);
            }
        } catch (Exception e) {
            System.err.print(e);
        }

        finally {
            hiveHelper.closeConnectionPool();
        }
    }




   // @Test
    public void sampleTest2() throws SQLException, InterruptedException {
        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            hiveHelper = new JDBCHiveHelper("10.201.1.65", "10000","default","hadoop","hadoop");

            ArrayList <String> resultSet2 = hiveHelper.getTableList();
            for(String table:resultSet2)
                System.out.println(table);

            String createTableQuery="CREATE EXTERNAL TABLE `autobahn_prd.mastering_mastering_User_1_0_From1`(\n" +
                    "  `autobahnmessage` string)\n" +
                    "PARTITIONED BY ( \n" +
                    "  `dt` string)\n" +
                    "ROW FORMAT SERDE \n" +
                    "  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' \n" +
                    "STORED AS INPUTFORMAT \n" +
                    "  'org.apache.hadoop.mapred.SequenceFileInputFormat' \n" +
                    "OUTPUTFORMAT \n" +
                    "  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'\n" +
                    "LOCATION\n" +
                    "  's3://messaging-persistence-prd/autobahn/mastering/UserAssessmentResult/1.0'";

            hiveHelper.executeQuery(createTableQuery);



        } catch (Exception e) {
            System.err.print(e);
        }
    }


     //@Test
    public void sampleTest1() throws SQLException, InterruptedException {
        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            helper = new JDBCSqlServerHelper("10.23.32.127", "54145","deltanetsioa","cds_ba","99AdCf49B33");
            tableDef = helper.getTableDefinition("disciplinegroups_disciplines");

            for (Map.Entry<String, String> entry : tableDef.entrySet()) {
                System.out.println(entry.getKey() + "\t"+ entry.getValue());
            }


        } catch (Exception e) {
            System.err.print(e);
        }

    }



   // @Test
    public void getColumnValueByQuery() throws SQLException, InterruptedException {
        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            helper = new JDBCSqlServerHelper("10.52.124.22", "1433","XL_prod_global","michaeld","md-89hj");
            String value = helper.getColumnValueOnQuery("select count(*) from products");

           System.out.println("count "+value);


        } catch (Exception e) {
            System.err.print(e);
        }

    }


    // @Test
    public void sampleTestToFindTableExists() throws SQLException, InterruptedException {
        boolean tableExists;
        try {
            helper = new JDBCSqlServerHelper("10.52.124.22", "1433","XL_prod_global","michaeld","md-89hj");
            tableExists = helper.findTableExists("products");

            System.out.println("table exists::"+ tableExists);

            System.out.println("rount count::"+helper.getTableRowCount("exercisecustompools"));


        } catch (Exception e) {
            System.err.print(e);
        }

    }


    // @Test
    public void testGetTableRowsOnQuery() throws SQLException, InterruptedException {
        ResultSet msSQLresultSet = null;
        try {
            helper = new JDBCSqlServerHelper("10.52.124.22", "1433","XL_prod_global","michaeld","md-89hj");
            msSQLresultSet = helper.getTableRowsOnQuery("select * from users where user_id = 67529 and authsystem_id = 1");

            int index =1;
            while(msSQLresultSet.next()) {
                System.out.println(msSQLresultSet.getString(index));
            }


        } catch (Exception e) {
            System.err.print(e);
        }

    }



   /* @Test
    public void sampleCassandraALEtest(){

        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            cassandraHelper = new JDBCCassandraHelper("stg-use1d-pr-79-alemmtxl-cadb-0002.prv-openclass.com", 9042,"mmtcat");
            resultSet = cassandraHelper.getTableRows("exercise");

            for (Row row : resultSet) {
                System.out.println(row.getString(1));
            }
          cassandraHelper.closeConnectionPool();
        } catch (Exception e) {
            System.err.print(e);
        }
    }*/

    //@Test
   /* public void testAWSS3()
    {
        AWSS3Helper awss3Helper = new AWSS3Helper();
        String str= awss3Helper.getObjectContents("daalt-cds-qa","qa1/raw/MathXL/MXLGBL/changeversion/part-m-00000");
        System.out.println(Integer.parseInt(str));



    } */

  /*  @Test
    public void getOracleTableDef()
    {
        JDBCOracleHelper jdbcOracleHelper=null;
        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            jdbcOracleHelper = new JDBCOracleHelper("10.20.1.83", "1521", "MASTGG","ALCHECDS", "alchecds");
            tableDef=jdbcOracleHelper.getTableDefinitionBySchema("PRD_ENGINEERING","BOOK");
            for (Map.Entry<String, String> entry : tableDef.entrySet()) {
                System.out.println(entry.getKey() + "\t"+ entry.getValue());
            }


        }catch(Exception e){

        }
        finally {
            jdbcOracleHelper.closeConnectionPool();
        }
    }*/

   // @Test
    public void testOracleconnection()
    {
        JDBCOracleHelper jdbcOracleHelper=null;
        try {
             jdbcOracleHelper = new JDBCOracleHelper("10.252.1.141","1521","rvl1dev","rvl1dev");
           // ArrayList<String> list=jdbcOracleHelper.getTableList();

            ResultSet resultSet1= jdbcOracleHelper.getTableRowsOnQuery("select * from product_mv where product_ssrid='006f51d5-430e-4088-b014-7b1b6940a4b6'");

            while(resultSet1.next())
            {
                for(int index=1;;index++)
                {
                    if(resultSet1.getString(index)!= null)
                    {
                        System.out.println(resultSet1.getString(index));

                    }
                }
            }
            System.out.println("list");
           // for(String str: list){
             //   System.out.println(str);
            //}
            //System.out.println(count);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            if(jdbcOracleHelper!=null)
            jdbcOracleHelper.closeConnectionPool();
        }


    }


    @AfterMethod
    public void tearDown()
    {
        if(hiveHelper != null)
            hiveHelper.closeConnectionPool();

        if(helper != null)
            helper.closeConnectionPool();
    }

}

