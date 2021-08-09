//package com.carstar;

import java.sql.*;
import java.time.*;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

public class OracleCon {

    public static void main(String[] args) throws SQLException, ClassNotFoundException, Exception {
        //File file = new File("sales.txt");
        ////////////    從ORACLE  ERP  SELECT 出  訂單資料
        String url = "jdbc:oracle:thin:@10.20.0.241:1521:topprod";
        String user = "ca1_db";
        String pass = "ca1_db";
        
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException ex) {
            System.out.println("NO OracleDriver: " + ex.getMessage());
        }
        
        //1 PO 2客編 3簡稱 4客戶 5市話 6手機 7傳真 8地址 9olderpno 10remark 11日期 12配套代號 13配套 14日期 15 服務代碼 16會員服務 17啟用日 18到期日 19   //11 訂單oea02 開單oea72 12                13                14                                            15        16        17                                      18                             19 1直購2油配              20            21                 22
        //String erp00 = "SELECT oea01,ta_oea19,occ02,occ28,occ261,NVL(tc_oeba21,'0'),NVL(occ271,'0'),NVL(occ241,'0'),NVL(occud02,'0'),NVL(occud03,'0'),NVL(occdate,oea02),NVL(ta_oea03,'0'),NVL(a1.ima02,'0'),NVL(to_char(OEA02,'yyyy-mm-dd HH:mm:ss'),'0'),tc_oebb05,a2.ima02,NVL(to_char(tc_oeba11,'yyyy-mm-dd'),'0'),NVL(to_char(tc_oeba12,'yyyy-mm-dd'),'0'),NVL(ta_oea06,'0'),NVL(gen02,'0'),NVL(gem02,'0'),NVL(oao06,'0') "
        String erp00 = "SELECT oea01,ta_oea19,NVL(occud04,'0'),occ28,occ261,NVL(tc_oeba21,'0'),NVL(occ271,'0'),NVL(occ241,'0'),NVL(occud02,'0'),NVL(occud03,'0'),NVL(occdate,oea02),NVL(ta_oea03,'0'),NVL(a1.ima02,'0'),NVL(to_char(OEA02,'yyyy-mm-dd HH:mm:ss'),'0'),tc_oebb05,a2.ima02,NVL(to_char(tc_oeba11,'yyyy-mm-dd'),'0'),NVL(to_char(tc_oeba12,'yyyy-mm-dd'),'0'),NVL(ta_oea06,'0'),NVL(gen02,'0'),NVL(gem02,'0'),NVL(oao06,'0') "
                + "FROM ca1_db.oea_file "
                + "LEFT JOIN ca1_db.occ_file on TA_OEA19=OCC01 LEFT JOIN ca1_db.ima_file a1 on ta_oea03=a1.ima01 "
                + "LEFT JOIN ca1_db.tc_oebb_file on tc_oebb01 = oea01 LEFT JOIN ca1_db.ima_file a2 on a2.ima01 = tc_oebb05 "
                + "LEFT JOIN ca1_db.tc_oeba_file on tc_oeba01 = tc_oebb01 and tc_oeba02 = tc_oebb02 "
                + "LEFT JOIN ca1_db.gen_file     on oea14=gen01       LEFT JOIN ca1_db.gem_file on oea15=gem01  LEFT JOIN ca1_db.oao_file on oea01=oao01 "
                + "WHERE oea00='0' AND oeaconf='Y'  AND to_char(OEA02,'yyyy-mm-dd') = to_char(sysdate-3,'yyyy-mm-dd') "
                + "AND tc_oebb05='MISC-1100001'" //MISC-1100001 會員服務       MISC-1100012創育中心全系列
                + "ORDER BY oea01";
        //String erp01 = "SELECT oea01,ta_oea19,occ02,occ28,occ261,NVL(tc_oeba21,'0'),NVL(occ271,'0'),NVL(occ241,'0'),NVL(occud02,'0'),NVL(occud03,'0'),NVL(occdate,oea02),NVL(ta_oea03,'0'),NVL(a1.ima02,'0'),NVL(to_char(OEA02,'yyyy-mm-dd HH:mm:ss'),'0'),tc_oebb05,a2.ima02,NVL(to_char(tc_oeba11,'yyyy-mm-dd'),'0'),NVL(to_char(tc_oeba12,'yyyy-mm-dd'),'0'),NVL(ta_oea06,'0'),NVL(gen02,'0'),NVL(gem02,'0'),NVL(oao06,'0') "
        String erp01 = "SELECT oea01,ta_oea19,NVL(occud04,'0'),occ28,occ261,NVL(tc_oeba21,'0'),NVL(occ271,'0'),NVL(occ241,'0'),NVL(occud02,'0'),NVL(occud03,'0'),NVL(occdate,oea02),NVL(ta_oea03,'0'),NVL(a1.ima02,'0'),NVL(to_char(OEA02,'yyyy-mm-dd HH:mm:ss'),'0'),tc_oebb05,a2.ima02,NVL(to_char(tc_oeba11,'yyyy-mm-dd'),'0'),NVL(to_char(tc_oeba12,'yyyy-mm-dd'),'0'),NVL(ta_oea06,'0'),NVL(gen02,'0'),NVL(gem02,'0'),NVL(oao06,'0') "
                + "FROM ca1_db.oea_file "
                + "LEFT JOIN ca1_db.occ_file on TA_OEA19=OCC01        LEFT JOIN ca1_db.ima_file a1 on ta_oea03=a1.ima01 "
                + "LEFT JOIN ca1_db.tc_oebb_file on tc_oebb01=oea01   LEFT JOIN ca1_db.ima_file a2 on a2.ima01=tc_oebb05 "
                + "LEFT JOIN ca1_db.tc_oeba_file on tc_oeba01=tc_oebb01 and tc_oeba02=tc_oebb02 "
                + "LEFT JOIN ca1_db.gen_file     on oea14=gen01       LEFT JOIN ca1_db.gem_file on oea15=gem01  LEFT JOIN ca1_db.oao_file on oea01=oao01 "
                + "WHERE oea00='0' AND oeaconf='Y'  AND to_char(OEA02,'yyyy-mm-dd') = to_char(sysdate-1,'yyyy-mm-dd') "
                + "AND tc_oebb05='MISC-1100001'" //MISC-1100001 會員服務       MISC-1100012創育中心全系列
                + "ORDER BY oea01";
//////////////  CTC ERPSyncCustInfo  ///////////////////////////////////////////////////////
        String connectionUrl = "jdbc:sqlserver://10.20.0.32:1433;database=ARTHGUI;user=sa;password=as;trustServerCertificate=true;";
        CallableStatement cs;
        try (Connection mscon0 = DriverManager.getConnection(connectionUrl);) {
            cs = mscon0.prepareCall("dbo.ERPSyncCustInfo"); // 設定 CallableStatement
            cs.execute();                                   // 執行 CallableStatement
        } catch (SQLException ex1) {
            System.out.println(ex1.getMessage());
        }
/////////////////////////////////////////////////////////////////////////////////////////////////////////
        LocalDateTime d = LocalDateTime.now();  //LocalDate d = LocalDate.now();// 取得今日
        int yearInt = d.getYear();
        String monthString = d.getMonth().name();
        int dayOfMonth = d.getDayOfMonth();
        int dayOfYear = d.getDayOfYear();
        String dayOfWeek = d.getDayOfWeek().name();  // 取得星期幾
//System.out.println(yearInt+" "+monthString+" "+dayOfMonth+" "+dayOfWeek);
//dayOfWeek ="MONDAY";
        Integer mondayInteger = dayOfWeek.compareTo("MONDAY");
//System.out.println(dayOfWeek+" 回傳 0 是星期一:"+mondayInteger);
//星期二到星期五  開通日取ERP訂單的前一天
//星期一 mondayInteger 變數零   星期一的訂單日要取星期五
        String ppString;
        if (mondayInteger == 0) {
            ppString = erp00; //  星期一的連線字串
        } else {
            ppString = erp01; //非星期一的連線字串  sysdate-1
        }
        try (Connection erpconnection = DriverManager.getConnection(url, user, pass);
                Statement stmt = erpconnection.createStatement();
                ResultSet rs = stmt.executeQuery(ppString)) {  //  訂單時間字串  timeString  00：00：00 時分秒
            String timeString = String.valueOf(d.getHour()) + ":" + String.valueOf(d.getMinute()) + ":" + String.valueOf(d.getSecond());
            String DString; //  訂單日期字串
            //////////////////////System.out.println("取ERP訂單資料");
            while (rs.next()) {
                if (rs.getRow() == 1) {
                    String enString = "";
                    String old_expired_day = "";
                    String old_product_main_name = "";
                    String buy_mode = "";
                    String acn = "";
                    int period_t3 = 0;
                    String erp_order_number = rs.getString(1);
                    int purchase_order_type;
                    if ("1".equals(rs.getString(19))) {
                        buy_mode = "直購";
                    }
                    if ("2".equals(rs.getString(19))) {
                        buy_mode = "油配";
                    }
                    DString = rs.getString(14).substring(0, 10);
                    if (rs.getRow() == 1) {
                        System.out.println("TipTop ERP 訂單日期:" + DString + " 程式執行時間:" + String.valueOf(d).substring(0, 10) + " " + dayOfWeek + " " + timeString);
                        System.out.println();
                    }
                    // 1合約 2客編 3簡稱  4負責人  5市話   6手機   7傳真  8地址  9old_erp_no  10 remark  11日期 12 配套代號  13配套  14日期 15服務代碼  16會員服務  17啟用日 18到期日  19 1直購2油配  20地區  21業務  22備註
                    System.out.println(rs.getRow() + " " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " + rs.getString(4) + " " + rs.getString(5) + " " + rs.getString(6) + " "
                            + rs.getString(7) + " " + rs.getString(8) + " " + rs.getString(9) + " " + rs.getString(10) + " " + rs.getString(11).substring(0, 10) + " " + rs.getString(12) + " "
                            + rs.getString(13) + " " + rs.getString(14).substring(0, 10) + " " + rs.getString(17) + " " + rs.getString(18) + " " + buy_mode + " " + rs.getString(21) + " "
                            + rs.getString(20) + " " + rs.getString(22));

                    String erp_phone, cell_phone, erp_ai, product_enable_date;
                    // 檢查 ERP 手機號碼                          //1 PO        2客編          4客戶         6 手機
                    erp_phone = check1_erp_phone(rs.getRow(), rs.getString(1), rs.getString(2), rs.getString(4), rs.getString(6));
                    if ("0".equals(erp_phone)) {
                        System.out.println(rs.getRow() + " erp_phone:" + erp_phone);
                    }
                    // 檢查 AI 手機號碼                           //1 PO        2客編          4客戶         6 手機
                    cell_phone = check2_ai_phone(rs.getRow(), rs.getString(1), rs.getString(2), rs.getString(4), erp_phone);
                    if ("0".equals(cell_phone)) {
                        System.out.println(rs.getRow() + " ai_phone:" + cell_phone);
                    }

                    // 檢查配套                             //1 PO            2客編           4客戶      12配套代號            13配套
                    erp_ai = check3_erp_ai(rs.getRow(), rs.getString(1), rs.getString(2), rs.getString(4), rs.getString(12), rs.getString(13));
                    if ("0".equals(erp_ai)) {
                        System.out.println(rs.getRow() + " erp_ai:" + erp_ai);
                    }
                    // 檢查開通日                                 //                       2客編           3簡稱           4客戶   6手機           14日期
                    //System.out.println(rs.getRow()+" Check 合約日");
                    product_enable_date = check4_product_enable_date(rs.getRow(), rs.getString(2), rs.getString(3), rs.getString(4), cell_phone, rs.getString(14).substring(0, 19));
                    //int YearInt = product_enable_date.compareTo(yearInt + "1");
                    int yyyy = Integer.parseInt(product_enable_date.substring(0, 4));   //取得 到期日訂單年
                    int mm = Integer.parseInt(product_enable_date.substring(5, 7));     //取得 到期日訂單月
                    int ddd = Integer.parseInt(product_enable_date.substring(8, 10));   //取得 到期日訂單日

                    LocalDate fromexpire = LocalDate.of(yyyy, mm, ddd);
                    LocalDate now = LocalDate.now();
                    Period pperiod = Period.between(now, fromexpire);

                    System.out.println("  到期日:" + fromexpire + "  今天日期:" + now);
                    System.out.println("  到期日多少年:" + pperiod.getYears() + "  到期日多少月:" + pperiod.getMonths());
                    //if (YearInt >= 2) {
                    if (pperiod.getMonths() > 6 || pperiod.getYears() > 0) {

                        //System.out.println(rs.getRow() + " 已開通 Check 合約日到 " + product_enable_date + " +" + YearInt + " year");
                        System.out.println(rs.getRow() + " 已開通 Check 合約日到 " + product_enable_date + " +" + pperiod.getMonths() + " 月");
                        //System.out.println(rs.getRow() + " Modified Day   " + period.getMonths() + "月" + period.getDays() +"日");
                        System.out.println("   !!!!!!!!!!");
                        manage_ai_report(rs.getRow(), "1", buy_mode, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(12), rs.getString(13), rs.getString(14), rs.getString(21), rs.getString(20), rs.getString(22), cell_phone);
                    } else {
//*print only
                        if (!"0".equals(cell_phone)) { //不能沒有手機號碼  有手機號碼才開通執行
                            if (!"0".equals(erp_ai)) {    //不能沒有配套      有手機號碼有配套才執行
                                /////////////////////////////////////////////////////////////////////////////////
                                ////////      從 ERP訂單   新增進  管理平台ManageDB
                                int manageid;
                                try {
                                    String url_managedb = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
                                    String user_managedb = "intit20200303"; //for localhost
                                    String pass_managedb = "jes#2354";
                                    Class.forName("com.mysql.jdbc.Driver");
                                    String str1 = "INSERT INTO manage_po (dealer_id,po_number,customer_number,bri_name,key_name,account_tel,account_phone,account_fax,"
                                            + "account_addr,old_erp_no,remarks,o_date,package_number,package_name,create_date) "
                                            + "SELECT 1,'" + rs.getString(1) + "','" + rs.getString(2) + "','" + rs.getString(3) + "','" + rs.getString(4) + "','" + rs.getString(5) + "','" + cell_phone + "','" + rs.getString(7) + "',"
                                            + "'" + rs.getString(8) + "','" + rs.getString(9) + "','" + rs.getString(10) + "','" + rs.getString(11) + "','" + rs.getString(12) + "','" + rs.getString(13) + "','" + rs.getString(14) + "' "
                                            + "FROM DUAL WHERE NOT EXISTS (SELECT account_phone FROM manage_po WHERE account_phone='" + cell_phone + "')";//customer_number='"+rs.getString(2)+"')";
                                    try (Connection con2 = DriverManager.getConnection(url_managedb, user_managedb, pass_managedb);
                                            Statement stmt2 = con2.createStatement()) {
                                        stmt2.executeUpdate(str1);                 //11111111 執行   INSERT
                                        ResultSet m_id = stmt2.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY   KEY
                                        m_id.next();                               //read
                                        manageid = m_id.getInt(1);             // 字串轉數字
                                        System.out.println(rs.getRow() + " 新增 manage_id : " + manageid);
                                    }
                                    int device_id = 0;
                                    String account_id_renew, member_type, device_phone = "", device_unique = "";
                                    //////////////////////////////////////////////                2客編            3簡稱            4客戶    6手機
                                    account_id_renew = check5_account_id(rs.getRow(), rs.getString(2), rs.getString(3), rs.getString(4), cell_phone);
                                    //System.out.println(rs.getRow()+" account_id:"+account_id_renew);
                                    member_type = check6_member_type(rs.getRow(), rs.getString(2), rs.getString(3), rs.getString(4), cell_phone);
                                    //System.out.println(rs.getRow()+" member_type:"+member_type);
                                    String url2 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
                                    String user2 = "intit-20200303";//"intit20200303"; //for localhost & 10.20.0.9
                                    String pass2 = "jes#2354";
                                    if (("2".equals(member_type)) || ("3".equals(member_type)))//體驗=2  副機=3
                                    {
                                        //     取得體驗 Id 基碼
                                        String str_t = "SELECT account_id,device_id,account_device_phone,account_device_unique FROM account_devices WHERE account_device_phone='" + cell_phone + "';";
                                        try (Connection con_t = DriverManager.getConnection(url2, user2, pass2);
                                                Statement st = con_t.createStatement();
                                                ResultSet rs_t = st.executeQuery(str_t)) {
                                            rs_t.next();
                                            //System.out.println(rs.getRow()+" id:"+rs_t.getInt(1)+" device_phone:"+rs_t.getString(3)+"  cell_phone:"+cell_phone+" device_unique:"+rs_t.getString(4));
                                            device_id = rs_t.getInt(2);
                                            device_phone = rs_t.getString(3) + "-555";
                                            device_unique = rs_t.getString(4);
                                            System.out.println(rs.getRow() + " id:" + rs_t.getInt(1) + " device_id:" + device_id + " device_phone:" + device_phone + "  cell_phone:" + cell_phone + " device_unique:" + device_unique);
                                            con_t.close();
                                        } catch (SQLException ex) {
                                            System.out.println("str_t 錯誤 " + ex.getMessage());
                                        }

                                        String sql_update0 = "UPDATE account_devices SET device_id=0,account_device_phone='" + device_phone + "', account_device_unique='' WHERE account_device_phone='" + cell_phone + "';";
                                        try (Connection conn_update = DriverManager.getConnection(url2, user2, pass2)) {
                                            Statement st1 = conn_update.createStatement();
                                            st1.executeUpdate(sql_update0);

                                            conn_update.close();
                                        } catch (SQLException ex) {
                                            System.out.println("sql_update0 錯誤 " + ex.getMessage());
                                        }
                                        String sql_update1 = "UPDATE accounts SET account_cell_phone='" + device_phone + "'WHERE account_cell_phone='" + cell_phone + "';";
                                        try (Connection conn_update = DriverManager.getConnection(url2, user2, pass2)) {
                                            Statement st1 = conn_update.createStatement();
                                            st1.executeUpdate(sql_update1);
                                            conn_update.close();
                                        } catch (SQLException ex) {
                                            System.out.println("sql_update1 錯誤 " + ex.getMessage());
                                        }
                                        account_id_renew = "0";
                                    }
                                    if ("0".equals(account_id_renew)) // account_id = 0  才能新增
                                    {
                                        purchase_order_type = 0;  //0:新增 1:續約 2:G3續約 3:續約轉300家族

                                        //String url2 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
                                        //String user2 = "intit-20200303";//"intit20200303"; //for localhost & 10.20.0.9
                                        //String pass2 = "jes#2354";
                                        ///////////////////////////////////////////////////////////////////////////////
                                        ///////////////////從 ERP訂單 新增進  AI accounts //////////
                                        String str2 = "INSERT INTO accounts(dealer_id,account_number,account_name,account_person_in_charge_name,"
                                                + "account_tel,account_cell_phone,account_addr,modified_at,created_at,updated_at) VALUES (1,?,?,?,?,?,?,?,?,?)";
                                        try (Connection con2 = DriverManager.getConnection(url2, user2, pass2);
                                                PreparedStatement ps2 = con2.prepareStatement(str2)) {
                                            //ps2.setInt   (1, 1);    //  dealer_id  固定寫 1
                                            ps2.setString(1, rs.getString(2));  //  account_customer_number
                                            ps2.setString(2, rs.getString(3));  //  account_name
                                            ps2.setString(3, rs.getString(4));  //  account_person_in_charge_name
                                            ps2.setString(4, rs.getString(5));  //  account_tel
                                            ps2.setString(5, cell_phone);  //  account_cell_phone
                                            ps2.setString(6, rs.getString(8));  //  account_addr
                                            ps2.setString(7, DString + " " + timeString); //  modified_at
                                            ps2.setString(8, DString + " " + timeString); //  created_at
                                            ps2.setString(9, DString + " " + timeString); //  updated_at
                                            ps2.executeUpdate();                // 222222222    執行   INSERT
                                            ResultSet a_id = ps2.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY   KEY
                                            a_id.next();                             //read
                                            int accountlastid = a_id.getInt(1);      // 字串轉數字
                                            System.out.println(rs.getRow() + " 新增 account_id : " + accountlastid);
                                            //////////////////////////////////////////////////////////
                                            /////////////  add to account_device
                                            String str3 = "INSERT INTO account_devices(account_id,account_device_phone,modified_at,created_at,updated_at,member_type) VALUES (?,?,?,?,?,1)";
                                            try (Connection con3 = DriverManager.getConnection(url2, user2, pass2);
                                                    PreparedStatement ps3 = con3.prepareStatement(str3)) {
                                                ps3.setInt(1, accountlastid);    // account_id
                                                ps3.setString(2, cell_phone);       // 手機
                                                ps3.setString(3, DString + " " + timeString); //  modified_at
                                                ps3.setString(4, DString + " " + timeString); //  created_at
                                                ps3.setString(5, DString + " " + timeString); //  updated_at
                                                //ps3.setString(6, 1);                      //  member_type   2020-10-14 add   1 for normal_user  2 for try member 3 for sub_user
                                                ps3.executeUpdate();   // 333333333    執行   INSERT
                                                ResultSet a_devices_id = ps3.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY   KEY
                                                a_devices_id.next();
                                                int a_devices_lastid = a_devices_id.getInt(1);   // 字串轉數字
                                                System.out.println(rs.getRow() + " 新增 account_device_id : " + a_devices_lastid);
                                                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                if (("2".equals(member_type))) //體驗=2  副機=3
                                                {
                                                    String sql_update2 = "UPDATE account_devices SET device_id='" + device_id + "',account_device_unique='" + device_unique + "' WHERE account_device_phone='" + cell_phone + "';";
                                                    try (Connection conn_update = DriverManager.getConnection(url2, user2, pass2)) {
                                                        Statement st1 = conn_update.createStatement();
                                                        st1.executeUpdate(sql_update2);
                                                        conn_update.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("sql_update2 錯誤 " + ex.getMessage());
                                                    }
                                                }
                                                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                // add to account_products
                                                // prepare  Product_ID    Product_Main_ID    // 12 配套代號
                                                String str4 = "SELECT product_id,product_main_id,period,ctc_code,point,TT_ERP_NAME FROM erp_ai WHERE TT_ERP_ID= '" + rs.getString(12) + "' ";
                                                try (Connection con4 = DriverManager.getConnection(url_managedb, user_managedb, pass_managedb);
                                                        Statement st = con4.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                                                        ResultSet rs4 = st.executeQuery(str4)) // 444444444
                                                {
                                                    rs4.next();
                                                    //System.out.println(rs.getRow()+" p_id:"+rs4.getInt(1)+" p_m_id:"+rs4.getInt(2)+" period:"+rs4.getInt(3)+" ctc_code:"+rs4.getString(4));
                                                    int product_main_id_nm = rs4.getInt(2);  //product_main_id_nm 為了和 product_main_id 有區別
                                                    int period = rs4.getInt(3);  //period
                                                    String ctc = rs4.getString(4);
                                                    int account_point = rs4.getInt(5);      //point
                                                    int account_point_left = account_point;
                                                    String TT_ERP_NAME = rs4.getString(6);      //TT_ERP_NAME for point

                                                    Calendar cal = Calendar.getInstance();  //使用預設時區和語言環境獲得日曆
                                                    java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  //通過格式化輸出日期
                                                    String expired_day, enable_day, T0;
                                                    enable_day = DString.substring(0, 10) + " 00:00:00"; //啟用日設定是ERP 訂單日
                                                    String a_p_today = format.format(cal.getTime()).substring(0, 10);//取得今日
                                                    System.out.println(rs.getRow() + " 今天日期 : " + a_p_today);
                                                    int compareTo = a_p_today.compareTo(enable_day);//今日大於ERP 訂單日  為正數 表示合約過期了
                                                    //System.out.println(rs.getRow()+" 今日 比較 ERP訂單 正數 啟用日是今天:"+compareTo);
                                                    if (compareTo > 0) {  //今日大於到期日 正數 表示合約日到期   啟用日是今天
                                                        enable_day = a_p_today;
                                                    }
                                                    ///enable_day=enable_day+" 00:00:00";
                                                    //今日小於到期日 負數 表示合約未到期  啟用日 是 續約客戶到期日
                                                    //a_p_enable_day=DString.substring(0,10)+" 00:00:00";; // 啟用日是ＥＲＰ訂單日
                                                    //*************************************************************************
                                                    // add to account_products
                                                    // prepare  Product_ID    Product_Main_ID
                                                    //System.out.println(rs.getRow()+" 今日 比較 ERP訂單 啟用日是:"+enable_day);
                                                    int year = Integer.parseInt(enable_day.substring(0, 4)); //取得 啟用日 訂單年
                                                    int month = Integer.parseInt(enable_day.substring(5, 7));//取得 啟用日 訂單月
                                                    int date = Integer.parseInt(enable_day.substring(8, 10));//取得 啟用日訂單日
                                                    cal.set(year, month - 1, date);   // JAVA month  0~11   數字 0 是1月  11是12月
                                                    cal.add(Calendar.YEAR, +1);   //取當年日期+1 年為到期日
                                                    T0 = format.format(cal.getTime());
                                                    expired_day = T0.substring(0, 10) + " 23:59:59";

                                                    if (period == 36) {
                                                        cal.add(Calendar.YEAR, +2);//取當年日期+2年為到期日
                                                        T0 = format.format(cal.getTime());
                                                        expired_day = T0.substring(0, 10) + " 23:59:59";
                                                    }
                                                    if (period == 48) {
                                                        cal.add(Calendar.YEAR, +3);//取當年日期+3年為到期日
                                                        T0 = format.format(cal.getTime());
                                                        expired_day = T0.substring(0, 10) + " 23:59:59";
                                                    }

                                                    System.out.println(rs.getRow() + " 啟用日 : " + enable_day + " 到期日 : " + expired_day);
                                                    enString = enable_day.substring(0, 10);
                                                    String S1 = check_CTC_data("0", period, rs.getString(2), rs.getString(9), rs.getString(10), DString, enable_day.substring(0, 10), expired_day.substring(0, 10), ctc); //A05026 // 檢查CTC_account
                                                    String sql_update2 = "UPDATE accounts SET account_customer_number='" + S1 + "' WHERE account_id='" + accountlastid + "';";
                                                    try (Connection conn_update = DriverManager.getConnection(url2, user2, pass2)) {
                                                        Statement st1 = conn_update.createStatement();
                                                        st1.executeUpdate(sql_update2);
                                                        conn_update.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("sql_update2 錯誤 " + ex.getMessage());
                                                    }
                                                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                    /////////////  add to account_point
                                                    String str_point = "INSERT INTO account_point(account_id,account_point,created_at,modified_at,updated_at) VALUES (?,?,?,?,?)";
                                                    try (Connection con_point = DriverManager.getConnection(url2, user2, pass2);
                                                            PreparedStatement ps_point = con_point.prepareStatement(str_point)) {
                                                        ps_point.setInt(1, accountlastid);    // account_id
                                                        ps_point.setInt(2, account_point);    //account_point
                                                        ps_point.setString(3, a_p_today + " " + timeString); //  created_at
                                                        ps_point.setString(4, a_p_today + " " + timeString); //  modified_at
                                                        ps_point.setString(5, a_p_today + " " + timeString); //  updated_at
                                                        ps_point.executeUpdate();                           // 執行 INSERT
                                                        ResultSet account_point_id = ps_point.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY KEY
                                                        account_point_id.next();
                                                        int account_point_lastid = account_point_id.getInt(1);   // 字串轉數字
                                                        System.out.println(rs.getRow() + " 新增 account_point_id : " + account_point_lastid);
                                                        ////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                        /////////////  add to account_point_history
                                                        String str_point_his = "INSERT INTO account_point_history"
                                                                + "(action_type,use_type,account_point,account_point_id,account_point_left,account_point_use_comment,use_type_description,created_at,updated_at,modified_at) "
                                                                + "VALUES (1,0,?,?,?,?,?,?,?,?) ";
                                                        //    1,2,3,4,5,6,7
                                                        try (Connection con_point_his = DriverManager.getConnection(url2, user2, pass2);
                                                                PreparedStatement ps_point_his = con_point_his.prepareStatement(str_point_his)) {
                                                            ps_point_his.setInt(1, account_point);                 // account_id
                                                            ps_point_his.setInt(2, account_point_lastid);          // account_point_lastid
                                                            ps_point_his.setInt(3, account_point_left);          // account_point_left
                                                            ps_point_his.setString(4, "購買產品:" + TT_ERP_NAME);  // account_point_use_commment
                                                            ps_point_his.setString(5, "點數獲得");                 // use_type_description
                                                            ps_point_his.setString(6, a_p_today + " " + timeString); // created_at
                                                            ps_point_his.setString(7, a_p_today + " " + timeString); // updated_at
                                                            ps_point_his.setString(8, a_p_today + " " + timeString); // modified_at
                                                            ps_point_his.executeUpdate();                          // 執行 INSERT
                                                            ResultSet account_point_his_id = ps_point_his.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY KEY
                                                            account_point_his_id.next();
                                                            int account_point_his_lastid = account_point_his_id.getInt(1);   // 字串轉數字
                                                            System.out.println(rs.getRow() + " 新增 account_point_history_id : " + account_point_his_lastid);
                                                            con_point_his.close();
                                                        } catch (SQLException ex) {
                                                            System.out.println("  !!! INSERT INTO account_point_history 錯誤 " + ex.getMessage());
                                                        }
                                                        con_point.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("  !!! INSERT INTO account_point錯誤 " + ex.getMessage());
                                                    }

                                                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                    String str5 = "INSERT INTO account_products(account_id,product_id,product_main_id,"
                                                            + "account_product_buy_datetime,account_product_enable_datetime,account_product_expire_datetime,account_product_buy_mode,erp_order_number,purchase_order_type,modified_at,created_at,updated_at,expiration_alert_notify_count) "
                                                            + "values (?,?,?,?,?,?,?,?,?,?,?,?,0)";     //  注意新加的到期提示欄位用 內定值 0
                                                    try (Connection con5 = DriverManager.getConnection(url2, user2, pass2);
                                                            PreparedStatement ps5 = con5.prepareStatement(str5)) {
                                                        ps5.setInt(1, accountlastid);     //  account_id
                                                        ps5.setInt(2, rs4.getInt(1));     //  product_id
                                                        ps5.setInt(3, rs4.getInt(2));     //  product_main_id
                                                        ps5.setString(4, DString + " " + timeString);//  buy_datetime
                                                        ps5.setString(5, enable_day);            //  enable_date ERP的定單日為啟用日
                                                        ps5.setString(6, expired_day);           //  expired_date 依合約日期為到期日
                                                        ps5.setString(7, buy_mode);              //  1直購2油配
                                                        ps5.setString(8, erp_order_number);      //  ERP訂單編號
                                                        ps5.setInt(9, purchase_order_type);      //  0:新增 1:續約 2:G3續約 3:續約轉300家族
                                                        ps5.setString(10, DString + " " + timeString);//  modified_at
                                                        ps5.setString(11, DString + " " + timeString);//  created_at
                                                        ps5.setString(12, DString + " " + timeString);//  updated_at
                                                        //ps5.setInt   (11,0);                   //  新加的到期提示欄位先用 內定值 0
                                                        ps5.executeUpdate();                     // 555555555  執行   INSERT
                                                        ResultSet a_product_id = ps5.getGeneratedKeys();
                                                        a_product_id.next();
                                                        int a_product_lastid = a_product_id.getInt(1);
                                                        System.out.println(rs.getRow() + " 新增 account_product_id : " + a_product_lastid);
                                                        /////////////////////////////////////////////////////////////////////////////////////////////////////
                                                        String sql_update3 = "UPDATE manage_po SET account_product_id = '" + a_product_lastid + "' "
                                                                + "WHERE manage_id='" + manageid + "';";
                                                        try (Connection conn_update = DriverManager.getConnection(url_managedb, user_managedb, pass_managedb)) {
                                                            Statement st1 = conn_update.createStatement();
                                                            st1.executeUpdate(sql_update3);
                                                            conn_update.close();
                                                        } catch (SQLException ex) {
                                                            System.out.println("sql_update3 manage_po 錯誤 " + ex.getMessage());
                                                        }
                                                        /////////////////////////////////////////////////////////////////////////////////////////////////////
                                                        //  找出  PRODUCT_GROOUP_ID   用 PRODUCT_MAIN_ID
                                                        String str6 = "SELECT product_group_id,product_main_id,limit_count "
                                                                + "FROM product_groups WHERE product_main_id='" + product_main_id_nm + "' ";
                                                        try (Connection con6 = DriverManager.getConnection(url2, user2, pass2)) {
                                                            Statement st6 = con6.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                                                            ResultSet rs6 = st6.executeQuery(str6); //6666666666
                                                            while (rs6.next()) {
                                                                cal.set(year, month - 1, date);           //取回起用日  //2020-09-15
                                                                //System.out.println(rs6.getRow()+" "+rs6.getInt(1)+" "+rs6.getInt(2)+" "+rs6.getInt(3));
                                                                String str7 = "INSERT INTO account_product_groups ("
                                                                        + "account_id,account_product_id,product_id,product_main_id,"
                                                                        + "product_group_id,group_limit_count,group_limit_count_original,"
                                                                        + "group_enable_datetime,group_expire_datetime,modified_at,created_at,updated_at) "
                                                                        + "values (?,?,?,?,?,?,?,?,?,?,?,?)";
                                                                /////////1 2 3 4 5 6 7 8 9 0 1 2

                                                                if (period == 36 || period == 48) // 三年合約
                                                                {
                                                                    T0 = format.format(cal.getTime());
                                                                    enable_day = T0.substring(0, 10) + " 00:00:00";      //2020-09-15 00:00:00
                                                                    cal.add(Calendar.YEAR, +1);                     //取當前日期+1 年
                                                                    T0 = format.format(cal.getTime());
                                                                    expired_day = T0.substring(0, 10) + " 23:59:59";     //2021-09-15  23:59:59

                                                                    try (Connection con7 = DriverManager.getConnection(url2, user2, pass2);
                                                                            PreparedStatement ps7 = con7.prepareStatement(str7)) {
                                                                        ps7.setInt(1, accountlastid);      //  account_id
                                                                        ps7.setInt(2, a_product_lastid);   //  account_product_id
                                                                        ps7.setInt(3, rs4.getInt(1));      //  product_id
                                                                        ps7.setInt(4, rs4.getInt(2));      //  product_main_id
                                                                        ps7.setInt(5, rs6.getInt(1));      //  product_group_id
                                                                        ps7.setInt(6, rs6.getInt(3));      //  limit_count
                                                                        ps7.setInt(7, rs6.getInt(3));      //  limit_count_original
                                                                        ps7.setString(8, enable_day);      //  group_enable_datetime
                                                                        ps7.setString(9, expired_day);     //  group_expired_datetim
                                                                        ps7.setString(10, DString + " " + timeString);// modified_at
                                                                        ps7.setString(11, DString + " " + timeString);// created_at
                                                                        ps7.setString(12, DString + " " + timeString);// updated_at
                                                                        ps7.executeUpdate();                //  777777777  執行   INSERT
                                                                        //////////////////////////////////////////////////////////
                                                                        //  add to account_product_groups
                                                                        //*****************************
                                                                        ResultSet a_p_g_id = ps7.getGeneratedKeys();
                                                                        a_p_g_id.next();
                                                                        int a_p_g_lastid = a_p_g_id.getInt(1);
                                                                        System.out.println(rs.getRow() + " 新增 account_product_group_id 第1年 : " + a_p_g_lastid);
                                                                        con7.close();
                                                                    } catch (SQLException ex) {
                                                                        System.out.println("str7 第1年 account_product_groups 錯誤 " + ex.getMessage());
                                                                    }

                                                                    enable_day = expired_day.substring(0, 10) + " 00:00:00"; //2021-09-15 23:59:59
                                                                    cal.add(Calendar.YEAR, +1);                         //取當前日期+1 年
                                                                    T0 = format.format(cal.getTime());
                                                                    expired_day = T0.substring(0, 10) + " 23:59:59";         //2022-09-15 23:59:59
                                                                    try (Connection con7_1 = DriverManager.getConnection(url2, user2, pass2);
                                                                            PreparedStatement ps7_1 = con7_1.prepareStatement(str7)) {
                                                                        ps7_1.setInt(1, accountlastid);      //  account_id
                                                                        ps7_1.setInt(2, a_product_lastid);   //  account_product_id
                                                                        ps7_1.setInt(3, rs4.getInt(1));      //  product_id
                                                                        ps7_1.setInt(4, rs4.getInt(2));      //  product_main_id
                                                                        ps7_1.setInt(5, rs6.getInt(1));      //  product_group_id
                                                                        ps7_1.setInt(6, rs6.getInt(3));      //  limit_count
                                                                        ps7_1.setInt(7, rs6.getInt(3));      //  limit_count_original
                                                                        ps7_1.setString(8, enable_day);      //  group_enable_datetime
                                                                        ps7_1.setString(9, expired_day);     //  group_expired_datetim
                                                                        ps7_1.setString(10, DString + " " + timeString);// modified_at
                                                                        ps7_1.setString(11, DString + " " + timeString);// created_at
                                                                        ps7_1.setString(12, DString + " " + timeString);// updated_at
                                                                        ps7_1.executeUpdate();                //  777777777  執行   INSERT
                                                                        //////////////////////////////////////////////////////////
                                                                        //  add to account_product_groups
                                                                        //*****************************
                                                                        ResultSet a_p_g_id = ps7_1.getGeneratedKeys();
                                                                        a_p_g_id.next();
                                                                        int a_p_g_lastid = a_p_g_id.getInt(1);
                                                                        System.out.println(rs.getRow() + " 新增 account_product_group_id 第2年 : " + a_p_g_lastid);
                                                                        con7_1.close();
                                                                    } catch (SQLException ex) {
                                                                        System.out.println("str7_1 第2年 account_product_groups 錯誤 " + ex.getMessage());
                                                                    }

                                                                    enable_day = expired_day.substring(0, 10) + " 00:00:00"; //2022-09-15 00:00:00
                                                                    cal.add(Calendar.YEAR, +1);                          //取當前日期的+1 YEAR
                                                                    T0 = format.format(cal.getTime());
                                                                    expired_day = T0.substring(0, 10) + " 23:59:59";         //2023-09-15 23:59:59

                                                                    try (Connection con7_2 = DriverManager.getConnection(url2, user2, pass2);
                                                                            PreparedStatement ps7_2 = con7_2.prepareStatement(str7)) {
                                                                        ps7_2.setInt(1, accountlastid);      //  account_id
                                                                        ps7_2.setInt(2, a_product_lastid);   //  account_product_id
                                                                        ps7_2.setInt(3, rs4.getInt(1));      //  product_id
                                                                        ps7_2.setInt(4, rs4.getInt(2));      //  product_main_id
                                                                        ps7_2.setInt(5, rs6.getInt(1));      //  product_group_id
                                                                        ps7_2.setInt(6, rs6.getInt(3));      //  limit_count
                                                                        ps7_2.setInt(7, rs6.getInt(3));      //  limit_count_original
                                                                        ps7_2.setString(8, enable_day);      //  group_enable_datetime
                                                                        ps7_2.setString(9, expired_day);     //  group_expired_datetim
                                                                        ps7_2.setString(10, DString + " " + timeString);// modified_at
                                                                        ps7_2.setString(11, DString + " " + timeString);// created_at
                                                                        ps7_2.setString(12, DString + " " + timeString);// updated_at
                                                                        ps7_2.executeUpdate();               //  777777777  執行   INSERT
                                                                        //////////////////////////////////////////////////////////
                                                                        //  add to account_product_groups
                                                                        //*****************************
                                                                        ResultSet a_p_g_id = ps7_2.getGeneratedKeys();
                                                                        a_p_g_id.next();
                                                                        int a_p_g_lastid = a_p_g_id.getInt(1);
                                                                        System.out.println(rs.getRow() + " 新增 account_product_group_id 第3年 : " + a_p_g_lastid);
                                                                        con7_2.close();
                                                                    } catch (SQLException ex) {
                                                                        System.out.println("str7_2 第3年 account_product_groups 錯誤 " + ex.getMessage());
                                                                    }
                                                                    if (period == 48) {
                                                                        enable_day = expired_day.substring(0, 10) + " 00:00:00"; //2023-09-15 00:00:00
                                                                        cal.add(Calendar.YEAR, +1);                          //取當前日期的+1 YEAR
                                                                        T0 = format.format(cal.getTime());
                                                                        expired_day = T0.substring(0, 10) + " 23:59:59";         //2024-09-15 23:59:59

                                                                        try (Connection con7_3 = DriverManager.getConnection(url2, user2, pass2);
                                                                                PreparedStatement ps7_3 = con7_3.prepareStatement(str7)) {
                                                                            ps7_3.setInt(1, accountlastid);      //  account_id
                                                                            ps7_3.setInt(2, a_product_lastid);   //  account_product_id
                                                                            ps7_3.setInt(3, rs4.getInt(1));      //  product_id
                                                                            ps7_3.setInt(4, rs4.getInt(2));      //  product_main_id
                                                                            ps7_3.setInt(5, rs6.getInt(1));      //  product_group_id
                                                                            ps7_3.setInt(6, rs6.getInt(3));      //  limit_count
                                                                            ps7_3.setInt(7, rs6.getInt(3));      //  limit_count_original
                                                                            ps7_3.setString(8, enable_day);      //  group_enable_datetime
                                                                            ps7_3.setString(9, expired_day);     //  group_expired_datetim
                                                                            ps7_3.setString(10, DString + " " + timeString);// modified_at
                                                                            ps7_3.setString(11, DString + " " + timeString);// created_at
                                                                            ps7_3.setString(12, DString + " " + timeString);// updated_at
                                                                            ps7_3.executeUpdate();                //  777777777  執行   INSERT
                                                                            //////////////////////////////////////////////////////////
                                                                            //  add to account_product_groups
                                                                            //*****************************
                                                                            ResultSet a_p_g_id = ps7_3.getGeneratedKeys();
                                                                            a_p_g_id.next();
                                                                            int a_p_g_lastid = a_p_g_id.getInt(1);
                                                                            System.out.println(rs.getRow() + " 新增 account_product_group_id 第4年 : " + a_p_g_lastid);
                                                                            con7_3.close();
                                                                        } catch (SQLException ex) {
                                                                            System.out.println("str7_3 第4年 account_product_groups 錯誤 " + ex.getMessage());
                                                                        }
                                                                    }
                                                                } else // 1年合約
                                                                {
                                                                    try (Connection con7 = DriverManager.getConnection(url2, user2, pass2);
                                                                            PreparedStatement ps7 = con7.prepareStatement(str7)) {
                                                                        ps7.setInt(1, accountlastid);      //  account_id
                                                                        ps7.setInt(2, a_product_lastid);   //  account_product_id
                                                                        ps7.setInt(3, rs4.getInt(1));      //  product_id
                                                                        ps7.setInt(4, rs4.getInt(2));      //  product_main_id
                                                                        ps7.setInt(5, rs6.getInt(1));      //  product_group_id
                                                                        ps7.setInt(6, rs6.getInt(3));      //  limit_count
                                                                        ps7.setInt(7, rs6.getInt(3));      //  limit_count_original
                                                                        ps7.setString(8, enable_day);      //  group_enable_datetime
                                                                        ps7.setString(9, expired_day);     //  group_expired_datetim
                                                                        ps7.setString(10, DString + " " + timeString);        // modified_at
                                                                        ps7.setString(11, DString + " " + timeString);        // created_at
                                                                        ps7.setString(12, DString + " " + timeString);        // updated_at
                                                                        ps7.executeUpdate();                //  777777777  執行   INSERT
                                                                        //////////////////////////////////////////////////////////
                                                                        //  add to account_product_groups
                                                                        //*****************************
                                                                        ResultSet a_p_g_id = ps7.getGeneratedKeys();
                                                                        a_p_g_id.next();
                                                                        int a_p_g_lastid = a_p_g_id.getInt(1);
                                                                        System.out.println(rs.getRow() + " 新增 account_product_group_id 1年合約: " + a_p_g_lastid);
                                                                        con7.close();
                                                                    } catch (SQLException ex) {
                                                                        System.out.println("str7 1年合約 account_product_groups 錯誤 " + ex.getMessage());
                                                                    }
                                                                }
                                                            }
                                                            con6.close();
                                                        } catch (SQLException ex) {
                                                            System.out.println("str6 SELECT product_main_id 錯誤 " + ex.getMessage());
                                                        }
                                                        con5.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("str5 INSERT account_products 錯誤 " + ex.getMessage() + str5);
                                                    }
                                                    manage_ai_report(rs.getRow(), "0", buy_mode, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(12), rs.getString(13), rs.getString(14), rs.getString(21), rs.getString(20), rs.getString(22), cell_phone);
                                                    con4.close();
                                                } catch (SQLException ex) {
                                                    System.out.println("str4 SELECT erp_ai 錯誤 " + ex.getMessage());
                                                }
                                                con3.close();
                                            } catch (SQLException ex) {
                                                System.out.println("str3 INSERT account_devices 錯誤 " + ex.getMessage() + str3);
                                            }
                                            con2.close();
                                        } catch (SQLException ex) {
                                            System.out.println("str2 INSERT accounts 錯誤 " + ex.getMessage() + str2);
                                        }
                                    } else //////////////////////////////renew   查 accout_id  舊合約的最新到期日
                                    //  有accout_id   是續約客戶模式  查account_products 延用舊合約日期 更新  buy  enable  expire
                                    {
                                        purchase_order_type = 1;
                                        //String url2 = "jdbc:mysql://10.20.0.10:3306/car?characterEncoding=UTF-8";
                                        //String user2 = "intit2020303";
                                        ///String url2 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
                                        ///String user2 = "intit-20200303";//"intit20200303";for localhost & 10.20.0.9
                                        ///String pass2 = "jes#2354";
                                        //int year;//  = Integer.parseInt(DString.substring(0,4)); //取得 ERP 訂單年
                                        //int month;// = Integer.parseInt(DString.substring(5,7)); //取得 ERP 訂單月
                                        //int date;//  = Integer.parseInt(DString.substring(8,10));//取得 ERP 訂單日
                                        Calendar cal = Calendar.getInstance();          //使用預設時區和語言環境獲得日曆
                                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  //通過格式化輸出日期

                                        String str_renew = "SELECT account_product_expire_datetime,product_main_name,account_id FROM account_products a "
                                                + "JOIN product_main b on a.product_main_id=b.product_main_id "
                                                + "WHERE account_id='" + account_id_renew + "' "
                                                + "ORDER BY account_product_expire_datetime DESC";
                                        try (Connection conn_renew = DriverManager.getConnection(url2, user2, pass2)) {
                                            Statement st_renew = conn_renew.createStatement();          //獲取用於向資料庫發送 SQL 語法的 Statement
                                            ResultSet rs_renew = st_renew.executeQuery(str_renew);      //6666666666
                                            rs_renew.next();

                                            String a_p_enable_day = rs_renew.getString(1).substring(0, 19); //續約客戶到期日  等於啟用日
                                            old_expired_day = rs_renew.getString(1).substring(0, 10);        //續約客戶到期日
                                            old_product_main_name = rs_renew.getString(2);                       //續約客舊配套
                                            System.out.println(rs.getRow() + " 續約到期日 : " + a_p_enable_day);   //到期日

                                            Calendar c = Calendar.getInstance();
                                            int syear = Integer.parseInt(a_p_enable_day.substring(0, 4));   //續約客戶到期年
                                            int smonth = Integer.parseInt(a_p_enable_day.substring(5, 7));  //續約客戶到期月
                                            int sdate = Integer.parseInt(a_p_enable_day.substring(8, 10));  //續約客戶到期日
                                            int shour = Integer.parseInt(a_p_enable_day.substring(11, 13)); //續約客戶到期
                                            int sminute = Integer.parseInt(a_p_enable_day.substring(14, 16));//續約客戶到期
                                            int ssecond = Integer.parseInt(a_p_enable_day.substring(17, 19));//續約客戶到期

                                            c.set(syear, smonth - 1, sdate, shour, sminute, ssecond); //取回起用日+1 Sec    H m s //2020-10-05 23:59:59
                                            c.add(Calendar.SECOND, 1);
                                            Date dd = c.getTime();
                                            a_p_enable_day = format.format(dd);

                                            String a_p_today = format.format(cal.getTime()).substring(0, 10);//取得今日
                                            int compareTo = a_p_today.compareTo(a_p_enable_day);//今日大於到期日  為正數 表示合約過期了
                                            //System.out.println(rs.getRow()+" 今日 比較 ERP訂單 正數 啟用日是今天:"+compareTo);
                                            if (compareTo > 0) {  //今日大於到期日 正數 表示合約日到期   啟用日是今天
                                                a_p_enable_day = a_p_today;
                                            } else {
                                                System.out.println(rs.getRow() + " 今天日期 : " + a_p_today);
                                            }
                                            //a_p_enable_day=a_p_enable_day+" 00:00:00";
                                            //今日小於到期日 負數 表示合約未到期  啟用日 是 續約客戶到期日
                                            //a_p_enable_day=DString.substring(0,10)+" 00:00:00";; // 啟用日是ＥＲＰ訂單日
                                            //*************************************************************************
                                            // add to account_products
                                            // prepare  Product_ID    Product_Main_ID
                                            String url_erp_ai = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
                                            String user_erp_ai = "intit20200303"; //intit20200303 for localhost;
                                            String pass_erp_ai = "jes#2354";

                                            String str4_renew = "SELECT product_id,product_main_id,period,ctc_code FROM erp_ai WHERE TT_ERP_ID='" + rs.getString(12) + "' ";
                                            try (Connection con4_renew = DriverManager.getConnection(url_erp_ai, user_erp_ai, pass_erp_ai);
                                                    Statement st1 = con4_renew.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                                                    ResultSet rs4_renew = st1.executeQuery(str4_renew)) // 444444444_renew
                                            {
                                                rs4_renew.next();
                                                //System.out.println(rs.getRow()+"  p_id:"+rs4_renew.getInt(1)+" p_m_id:"+rs4_renew.getInt(2)+" period:"+rs4_renew.getInt(3)+" ctc_code:"+rs4_renew.getString(4));
                                                int product_main_id_nm_renew = rs4_renew.getInt(2);  //product_main_id_nm 為了和 product_main_id 有區別
                                                int period = rs4_renew.getInt(3);  //period
                                                String ctc = rs4_renew.getString(4);
                                                int year = Integer.parseInt(a_p_enable_day.substring(0, 4)); //續約客戶到期年
                                                int month = Integer.parseInt(a_p_enable_day.substring(5, 7)); //續約客戶到期月
                                                int date = Integer.parseInt(a_p_enable_day.substring(8, 10));//續約客戶到期日

                                                String a_p_expired_day, T0;  //準備續約客戶的到期日變數字串
                                                cal.set(year, month - 1, date); //設定日歷為續約客戶的日期  month  0~11  所以 0是1月  11是12月
                                                if ((period == 36) || (period == 48)) // 3年合約的
                                                {
                                                    cal.add(Calendar.YEAR, +3);//續約客戶到期年+3
                                                    T0 = format.format(cal.getTime());
                                                    //T0=T0.substring(0,10)+" 00:00:00";
                                                    a_p_expired_day = T0.substring(0, 10) + " 23:59:59";

                                                    if (period == 48) {
                                                        cal.add(Calendar.YEAR, +1);//續約客戶到期年+4
                                                        T0 = format.format(cal.getTime());
                                                        //T0=T0.substring(0,10)+" 00:00:00";
                                                        a_p_expired_day = T0.substring(0, 10) + " 23:59:59";
                                                    }
                                                } else {
                                                    cal.add(Calendar.YEAR, +1);   //取當前日期+1 年
                                                    T0 = format.format(cal.getTime());
                                                    //T0=T0.substring(0,10)+" 00:00:00";
                                                    a_p_expired_day = T0.substring(0, 10) + " 23:59:59";
                                                }

                                                System.out.println(rs.getRow() + " 續約啟用日 : " + a_p_enable_day + " 續約到期日 : " + a_p_expired_day);
                                                enString = a_p_enable_day.substring(0, 10);
                                                String S1 = check_CTC_data("1", period, rs.getString(2), rs.getString(9), rs.getString(10), DString, a_p_enable_day.substring(0, 10), a_p_expired_day.substring(0, 10), ctc); //A05026 // 檢查CTC_account
                                                String str5_renew = "INSERT INTO account_products(account_id,product_id,product_main_id,"
                                                        + "account_product_buy_datetime,account_product_enable_datetime,account_product_expire_datetime,account_product_buy_mode,erp_order_number,purchase_order_type,modified_at,created_at,updated_at,expiration_alert_notify_count) "
                                                        + "values (?,?,?,?,?,?,?,?,?,?,?,?,0)";     //  注意新加的到期提示欄位用 內定值 0
                                                try (Connection con5_renew = DriverManager.getConnection(url2, user2, pass2);
                                                        PreparedStatement ps5_renew = con5_renew.prepareStatement(str5_renew)) {
                                                    ps5_renew.setInt(1, Integer.parseInt(account_id_renew));//  account_id
                                                    ps5_renew.setInt(2, rs4_renew.getInt(1));     //  product_id
                                                    ps5_renew.setInt(3, rs4_renew.getInt(2));     //  product_main_id
                                                    ps5_renew.setString(4, DString + " " + timeString); //  buy_datetime
                                                    ps5_renew.setString(5, a_p_enable_day);         //  eable_datetime
                                                    ps5_renew.setString(6, a_p_expired_day);        //  expired_datetime
                                                    ps5_renew.setString(7, buy_mode);               //  1直購2油配
                                                    ps5_renew.setString(8, erp_order_number);      //  ERP訂單編號
                                                    ps5_renew.setInt(9, purchase_order_type);      //  0:新增 1:續約 2:G3續約 3:續約轉300家族
                                                    ps5_renew.setString(10, DString + " " + timeString); //  modified_at
                                                    ps5_renew.setString(11, DString + " " + timeString); //  created_at
                                                    ps5_renew.setString(12, DString + " " + timeString);//  updated_at
                                                    //ps5_renew.setInt   (11, 0);                    //   新加的到期提示欄位先用 內定值 0
                                                    ps5_renew.executeUpdate();                     // 555555555  執行   INSERT
                                                    //////////////////////////////////////////////////////////
                                                    //  add to account_product_groups
                                                    //////////////////////////////////////////////////////////
                                                    ResultSet a_product_id_renew = ps5_renew.getGeneratedKeys();
                                                    a_product_id_renew.next();
                                                    int a_product_lastid_renew = a_product_id_renew.getInt(1);
                                                    System.out.println(rs.getRow() + " 新增 account_product_id_renew : " + a_product_lastid_renew);
                                                    /////////////////////////////////////////////////////////////////////////////////////////////////////
                                                    String sql_update3_renew = "UPDATE manage_po SET account_product_id = '" + a_product_lastid_renew + "'"
                                                            + "WHERE manage_id='" + manageid + "';";
                                                    try (Connection conn_update = DriverManager.getConnection(url_managedb, user_managedb, pass_managedb)) {
                                                        Statement st3 = conn_update.createStatement();
                                                        st3.executeUpdate(sql_update3_renew);
                                                        conn_update.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("sql_update3_renew manage_po 錯誤 " + ex.getMessage());
                                                    }
                                                    /////////////////////////////////////////////////////////////////////////////////////////////////////
                                                    //  找出  PRODUCT_GROOUP_ID   用 PRODUCT_MAIN_ID
                                                    String str6_renew = "SELECT product_group_id,product_main_id,limit_count FROM product_groups WHERE product_main_id='" + product_main_id_nm_renew + "' ";
                                                    try (Connection con6_renew = DriverManager.getConnection(url2, user2, pass2)) {
                                                        Statement st6_renew = con6_renew.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                                                        ResultSet rs6_renew = st6_renew.executeQuery(str6_renew); //6666666666_renew

                                                        year = Integer.parseInt(a_p_enable_day.substring(0, 4)); //續約客戶到期年
                                                        month = Integer.parseInt(a_p_enable_day.substring(5, 7)); //續約客戶到期月
                                                        date = Integer.parseInt(a_p_enable_day.substring(8, 10));//續約客戶到期日

                                                        while (rs6_renew.next()) {
                                                            cal.set(year, month - 1, date); //取回起用日  //2020-09-15
                                                            //System.out.println(rs6.getRow()+" "+rs6.getInt(1)+" "+rs6.getInt(2)+" "+rs6.getInt(3));
                                                            String str7_renew = "INSERT INTO account_product_groups ("
                                                                    + "account_id,account_product_id,product_id,product_main_id,"
                                                                    + "product_group_id,group_limit_count,group_limit_count_original,"
                                                                    + "group_enable_datetime,group_expire_datetime,modified_at,created_at,updated_at) "
                                                                    + "values (?,?,?,?,?,?,?,?,?,?,?,?)";
                                                            /////////1 2 3 4 5 6 7 8 9 0 1 2
                                                            //String enable_day;
                                                            if (period == 36 || period == 48) // 三年 or
                                                            {
                                                                T0 = format.format(cal.getTime());
                                                                a_p_enable_day = T0.substring(0, 10) + " 00:00:00";
                                                                cal.add(Calendar.YEAR, +1);   //取當前日期+1 年
                                                                T0 = format.format(cal.getTime());
                                                                a_p_expired_day = T0.substring(0, 10) + " 23:59:59";
                                                                try (Connection con7_renew = DriverManager.getConnection(url2, user2, pass2);
                                                                        PreparedStatement ps7_renew = con7_renew.prepareStatement(str7_renew)) {
                                                                    ps7_renew.setInt(1, Integer.parseInt(account_id_renew));      //  account_id
                                                                    ps7_renew.setInt(2, a_product_lastid_renew);   //  a_ccount_product_id
                                                                    ps7_renew.setInt(3, rs4_renew.getInt(1));      //  product_id
                                                                    ps7_renew.setInt(4, rs4_renew.getInt(2));      //  product_main_id
                                                                    ps7_renew.setInt(5, rs6_renew.getInt(1));      //  product_group_id
                                                                    ps7_renew.setInt(6, rs6_renew.getInt(3));      //  limit_count
                                                                    ps7_renew.setInt(7, rs6_renew.getInt(3));      //  limit_count
                                                                    ps7_renew.setString(8, a_p_enable_day);        //  enable_datetime
                                                                    ps7_renew.setString(9, a_p_expired_day);       //  expired_datetime
                                                                    ps7_renew.setString(10, DString + " " + timeString);      //  modified_at
                                                                    ps7_renew.setString(11, DString + " " + timeString);      //  created_at
                                                                    ps7_renew.setString(12, DString + " " + timeString);      //  updated_at
                                                                    ps7_renew.executeUpdate();                      //  777777777  執行   INSERT
                                                                    //////////////////////////////////////////////////////////
                                                                    //  add to account_product_groups
                                                                    //////////////////////////////////////////////////////////
                                                                    ResultSet a_p_g_id_renew = ps7_renew.getGeneratedKeys();
                                                                    a_p_g_id_renew.next();
                                                                    int a_p_g_lastid_renew = a_p_g_id_renew.getInt(1);
                                                                    System.out.println(rs.getRow() + " 新增 account_product_group_id_renew 第1年 : " + a_p_g_lastid_renew);
                                                                    con7_renew.close();
                                                                } catch (SQLException ex) {
                                                                    System.out.println("str7_renew 錯誤 " + ex.getMessage());
                                                                }

                                                                a_p_enable_day = a_p_expired_day.substring(0, 10) + " 00:00:00";
                                                                cal.add(Calendar.YEAR, +1);   //取當前日期+1 年
                                                                T0 = format.format(cal.getTime());
                                                                a_p_expired_day = T0.substring(0, 10) + " 23:59:59";
                                                                try (Connection con7_1_renew = DriverManager.getConnection(url2, user2, pass2);
                                                                        PreparedStatement ps7_1_renew = con7_1_renew.prepareStatement(str7_renew)) {
                                                                    ps7_1_renew.setInt(1, Integer.parseInt(account_id_renew));      //  account_id
                                                                    ps7_1_renew.setInt(2, a_product_lastid_renew);   //  account_product_id
                                                                    ps7_1_renew.setInt(3, rs4_renew.getInt(1));      //  product_id
                                                                    ps7_1_renew.setInt(4, rs4_renew.getInt(2));      //  product_main_id
                                                                    ps7_1_renew.setInt(5, rs6_renew.getInt(1));      //  product_group_id
                                                                    ps7_1_renew.setInt(6, rs6_renew.getInt(3));      //  limit_count
                                                                    ps7_1_renew.setInt(7, rs6_renew.getInt(3));      //  limit_count_original
                                                                    ps7_1_renew.setString(8, a_p_enable_day);        //  group_enable_datetime
                                                                    ps7_1_renew.setString(9, a_p_expired_day);       //  group_expired_datetim
                                                                    ps7_1_renew.setString(10, DString + " " + timeString);     // modified_at
                                                                    ps7_1_renew.setString(11, DString + " " + timeString);     // created_at
                                                                    ps7_1_renew.setString(12, DString + " " + timeString);     // updated_at
                                                                    ps7_1_renew.executeUpdate();                      //  777777777  執行   INSERT
                                                                    //////////////////////////////////////////////////////////
                                                                    //  add to account_product_groups
                                                                    //////////////////////////////////////////////////////////
                                                                    ResultSet a_p_g_id = ps7_1_renew.getGeneratedKeys();
                                                                    a_p_g_id.next();
                                                                    int a_p_g_lastid_renew = a_p_g_id.getInt(1);
                                                                    System.out.println(rs.getRow() + " 新增 account_product_group_id_renew 第2年 : " + a_p_g_lastid_renew);
                                                                    con7_1_renew.close();
                                                                } catch (SQLException ex) {
                                                                    System.out.println("str7_1 錯誤 " + ex.getMessage());
                                                                }
                                                                //2022-09-15-23:59:59
                                                                a_p_enable_day = a_p_expired_day.substring(0, 10) + " 00:00:00";
                                                                cal.add(Calendar.YEAR, +1);//取當前日期的+1
                                                                T0 = format.format(cal.getTime());
                                                                a_p_expired_day = T0.substring(0, 10) + " 23:59:59";
                                                                try (Connection con7_2_renew = DriverManager.getConnection(url2, user2, pass2);
                                                                        PreparedStatement ps7_2_renew = con7_2_renew.prepareStatement(str7_renew)) {
                                                                    ps7_2_renew.setInt(1, Integer.parseInt(account_id_renew));      //  account_id
                                                                    ps7_2_renew.setInt(2, a_product_lastid_renew);   //  account_product_id
                                                                    ps7_2_renew.setInt(3, rs4_renew.getInt(1));      //  product_id
                                                                    ps7_2_renew.setInt(4, rs4_renew.getInt(2));      //  product_main_id
                                                                    ps7_2_renew.setInt(5, rs6_renew.getInt(1));      //  product_group_id
                                                                    ps7_2_renew.setInt(6, rs6_renew.getInt(3));      //  limit_count
                                                                    ps7_2_renew.setInt(7, rs6_renew.getInt(3));      //  limit_count_original
                                                                    ps7_2_renew.setString(8, a_p_enable_day);        //  group_enable_datetime
                                                                    ps7_2_renew.setString(9, a_p_expired_day);       //  group_expired_datetim
                                                                    ps7_2_renew.setString(10, DString + " " + timeString);     // modified_at
                                                                    ps7_2_renew.setString(11, DString + " " + timeString);     // created_at
                                                                    ps7_2_renew.setString(12, DString + " " + timeString);     // updated_at
                                                                    ps7_2_renew.executeUpdate();                      //  777777777  執行   INSERT
                                                                    //////////////////////////////////////////////////////////
                                                                    //  add to account_product_groups
                                                                    /////////////////////////////////////////////////////////
                                                                    ResultSet a_p_g_id_renew = ps7_2_renew.getGeneratedKeys();
                                                                    a_p_g_id_renew.next();
                                                                    int a_p_g_lastid_renew = a_p_g_id_renew.getInt(1);
                                                                    System.out.println(rs.getRow() + " 新增 account_product_group_id_renew 第3年 : " + a_p_g_lastid_renew);
                                                                    con7_2_renew.close();
                                                                } catch (SQLException ex) {
                                                                    System.out.println("str7_2_renew 錯誤 " + ex.getMessage());
                                                                }
                                                                if (period == 48) {
                                                                    a_p_enable_day = a_p_expired_day.substring(0, 10) + " 00:00:00";
                                                                    cal.add(Calendar.YEAR, +1);//取當前日期的+1
                                                                    T0 = format.format(cal.getTime());
                                                                    a_p_expired_day = T0.substring(0, 10) + " 23:59:59";
                                                                    try (Connection con7_3_renew = DriverManager.getConnection(url2, user2, pass2);
                                                                            PreparedStatement ps7_3_renew = con7_3_renew.prepareStatement(str7_renew)) {
                                                                        ps7_3_renew.setInt(1, Integer.parseInt(account_id_renew));      //  account_id
                                                                        ps7_3_renew.setInt(2, a_product_lastid_renew);   //  account_product_id
                                                                        ps7_3_renew.setInt(3, rs4_renew.getInt(1));      //  product_id
                                                                        ps7_3_renew.setInt(4, rs4_renew.getInt(2));      //  product_main_id
                                                                        ps7_3_renew.setInt(5, rs6_renew.getInt(1));      //  product_group_id
                                                                        ps7_3_renew.setInt(6, rs6_renew.getInt(3));      //  limit_count
                                                                        ps7_3_renew.setInt(7, rs6_renew.getInt(3));      //  limit_count_original
                                                                        ps7_3_renew.setString(8, a_p_enable_day);        //  group_enable_datetime
                                                                        ps7_3_renew.setString(9, a_p_expired_day);       //  group_expired_datetim
                                                                        ps7_3_renew.setString(10, DString + " " + timeString);     // modified_at
                                                                        ps7_3_renew.setString(11, DString + " " + timeString);     // created_at
                                                                        ps7_3_renew.setString(12, DString + " " + timeString);     // updated_at
                                                                        ps7_3_renew.executeUpdate();                      //  777777777  執行   INSERT
                                                                        //////////////////////////////////////////////////////////
                                                                        //  add to account_product_groups
                                                                        //////////////////////////////////////////////////////////
                                                                        ResultSet a_p_g_id_renew = ps7_3_renew.getGeneratedKeys();
                                                                        a_p_g_id_renew.next();
                                                                        int a_p_g_lastid_renew = a_p_g_id_renew.getInt(1);
                                                                        System.out.println(rs.getRow() + " 新增 account_product_group_id_renew 第4年 : " + a_p_g_lastid_renew);
                                                                        con7_3_renew.close();
                                                                    } catch (SQLException ex) {
                                                                        System.out.println("str7_3_renew 錯誤 " + ex.getMessage());
                                                                    }
                                                                }
                                                            } else {
                                                                //a_p_enable_day=DString.substring(0,10)+" 00:00:00";
                                                                try (Connection con7_renew = DriverManager.getConnection(url2, user2, pass2);
                                                                        PreparedStatement ps7_renew = con7_renew.prepareStatement(str7_renew)) {
                                                                    ps7_renew.setInt(1, Integer.parseInt(account_id_renew));      //  account_id
                                                                    ps7_renew.setInt(2, a_product_lastid_renew);   //  account_product_id
                                                                    ps7_renew.setInt(3, rs4_renew.getInt(1));      //  product_id
                                                                    ps7_renew.setInt(4, rs4_renew.getInt(2));      //  product_main_id
                                                                    ps7_renew.setInt(5, rs6_renew.getInt(1));      //  product_group_id
                                                                    ps7_renew.setInt(6, rs6_renew.getInt(3));      //  limit_count
                                                                    ps7_renew.setInt(7, rs6_renew.getInt(3));      //  limit_count_original
                                                                    ps7_renew.setString(8, a_p_enable_day);        //  group_enable_datetime
                                                                    ps7_renew.setString(9, a_p_expired_day);       //  group_expired_datetim
                                                                    ps7_renew.setString(10, DString + " " + timeString);     // modified_at
                                                                    ps7_renew.setString(11, DString + " " + timeString);     // created_at
                                                                    ps7_renew.setString(12, DString + " " + timeString);     // updated_at
                                                                    ps7_renew.executeUpdate();                      //  777777777  執行   INSERT
                                                                    //////////////////////////////////////////////////////////
                                                                    //  add to account_product_groups
                                                                    //////////////////////////////////////////////////////////
                                                                    ResultSet a_p_g_id_renew = ps7_renew.getGeneratedKeys();
                                                                    a_p_g_id_renew.next();
                                                                    int a_p_g_lastid_renew = a_p_g_id_renew.getInt(1);
                                                                    System.out.println(rs.getRow() + " 新增 account_product_group_id_renew 1年: " + a_p_g_lastid_renew);
                                                                    con7_renew.close();
                                                                } catch (SQLException ex) {
                                                                    System.out.println("str7_renew 錯誤 " + ex.getMessage());
                                                                }
                                                            }
                                                        }
                                                        con6_renew.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("str6_renew SELECT product_main_id 錯誤 " + ex.getMessage() + str6_renew);
                                                    }
                                                    con5_renew.close();
                                                } catch (SQLException ex) {
                                                    System.out.println("str5_renew INSERT account_products 錯誤 " + ex.getMessage() + str5_renew);
                                                }
                                                manage_ai_report(rs.getRow(), "0", buy_mode, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(12), rs.getString(13), rs.getString(14), rs.getString(21), rs.getString(20), rs.getString(22), cell_phone);
                                                con4_renew.close();
                                            } catch (SQLException ex) {
                                                System.out.println("str4_renew SELECT erp_ai錯誤 " + ex.getMessage());
                                            }
                                            conn_renew.close();
                                        } catch (SQLException ex) {
                                            System.out.println("str_renew SELECT account_products 錯誤 " + ex.getMessage());
                                        }
                                        //System.out.println(rs.getRow() + " "+ account_id_renew);
                                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                        //Begining Add_point   searh erp point number
                                        String url_erp_ai = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
                                        String user_erp_ai = "intit20200303";
                                        String pass_erp_ai = "jes#2354";
                                        String str_po = "SELECT point,TT_ERP_ID,TT_ERP_NAME FROM erp_ai WHERE TT_ERP_ID= '" + rs.getString(12) + "' ";
                                        try (Connection con_po = DriverManager.getConnection(url_erp_ai, user_erp_ai, pass_erp_ai);
                                                Statement st = con_po.createStatement();
                                                ResultSet rs_po = st.executeQuery(str_po)) {
                                            rs_po.next();

                                            int erp_point = rs_po.getInt(1); //TT_ERP_NAME point
                                            int account_point_left = erp_point;
                                            String TT_ERP_NAME = rs_po.getString(3); //
                                            System.out.println(rs.getRow() + " " + rs_po.getString(2) + " " + TT_ERP_NAME + " 點數:" + erp_point);
                                            // search account_point table  point nunber
                                            String str_point0 = "SELECT account_point_id,account_id,account_point "
                                                    + "FROM account_point WHERE account_id='" + account_id_renew + "' ";
                                            try (Connection con_point = DriverManager.getConnection(url2, user2, pass2)) {
                                                Statement st_point = con_point.createStatement();
                                                ResultSet rs_point = st_point.executeQuery(str_point0);
                                                if (rs_point.next()) //have account_point
                                                {
                                                    int a_point_id = rs_point.getInt(1);
                                                    int account_id = rs_point.getInt(2);
                                                    int account_point = rs_point.getInt(3);

                                                    System.out.println(rs.getRow() + " account_point_id:" + a_point_id + " account_id:" + account_id + " 點數:" + account_point);

                                                    account_point += erp_point;
                                                    account_point_left = account_point;
                                                    String str_update_po = "UPDATE account_point SET account_point = '" + account_point + "' WHERE account_id='" + account_id_renew + "' ";
                                                    try (Connection con_update_po = DriverManager.getConnection(url2, user2, pass2)) {
                                                        Statement st1 = con_update_po.createStatement();
                                                        st1.executeUpdate(str_update_po);
                                                        con_update_po.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("str_update_point0 錯誤 " + ex.getMessage());
                                                    }
                                                    //insert into point_history
                                                    String str_repoint_his = "INSERT INTO account_point_history"
                                                            + "(action_type,use_type,account_point,account_point_id,account_point_left,account_point_use_comment,use_type_description,created_at,updated_at,modified_at) "
                                                            + "VALUES (1,0,?,?,?,?,?,?,?,?) ";
                                                    //   1,2,3,4,5,6,7
                                                    try (Connection con_repoint_his = DriverManager.getConnection(url2, user2, pass2);
                                                            PreparedStatement ps_repoint_his = con_repoint_his.prepareStatement(str_repoint_his)) {
                                                        ps_repoint_his.setInt(1, erp_point);             // account_id
                                                        ps_repoint_his.setInt(2, a_point_id);    // account_point_id
                                                        ps_repoint_his.setInt(3, account_point_left);        // account_point_left
                                                        ps_repoint_his.setString(4, "購買產品:" + TT_ERP_NAME);  // account_point_use_commment
                                                        ps_repoint_his.setString(5, "點數獲得");                 // use_type_description
                                                        ps_repoint_his.setString(6, DString + " " + timeString); // created_at
                                                        ps_repoint_his.setString(7, DString + " " + timeString); // updated_at
                                                        ps_repoint_his.setString(8, DString + " " + timeString); // modified_at
                                                        ps_repoint_his.executeUpdate();                          // 執行 INSERT
                                                        ResultSet account_repoint_his_id = ps_repoint_his.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY KEY
                                                        account_repoint_his_id.next();
                                                        int account_repoint_his_lastid = account_repoint_his_id.getInt(1);   // 字串轉數字
                                                        System.out.println(rs.getRow() + " 新增 account_point_history_id : " + account_repoint_his_lastid);
                                                        con_repoint_his.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("  !!! INSERT INTO account_point_history 錯誤 " + ex.getMessage() + str_repoint_his);
                                                    }
                                                } else //all new contract point
                                                {   ////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                    /////////////  add to account_point
                                                    String str_repoint = "INSERT INTO account_point(account_id,account_point,created_at,modified_at,updated_at) "
                                                            + "VALUES ('" + account_id_renew + "',?,?,?,?)";
                                                    try (Connection con_repoint = DriverManager.getConnection(url2, user2, pass2);
                                                            PreparedStatement ps_repoint = con_repoint.prepareStatement(str_repoint)) {
                                                        //ps_repoint.setInt(1, account_id_renew);    // account_id
                                                        ps_repoint.setInt(1, erp_point);    //account_point
                                                        ps_repoint.setString(2, DString + " " + timeString); //  created_at
                                                        ps_repoint.setString(3, DString + " " + timeString); //  modified_at
                                                        ps_repoint.setString(4, DString + " " + timeString); //  updated_at
                                                        ps_repoint.executeUpdate();                           // 執行 INSERT
                                                        ResultSet account_repoint_id = ps_repoint.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY KEY
                                                        account_repoint_id.next();
                                                        int account_repoint_lastid = account_repoint_id.getInt(1);   // 字串轉數字
                                                        System.out.println(rs.getRow() + " 新增 account_point_id:" + account_repoint_lastid);
                                                        //System.out.println(rs.getRow() + " 新增 account_point_id:" + a_point_id + " account_id:" + account_id + " 點數:" + account_point);
                                                        ////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                        /////////////  add to account_point_history
                                                        String str_repoint_his = "INSERT INTO account_point_history"
                                                                + "(action_type,use_type,account_point,account_point_id,account_point_left,account_point_use_comment,use_type_description,created_at,updated_at,modified_at) "
                                                                + "VALUES (1,0,?,?,?,?,?,?,?,?) ";
                                                        // 1,2,3,4,5,6,7,8
                                                        try (Connection con_repoint_his = DriverManager.getConnection(url2, user2, pass2);
                                                                PreparedStatement ps_repoint_his = con_repoint_his.prepareStatement(str_repoint_his)) {
                                                            ps_repoint_his.setInt(1, erp_point);             // account_point
                                                            ps_repoint_his.setInt(2, account_repoint_lastid);    // account_point_lastid
                                                            ps_repoint_his.setInt(3, account_point_left);        // account_point_left
                                                            ps_repoint_his.setString(4, "購買產品:" + TT_ERP_NAME);  // account_point_use_commment
                                                            ps_repoint_his.setString(5, "點數獲得");                 // use_type_description
                                                            ps_repoint_his.setString(6, DString + " " + timeString); // created_at
                                                            ps_repoint_his.setString(7, DString + " " + timeString); // updated_at
                                                            ps_repoint_his.setString(8, DString + " " + timeString); // modified_at
                                                            ps_repoint_his.executeUpdate();                          // 執行 INSERT
                                                            ResultSet account_repoint_his_id = ps_repoint_his.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY KEY
                                                            account_repoint_his_id.next();
                                                            int account_repoint_his_lastid = account_repoint_his_id.getInt(1);   // 字串轉數字
                                                            System.out.println(rs.getRow() + " 新增 account_point_history_id : " + account_repoint_his_lastid);
                                                            con_repoint_his.close();
                                                        } catch (SQLException ex) {
                                                            System.out.println("  !!! INSERT INTO account_point_history 錯誤 " + ex.getMessage() + str_repoint_his);
                                                        }
                                                        con_point.close();
                                                    } catch (SQLException ex) {
                                                        System.out.println("  !!! str_repoint INSERT INTO account_point錯誤 " + ex.getMessage() + str_repoint);
                                                    }
                                                }/////////////////////////////////////////////////////////////////////////////////////////////////////
                                            } catch (SQLException ex) {
                                                System.out.println("str_po SELECT account_point  錯誤 " + ex.getMessage());
                                            }
                                        } //End of Add_Point
                                    } //end of renew  ////////////////////////////////////////////////////////////////////////
                                    System.out.println("  ######## Auto Created ########");
                                } catch (SQLException ex) {
                                    System.out.println(rs.getRow() + " INSERT INTO manage_po 錯誤 " + ex.getMessage());
                                }
                            } else // Begin  erp_ai 無配套的資料進error
                            {
                                String MISC = rs.getString(12);  // CTC 上課會員 MISC-1200513 MISC-1200514 MISC-1200538 MISC-1200519
                                if (("MISC-1200513".equals(MISC)) || ("MISC-1200514".equals(MISC)) || ("MISC-1200538".equals(MISC)) || ("MISC-1200539".equals(MISC))) {
                                    String ctc_erp = rs.getString(9);
                                    if ("0".equals(rs.getString(9))) {
                                        ctc_erp = rs.getString(2);
                                    }
                                    int period = 12;  //period
                                    String mi000 = "", ci000 = "", ci001 = "", ci002 = "", ci003 = "", ci004 = "", ci005 = "", ci006 = "";

                                    Calendar ca0 = Calendar.getInstance();          //使用預設時區和語言環境獲得日曆
                                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");  //通過格式化輸出日期
                                    String ctc_enable_day = format.format(ca0.getTime()).substring(0, 10);//取得今日
                                    int cyear = Integer.parseInt(ctc_enable_day.substring(0, 4));   //今日年
                                    int cmonth = Integer.parseInt(ctc_enable_day.substring(5, 7));  //今日月
                                    int cdate = Integer.parseInt(ctc_enable_day.substring(8, 10));  //今日日
                                    ca0.set(cyear, cmonth - 1, cdate);
                                    ca0.add(Calendar.YEAR, 1);
                                    Date dd0 = ca0.getTime();
                                    String ctc_expired_day = format.format(dd0);
                                    //System.out.println(rs.getRow() + " 無配套資料CTC上課會員 ");
                                    //System.out.println(rs.getRow() + " enable: " + ctc_enable_day + " expired: " + ctc_expired_day);
                                    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                    String connectionCTSDB = "jdbc:sqlserver://10.20.0.32:1433;database=CTSDB;user=sa;password=as;trustServerCertificate=true;";
                                    try (Connection mscon1 = DriverManager.getConnection(connectionCTSDB); Statement stmt_mi000 = mscon1.createStatement();) {
                                        String SQL_A = "SELECT CONVERT(nvarchar(36), MI000) as MI000,MI001,MI002 FROM dbo.CTPMemberInfo WHERE MI002='" + ctc_erp + "' ";
                                        ResultSet rss1 = stmt_mi000.executeQuery(SQL_A);
                                        if (rss1.next()) {
                                            mi000 = rss1.getString(1);
                                            //System.out.println("  mi000:" + mi000);
                                            try (Connection mscon2 = DriverManager.getConnection(connectionCTSDB); Statement stmt_ci000 = mscon2.createStatement();) {
                                                String SQL_B = "SELECT convert(datetime,CI004,20),convert(datetime,CI005,20) FROM dbo.CTPContractInfo WHERE CI001='" + mi000 + "' ORDER BY CI005 DESC";
                                                ResultSet rss2 = stmt_ci000.executeQuery(SQL_B);
                                                if (rss2.next()) {
                                                    ci004 = rss2.getString(1).substring(0, 10);//ENABLE
                                                    ci005 = rss2.getString(2).substring(0, 10);//END
                                                    System.out.println("  CTC上課會員啟用日:" + ci004 + " 到期日:" + ci005);
                                                    int compareTo = ctc_enable_day.compareTo(ci005);//今日大於到期日  為正數 表示合約過期了
                                                    if (compareTo > 0) {  //今日大於到期日 正數 表示合約日到期   啟用日是今天

                                                        Calendar cal = Calendar.getInstance();
                                                        int syear = Integer.parseInt(ci005.substring(0, 4));   //續約客戶到期年
                                                        int smonth = Integer.parseInt(ci005.substring(5, 7));  //續約客戶到期月
                                                        int sdate = Integer.parseInt(ci005.substring(8, 10));  //續約客戶到期日

                                                        cal.set(syear, smonth - 1, sdate);
                                                        cal.add(Calendar.DATE, 1);               //CTC 續約客戶到期日+1天等於啟用日
                                                        Date dd1 = cal.getTime();
                                                        ctc_enable_day = format.format(dd1);
                                                        //System.out.println(rs.getRow() + " CTC續約啟用日:" + ctc_enable_day); //啟用日

                                                        cal.add(Calendar.YEAR, 1);               //CTC 續約客戶到期日+1年等於到期日
                                                        Date dd2 = cal.getTime();
                                                        ctc_expired_day = format.format(dd2);
                                                        //System.out.println(rs.getRow() + " CTC續約到期日:" + ctc_expired_day);   //到期日
                                                    }
                                                }
                                            }
                                        }
                                        mscon1.close();
                                    } catch (SQLException ex2) {
                                        System.out.println(ex2.getMessage());
                                    }
                                    System.out.println("  CTC上課續約啟用日:" + ctc_enable_day + " 到期日:" + ctc_expired_day);   //到期日
                                    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                String S1 = check_CTC_data("0", period, rs.getString(2), rs.getString(9), rs.getString(10), DString, ctc_enable_day, ctc_expired_day, "MT111");
//                               String S2 = manage_erp(rs.getRow(), rs.getString(1), ctc_enable_day, ctc_expired_day,"");
                                }
                                String url_phone_err = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
                                String user_phone_err = "intit20200303"; //for localhost;
                                String pass_phone_err = "jes#2354";

                                Class.forName("com.mysql.jdbc.Driver");
                                String phone_err = "INSERT INTO manage_po_error (dealer_id,po_number,customer_number,bri_name,key_name,account_phone,package_number,package_name,create_date) "
                                        + "values (1,?,?,?,?,?,?,?,?)";
                                try (Connection packageerr = DriverManager.getConnection(url_phone_err, user_phone_err, pass_phone_err);
                                        PreparedStatement pserr = packageerr.prepareStatement(phone_err)) {
                                    pserr.setString(1, rs.getString(1)); // 定單號碼
                                    pserr.setString(2, rs.getString(2)); // 客編
                                    pserr.setString(3, rs.getString(3)); // 簡稱
                                    pserr.setString(4, rs.getString(4)); // 聯絡人
                                    pserr.setString(5, cell_phone);      // 手機號
                                    pserr.setString(6, rs.getString(12));// 配套代號
                                    pserr.setString(7, rs.getString(13));// 配套中文
                                    pserr.setString(8, DString + " " + timeString);//  create_datetime
                                    pserr.executeUpdate();
                                    ResultSet manage_error_id = pserr.getGeneratedKeys();
                                    manage_error_id.next();
                                    int manage_error_id_lastid = manage_error_id.getInt(1);
                                    System.out.println(rs.getRow() + " 新增 無配套 manage_po_error_id : " + manage_error_id_lastid);
                                    packageerr.close();
                                } catch (SQLException ex) {
                                    System.out.println("   " + ex.getMessage() + phone_err);
                                }
                                manage_ai_report(rs.getRow(), "0", buy_mode, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(12), rs.getString(13), rs.getString(14), rs.getString(21), rs.getString(20), rs.getString(22), cell_phone);
                            }// End  of erp_ai 無配套的資料進error
                        } else // Begin erp_ai_phone 檢查電話
                        {
                            String url_phone_err = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
                            String user_phone_err = "intit20200303"; //for localhost;
                            String pass_phone_err = "jes#2354";

                            Class.forName("com.mysql.jdbc.Driver");
                            String phone_err = "INSERT INTO manage_po_error (dealer_id,po_number,customer_number,bri_name,key_name,account_phone,package_number,package_name,create_date) "
                                    + "values (1,?,?,?,?,?,?,?,?)";
                            try (Connection packageerr = DriverManager.getConnection(url_phone_err, user_phone_err, pass_phone_err);
                                    PreparedStatement pserr = packageerr.prepareStatement(phone_err)) {
                                pserr.setString(1, rs.getString(1)); // 定單號碼
                                pserr.setString(2, rs.getString(2)); // 客編
                                pserr.setString(3, rs.getString(3)); // 簡稱
                                pserr.setString(4, rs.getString(4)); // 聯絡人
                                pserr.setString(5, cell_phone);      // 手機號
                                pserr.setString(6, rs.getString(12));// 配套代號
                                pserr.setString(7, rs.getString(13));// 配套中文
                                pserr.setString(8, DString + " " + timeString);//  ceable_datetime
                                pserr.executeUpdate();
                                ResultSet manage_error_id = pserr.getGeneratedKeys();
                                manage_error_id.next();
                                int manage_error_id_lastid = manage_error_id.getInt(1);
                                System.out.println(rs.getRow() + " 新增無手機 manage_po_error_id : " + manage_error_id_lastid);
                                packageerr.close();
                            } catch (SQLException ex) {
                                System.out.println("strerr錯誤 " + ex.getMessage() + phone_err);
                            }
                            manage_ai_report(rs.getRow(), "0", buy_mode, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(12), rs.getString(13), rs.getString(14), rs.getString(21), rs.getString(20), rs.getString(22), cell_phone);
                        }// End of erp_ai_ai 檢查電話
//print only */
                        System.out.println("");
                    } //End of if (YearInt > 2)

                } //End of while()
            }            ///////////////////////////////////////////////////////////////////////////////////////
            //取得 Oracle DB 欄位數量 & 資料型態
            //int columns = rs.getMetaData().getColumnCount();for (int i=1;i<columns; i++){System.out.println(i+" "+rs.getMetaData().getColumnName(i)+":"+rs.getMetaData().getColumnTypeName(i)); }
            erpconnection.close();
        } catch (SQLException ex) {
            System.out.println("Oracle ERP 錯誤 " + ex.getMessage());
        }
    }  // End of main function()

//  Sub Function Below ////////////////////////
// Begin 檢查ERP手機號碼                       手機號碼 //1 PO          2客編        4客戶        6手機
    public static String check1_erp_phone(int row, String oea01, String ta_oea19, String name, String phone) throws SQLException, ClassNotFoundException {
        String pattern0 = "^09[0-9]{2}-[0-9]{3}-[0-9]{3}$"; //0900-000-000
        String pattern1 = "^09[0-9]{2}-[0-9]{6}$";          //0900-000000
        String pattern2 = "^09[0-9]{5}-[0-9]{3}$";          //0910000-000
        String pattern3 = "^09[0-9]{8}$";                   //0900000000
        String p1, p2, p3, ai_phone = null;
        int id = 0;
        if (phone.length() == 1 || phone.length() > 11) {   //ERP 訂單無手機號碼
            System.out.println(row + " Check1:" + ta_oea19 + " " + name + " ERP訂單無手機號碼!!! ");
            // check account_cell_phone with account_number from accounts
            String url_phone = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
            String user_phone = "intit-20200303";//"intit20200303"; //for localhost & 10.20.0.9
            String pass_phone = "jes#2354";
            String str_cell_phone = "SELECT account_id,account_cell_phone FROM accounts WHERE account_number='" + ta_oea19 + "' "; //客編查 account_id and 手機
            Class.forName("com.mysql.jdbc.Driver");
            try (Connection con_phone = DriverManager.getConnection(url_phone, user_phone, pass_phone)) {
                Statement stmt_phone = con_phone.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                ResultSet rs_phone = stmt_phone.executeQuery(str_cell_phone);
                rs_phone.next();
                id = rs_phone.getInt(1);
                ai_phone = rs_phone.getString(2); //AI accounts手機
                System.out.println("  客編查id:" + id + " accounts 手機號碼:" + ai_phone);
                con_phone.close();
            } catch (SQLException ex) {
                System.out.println(row + " !!!用客編查無accouts_id and account_cell_phone 錯誤 " + ex.getMessage());
                ai_phone = "0";
            }
            if ("0".equals(ai_phone)) {
                String str_device_phone = "SELECT account_id,account_device_phone FROM account_devices WHERE account_id='" + id + "' "; //account_id 查AI device_phone
                Class.forName("com.mysql.jdbc.Driver");
                try (Connection con_phone1 = DriverManager.getConnection(url_phone, user_phone, pass_phone)) {
                    Statement stmt_phone1 = con_phone1.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                    ResultSet rs_phone1 = stmt_phone1.executeQuery(str_device_phone);
                    rs_phone1.next();
                    ai_phone = rs_phone1.getString(2); //AI account_device_phone
                    System.out.println("  account_id查id:" + id + " account_device_phone:" + ai_phone);
                    con_phone1.close();
                } catch (SQLException ex) {
                    System.out.println(row + " !!!用客編查無accound_id and account_device_phone 錯誤 " + ex.getMessage());
                    ai_phone = "0";
                }
            }
            return ai_phone;
        }

        // 檢查手機號碼格式 並拿掉“-”
        if (phone.matches(pattern1)) {
            System.out.print(row + " Check_ERP_Phone:pattern1:" + phone);
            p1 = phone.substring(0, 4);
            p2 = phone.substring(5, 11);
            ai_phone = p1 + p2;
            System.out.println("->" + ai_phone);
        } else {
            if (phone.matches(pattern0)) {
                System.out.print(row + " Check_ERP_Phone:pattern0:" + phone);
                p1 = phone.substring(0, 4);
                p2 = phone.substring(5, 8);
                p3 = phone.substring(9, 12);
                ai_phone = p1 + p2 + p3;
                System.out.println("->" + ai_phone);
            } else if (phone.matches(pattern2)) {
                System.out.print(row + " Check_ERP_Phone:pattern2:" + phone);
                p1 = phone.substring(0, 7);
                p2 = phone.substring(8, 11);
                ai_phone = p1 + p2;
                System.out.println("->" + ai_phone);
            } else if (phone.matches(pattern3)) {
                ai_phone = phone;
                System.out.println(row + " Check_ERP_Phone:pattern3:" + phone);
            } else {
                System.out.println(row + " " + oea01 + " " + ta_oea19 + " " + name + " " + phone + " 手機格式不對");
                ai_phone = "0";
            }
        }
        return ai_phone;
    }

    //  End of 檢查ERP手機號碼 //
    //  Begin  檢查AI手機號碼  //
    public static String check2_ai_phone(int row, String oea01, String ta_oea19, String name, String erp_phone) throws SQLException, ClassNotFoundException {
        String ai_id = null;
        String ai_phone = null;
        String url2 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
        String user2 = "intit-20200303";//"intit20200303"; //for localhost & 10.20.0.9
        String pass2 = "jes#2354";
        String str_acc = "SELECT account_id FROM accounts WHERE account_number='" + ta_oea19 + "' "; //查客編有沒有account_id
        Class.forName("com.mysql.jdbc.Driver");
        try (Connection con_acc = DriverManager.getConnection(url2, user2, pass2);
                Statement st_acc = con_acc.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                ResultSet rs_acc = st_acc.executeQuery(str_acc)) {
            rs_acc.next();
            System.out.println(row + " 客戶編號:" + ta_oea19 + " " + name + " " + erp_phone + " account_id：" + rs_acc.getInt(1));
            ai_id = String.valueOf(rs_acc.getInt(1)); // 數字轉字串
            con_acc.close();
        } catch (SQLException ex) {
            System.out.println(row + " !!! 客戶編號:" + ta_oea19 + " 無 account_id 錯誤 " + ex.getMessage());
            ai_id = "0";
        }
        if ("0".equals(ai_id)) //無客戶編號ID 用ERP_phone 查手機
        {
            String str_acc1 = "SELECT account_id,account_device_phone,member_type,account_device_id_parent FROM account_devices WHERE account_device_phone='" + erp_phone + "' "; //查手機有沒有account_id
            try (Connection con_acc1 = DriverManager.getConnection(url2, user2, pass2);
                    Statement st_acc1 = con_acc1.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                    ResultSet rs_acc1 = st_acc1.executeQuery(str_acc1)) {
                boolean A = true;
                rs_acc1.next();

                if (rs_acc1.getInt(3) == 2) {
                    A = false;
                    System.out.println(row + " 無客編 查客戶_Type:" + rs_acc1.getString(2) + " account_id:" + rs_acc1.getInt(1) + " member_type:" + rs_acc1.getInt(3) + " device_id_parent:" + rs_acc1.getInt(4) + " 體驗轉主機");
                }
                if (rs_acc1.getInt(4) != 0) {
                    A = false;
                    System.out.println(row + " 無客編 查客戶_Type:" + rs_acc1.getString(2) + " account_id:" + rs_acc1.getInt(1) + " member_type:" + rs_acc1.getInt(3) + " device_id_parent:" + rs_acc1.getInt(4) + " 副機轉主機");
                }
                if (A) {
                    System.out.println(row + " 無客編 查客戶_Type:" + rs_acc1.getString(2) + " account_id:" + rs_acc1.getInt(1) + " member_type:" + rs_acc1.getInt(3) + " device_id_parent:" + rs_acc1.getInt(4));
                }
                ai_phone = rs_acc1.getString(2); // 數字轉字串
                con_acc1.close();
            } catch (SQLException ex) {
                System.out.println(row + " !!! ERP 手機:" + erp_phone + " 查 account_id 錯誤 " + ex.getMessage());
                ai_phone = erp_phone;
            }
        } else //有客戶編號     用 id 查 account_device_phone 手機
        {
            String str_acc1 = "SELECT account_id,account_device_phone,member_type FROM account_devices WHERE account_id='" + ai_id + "' "; //查手機有沒有account_id
            try (Connection con_acc1 = DriverManager.getConnection(url2, user2, pass2);
                    Statement st_acc1 = con_acc1.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                    ResultSet rs_acc1 = st_acc1.executeQuery(str_acc1)) {
                rs_acc1.next();
                System.out.print(row + " ERP 手機:" + erp_phone + " AI 手機:" + rs_acc1.getString(2) + " account_id:" + rs_acc1.getInt(1) + " member_type:" + rs_acc1.getInt(3));
                if (rs_acc1.getInt(3) == 1) {
                    System.out.println(" 續約客戶");
                }
                ai_phone = rs_acc1.getString(2); // 數字轉字串
                con_acc1.close();
            } catch (SQLException ex) {
                System.out.println(row + " !!! ERP 手機:" + erp_phone + " 查 account_id 錯誤 " + ex.getMessage());
            }
        }
        return ai_phone;
    }

    //  End of 檢查AI手機號碼 //
    //  Begin  檢查配套       //
    public static String check3_erp_ai(int row, String po, String ta_oea19, String name, String erp_ai, String erp_ai_ch) throws SQLException, ClassNotFoundException {
        String url_erp_ai = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
        String user_erp_ai = "intit20200303"; //for localhost;
        String pass_erp_ai = "jes#2354";
        String str_erp_ai = "SELECT product_id,product_main_id,period,point,ctc_code FROM erp_ai WHERE TT_ERP_ID='" + erp_ai + "' ";
        Class.forName("com.mysql.jdbc.Driver");
        try (Connection con_erp_ai = DriverManager.getConnection(url_erp_ai, user_erp_ai, pass_erp_ai);
                Statement st = con_erp_ai.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                ResultSet rs_erp_ai = st.executeQuery(str_erp_ai)) {
            if (rs_erp_ai.next()) {
                System.out.println(row + " Check_ERP_配套:" + ta_oea19 + " " + name + " " + rs_erp_ai.getInt(1) + " " + rs_erp_ai.getInt(2) + " " + rs_erp_ai.getInt(3) + " " + rs_erp_ai.getInt(4) + " " + rs_erp_ai.getString(5) + " " + erp_ai + " " + erp_ai_ch); //要去謮一下product_main_id 無資料才會引發SQLException
                erp_ai = String.valueOf(rs_erp_ai.getInt(3));
            } else {
                System.out.println(row + " Check_ERP_AI 無配套資料:" + po + " " + ta_oea19 + " " + name + " " + erp_ai + " " + erp_ai_ch);
                erp_ai = "0";
            }
            con_erp_ai.close();
        } catch (SQLException ex) {
            System.out.println(row + " Check_ERP_AI 無配套資料 錯誤 " + ex.getMessage());
            erp_ai = "0";
        }
        return erp_ai;
    }

    // End of   檢查配套            //
    // Begin ****  檢查  accounts ************//////////////      2客編          3簡稱             4客戶           6手機
    public static String check5_account_id(int row, String account_number, String account_name, String person, String ai_phone) throws SQLException, ClassNotFoundException {
        //String url_acc1 = "jdbc:mysql://10.20.0.10:3306/car?characterEncoding=UTF-8";
        //String user_acc1 = "intit2020303";
        String url2 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
        String user2 = "intit-20200303";//"intit20200303"; //for localhost & 10.20.0.9
        String pass2 = "jes#2354";
        String str_acc = "SELECT account_id FROM accounts WHERE account_number='" + account_number + "' ";    //查客戶編號有沒有 account_id
        //String str_acc = "SELECT account_id FROM accounts WHERE account_cell_phone='"+ai_phone+"' ";        //查客戶手機有沒有 account_id
        Class.forName("com.mysql.jdbc.Driver");
        try (Connection con_acc = DriverManager.getConnection(url2, user2, pass2);
                Statement st_acc = con_acc.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                ResultSet rs_acc = st_acc.executeQuery(str_acc)) {
            if (rs_acc.next()) {
                //System.out.println(row + " " + account_name + " " + person + " " + ai_phone + " 有 account_id：" + rs_acc.getInt(1));
                ai_phone = String.valueOf(rs_acc.getInt(1)); // account_id 數字轉字串
            } else {
                //這行不能使用因為查不到資料己經有  SQLException 不會印出了  System.out.println(row+" 客戶編號："+account_number+" "+person+" "+ai_phone+" account_id："+rs_acc.getInt(1)+"新合約");
                ai_phone = "0";  // 沒有 account_id  指定 "0'"
            }
            con_acc.close();
        } catch (SQLException ex) {
            System.out.println(row + " check_accounts_id 錯誤 " + ex.getMessage());
        }
        return ai_phone;
    }

    //  End of ****  檢查  accounts  *************//////////////
    // Begin ****  檢查  member_type************//////////////      2客編          3簡稱             4客戶           6手機
    public static String check6_member_type(int row, String account_number, String account_name, String person, String ai_phone) throws SQLException, ClassNotFoundException {
        //String url_acc1 = "jdbc:mysql://10.20.0.10:3306/car?characterEncoding=UTF-8";
        //String user_acc1 = "intit2020303";
        String url_acc1 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
        String user_acc1 = "intit-20200303";//"intit20200303"; //for localhost & 10.20.0.9
        String pass_acc1 = "jes#2354";
        String str_acc1 = "SELECT account_id,account_device_phone,member_type,account_device_id_parent FROM account_devices WHERE account_device_phone='" + ai_phone + "' "; //查手機有沒有account_id
        Class.forName("com.mysql.jdbc.Driver");
        try (Connection con_acc = DriverManager.getConnection(url_acc1, user_acc1, pass_acc1);
                Statement st_acc = con_acc.createStatement(); //獲取用於向資料庫發送 SQL 語法的 Statement
                ResultSet rs_acc = st_acc.executeQuery(str_acc1)) {
            if (rs_acc.next()) {
                if (rs_acc.getInt(3) == 2) {
                    System.out.println(row + " AI device 手機:" + rs_acc.getString(2) + " member_type:" + rs_acc.getInt(3) + " device_id_parent:" + rs_acc.getInt(4) + " 體驗轉主機");
                    ai_phone = "2";
                }
                if (rs_acc.getInt(4) != 0) {
                    System.out.println(row + " AI device 手機:" + rs_acc.getString(2) + " member_type:" + rs_acc.getInt(3) + " device_id_parent:" + rs_acc.getInt(4) + " 副機轉主機");
                    ai_phone = "3";
                }
            }
            con_acc.close();
        } catch (SQLException ex) {
            System.out.println(row + " check_member_type 錯誤 " + ex.getMessage());
        }
        return ai_phone;
    }

    //End of **** 檢查 accounts    /////////////
    //Begin  **** 檢查 check product_enable date
    //   檢查開通日                                                          2客編           3簡稱           4客戶   6手機              14日期OEA02
    //product_enable_date=check_product_enable_date(rs.getRow(),rs.getString(2),rs.getString(3),rs.getString(4),erp_phone,rs.getString(14).substring(0,10));
    public static String check4_product_enable_date(int row, String ta_oea19, String account_name, String person, String ai_phone, String product_enable_date) throws SQLException, ClassNotFoundException {
        //System.out.println(row+" "+ta_oea19+" "+account_name+" "+person+" "+ai_phone+" "+product_enable_date);
        //String url_acc1 = "jdbc:mysql://10.20.0.10:3306/car?characterEncoding=UTF-8";
        //String user_acc1 = "intit2020303";
        String url_acc1 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
        String user_acc1 = "intit-20200303";//String user_acc1 = "intit20200303";
        String pass_acc1 = "jes#2354";
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String str_acc2 = "SELECT account_id FROM accounts WHERE account_number='" + ta_oea19 + "' Order by account_id DESC"; //客編查 id
        Class.forName("com.mysql.jdbc.Driver");
        try (Connection con_acc2 = DriverManager.getConnection(url_acc1, user_acc1, pass_acc1);
                Statement st_acc2 = con_acc2.createStatement();
                ResultSet rs_acc2 = st_acc2.executeQuery(str_acc2)) {
            rs_acc2.next();
            int ai_id = rs_acc2.getInt(1);
            String DateString;
            String str_acc22 = "SELECT account_id,account_product_expire_datetime "
                    + "FROM account_products WHERE account_id='" + ai_id + "' "
                    + "Order BY account_product_expire_datetime DESC"; //查客戶編號ID
            try (Connection con_acc22 = DriverManager.getConnection(url_acc1, user_acc1, pass_acc1)) {
                Statement st_acc22 = con_acc22.createStatement();
                ResultSet rs_acc22 = st_acc22.executeQuery(str_acc22);
                if (rs_acc22.next()) {
                    DateString = (rs_acc22.getString(2).substring(0, 19));
                    System.out.println(row + " Check AI 客編 :" + ta_oea19 + " " + person + " account_id " + ai_id + " 到期日" + DateString);
                } else {
                    DateString = "0000:00:00 00:00:00";
                }
//////
                Calendar c = Calendar.getInstance();
                int year = Integer.parseInt(DateString.substring(0, 4)); //續約客戶到期年
                int month = Integer.parseInt(DateString.substring(5, 7)); //續約客戶到期月
                int date = Integer.parseInt(DateString.substring(8, 10));//續約客戶到期日
                int hour = Integer.parseInt(DateString.substring(11, 13));//續約客戶到期
                int minute = Integer.parseInt(DateString.substring(14, 16));//續約客戶到期
                int second = Integer.parseInt(DateString.substring(17, 19));//續約客戶到期

                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  //通過格式化輸出日期
                c.set(year, month - 1, date, hour, minute, second); //取回起用日  //2020-09-15
                c.add(Calendar.SECOND, 1);
                Date d = c.getTime();
                product_enable_date = format.format(d);
//////
                con_acc22.close();
            } catch (SQLException ex) {
                System.out.println(row + " check_product_enable_date acc22 錯誤 " + ex.getMessage());
                System.out.println("!");
            }
            con_acc2.close();
        } catch (SQLException ex) {
            System.out.println(row + " check_product_22 " + ex.getMessage());
            System.out.println("  !!!無 AI客編");
        }
        /*  Don't check PHONE date for duplicate devices phone
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String str_acc1 = "SELECT account_id FROM account_devices WHERE account_device_phone='" + ai_phone + "' Order by account_id DESC"; //手機查 id
        try (Connection con_acc1 = DriverManager.getConnection(url_acc1, user_acc1, pass_acc1);
                Statement st_acc1 = con_acc1.createStatement();
                ResultSet rs_acc1 = st_acc1.executeQuery(str_acc1)) {
            rs_acc1.next();
            int ai_id = rs_acc1.getInt(1);
            String DateString;
            String str_acc11 = "SELECT account_id,account_product_expire_datetime,modified_at "
                    + "FROM account_products WHERE account_id='" + ai_id + "' "
                    + "Order BY account_product_expire_datetime DESC"; //查手機ID
            try (Connection con_acc11 = DriverManager.getConnection(url_acc1, user_acc1, pass_acc1)) {
                Statement st_acc11 = con_acc11.createStatement();
                ResultSet rs_acc11 = st_acc11.executeQuery(str_acc11);
                if (rs_acc11.next()) {
                    DateString = rs_acc11.getString(2).substring(0, 19);
                    System.out.println(row + " Check AI 手機 :" + ta_oea19 + " " + person + " account_id " + ai_id + " 到期日" + DateString + " modified_at:" + rs_acc11.getString(3).substring(0, 19));
                } else {
                    DateString = "0000:00:00 00:00:00";
                }

                Calendar c = Calendar.getInstance();
                int year = Integer.parseInt(DateString.substring(0, 4)); //續約客戶到期年
                int month = Integer.parseInt(DateString.substring(5, 7)); //續約客戶到期月
                int date = Integer.parseInt(DateString.substring(8, 10));//續約客戶到期日
                int hour = Integer.parseInt(DateString.substring(11, 13));//續約客戶到期
                int minute = Integer.parseInt(DateString.substring(14, 16));//續約客戶到期
                int second = Integer.parseInt(DateString.substring(17, 19));//續約客戶到期

                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  //通過格式化輸出日期
                c.set(year, month - 1, date, hour, minute, second); //取回起用日  //2020-09-15
                c.add(Calendar.SECOND, 1);
                Date d = c.getTime();
                product_enable_date = format.format(d);

                con_acc11.close();
            } catch (SQLException ex) {
                System.out.println(row + " check_product_enable_date acc11 錯誤 " + ex.getMessage());
                System.out.println("!");
            }
            con_acc1.close();

        } catch (SQLException ex) {
            System.out.println(row + " check_product_11 " + ex.getMessage());
            System.out.println("  !!!無 AI手機");
        }
         */
        //System.out.println(row+" "+account_name+" "+person+" 到期日"+product_enable_date);
        return product_enable_date;
    }
    // End of check product_enable date *******************

    public static String check_CTC_data(String contract, int period, String rs2, String rs9, String rs10, String buy_day, String enable_day, String expired_day, String ctc_code) throws SQLException, ClassNotFoundException {
        System.out.println("  --------------- Start CTC Process-----------------");
        String ctc_erp = rs9;
        if ("0".equals(rs9)) {
            ctc_erp = rs2;
        }
        String member = null;
        String mi000 = null;
        String mi001 = null;
        int year = period / 12;
        String m1 = null, m2 = null, m3 = null;
        String formatStr = "%02d";
        String f_getrow = "";
        //System.out.println("  but_day:" + buy_day+"  enable_day:" + enable_day+"  expired_day:" + expired_day);
        String yyyy = buy_day.substring(0, 4);
        String mm = buy_day.substring(5, 7);
        String dd = buy_day.substring(8, 10);
        buy_day = yyyy + mm + dd;
        yyyy = enable_day.substring(0, 4);
        mm = enable_day.substring(5, 7);
        dd = enable_day.substring(8, 10);
        enable_day = yyyy + mm + dd;
        yyyy = expired_day.substring(0, 4);
        mm = expired_day.substring(5, 7);
        dd = expired_day.substring(8, 10);
        expired_day = yyyy + mm + dd;
        System.out.println("  buy_day:" + buy_day + "  enable_day:" + enable_day + "  expired_day:" + expired_day);
        Calendar ctc_ca = Calendar.getInstance();  //使用預設時區和語言環境獲得日曆
        SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");   //通過格式化輸出日期
        //String ctc_today = DF.format(ctc_ca.getTime());                 //取得今日
        //System.out.println("  CTC今天日期 : " + ctc_today);

        String connectionCTSDB = "jdbc:sqlserver://10.20.0.32:1433;database=CTSDB;user=sa;password=as;trustServerCertificate=true;";

        //       try (Connection msconn = DriverManager.getConnection(connectionCTSDB);
        //               Statement stmt = msconn.createStatement();) {
        //           String SQL = "SELECT 類別,代碼,管制條件代碼,名稱,管制條件名稱,條件值,會員種類代碼 FROM dbo.VCTPMTypeLimit WHERE 會員種類代碼='" + ctc_code + "' ";
        //           rss = stmt.executeQuery(SQL);
        //           while (rss.next()) {
        //               System.out.println(" 管制條件代碼:" +rss.getString(3)+" "+rss.getString(5)+"            類別:" +rss.getString(1)+" 類別名稱:"+rss.getString(4)+ " 代碼:" +rss.getString(2));
        //           }
        //           msconn.close();
        //       } catch (SQLException ex2) {
        //           System.out.println(ex2.getMessage());
        //       }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        try (Connection mscon1 = DriverManager.getConnection(connectionCTSDB); Statement stmt_mi001 = mscon1.createStatement();) {
            String SQL_1 = "SELECT TOP 1 MI001 FROM dbo.CTPMemberInfo WHERE MI001 like 'A%' order by MI001 DESC";
            ResultSet rss1 = stmt_mi001.executeQuery(SQL_1);
            rss1.next();
            member = rss1.getString(1);  //取出最後的帳號編號
            mscon1.close();
        } catch (SQLException ex2) {
            System.out.println(ex2.getMessage());
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String S1 = member.substring(0, 2);
        String S2 = member.substring(2, 6);
        int zz = Integer.parseInt(S2) + 1;// 最後的帳號編號 + 1
        S1 = S1 + zz;

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        try (Connection mscon2 = DriverManager.getConnection(connectionCTSDB); Statement stmt_mi = mscon2.createStatement();) {
            String SQL = "SELECT CONVERT(nvarchar(36), MI000) as MI000,MI001,MI002,MI003,MI006 FROM dbo.CTPMemberInfo WHERE MI002='" + ctc_erp + "' ";
            ResultSet rss2 = stmt_mi.executeQuery(SQL);
            if (rss2.next()) {
                mi000 = rss2.getString(1);
                mi001 = rss2.getString(2);
                System.out.println("  CTP_會員編號:" + mi001 + "  CTC_ERP_客戶編號:" + ctc_erp + " CTC_CODE:" + ctc_code + " 合約年限:" + year);
            } else {
                mi001 = "0";
            }
            mscon2.close();
        } catch (SQLException ex2) {
            System.out.println(ex2.getMessage());
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        if ("0".equals(mi001)) //ERP 帳號不存在CTC 新增
        {
            try (Connection msc2 = DriverManager.getConnection(connectionCTSDB); Statement st = msc2.createStatement();) {
                UUID uid1 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00a");
                uid1 = uid1.randomUUID();//CTPMemberInfo(會員主檔)
                st.executeUpdate("INSERT INTO dbo.CTPMemberinfo(MI000,MI001,MI002,MI003,MI004,MI005,MI006,MI007,MI008) VALUES ('" + uid1 + "','" + S1 + "','" + ctc_erp + "','A'  ,'1'    ,'N'  ,'" + rs10 + "',''   ,'N')");
                System.out.println("  新增CTC客戶編號:" + S1 + " CTPMemberInfo_MI000:" + uid1 + " 備註:" + rs10);

                UUID uid2 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00b");
                uid2 = uid2.randomUUID();//CTPContractInfo(合約主檔) ;
                st.executeUpdate("INSERT INTO dbo.CTPContractInfo(CI000,CI001,CI002,CI003,CI004,CI005,CI006,CI007,CI008,CI009,CI010,CI011,CI012,CI013,CI014,CI015,CI016,CI017,CI018,CI019,CI020,CI021) "
                        + "VALUES ('" + uid2 + "','" + uid1 + "',  'M','" + ctc_code + "','" + enable_day + "','" + expired_day + "','" + buy_day + "','" + year + "',0,'Y','Y','0','9','N','管理平台','管理平台','管理平台','','','',0,0  )");
                //System.out.println("  CTC新增   CTPContractInfo    CI000:" + uid2 + "  CI001:" + uid1 + "  CTC_CODE:" + ctc_code + " 購買日:" + buy_day + " 啟用日:" + enable_day + " 到期日:" + expired_day);
                System.out.println("  CTC新增    CTPContractInfo    CI001:" + uid1 + "  CTC_CODE:" + ctc_code + " 購買日:" + buy_day + " 啟用日:" + enable_day + " 到期日:" + expired_day);

                int ctc_year = Integer.parseInt(enable_day.substring(0, 4));      //續約客戶到期年
                int ctc_month = Integer.parseInt(enable_day.substring(4, 6));     //續約客戶到期月
                int ctc_date = Integer.parseInt(enable_day.substring(6, 8));      //續約客戶到期日
                ctc_ca.set(ctc_year, ctc_month - 1, ctc_date);                    //設定日為到期日

                UUID uid3 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00c");
                if (period == 36 || period == 48) // 三年合約
                {
                    int yy = 1;
                    ctc_ca.add(Calendar.YEAR, +1);
                    expired_day = DF.format(ctc_ca.getTime());
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + yy + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    //System.out.println("  CTC新增:1  CTPMemberYear      MY000:" + uid3 + "  MY001:" + uid2 + "  MY002年限:" + yy + "  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    System.out.println("  CTC新增:1  CTPMemberYear      MY001:" + uid2 + "  MY002年限:" + yy + "  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    yy = yy + 1;
                    ctc_ca.add(Calendar.DATE, +1);
                    enable_day = DF.format(ctc_ca.getTime());
                    ctc_ca.add(Calendar.YEAR, +1);
                    ctc_ca.add(Calendar.DATE, -1);
                    expired_day = DF.format(ctc_ca.getTime());
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + yy + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    System.out.println("  CTC新增:2  CTPMemberYear      MY001:" + uid2 + "  MY002年限:" + yy + "  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    yy = yy + 1;
                    ctc_ca.add(Calendar.DATE, +1);
                    enable_day = DF.format(ctc_ca.getTime());
                    ctc_ca.add(Calendar.YEAR, +1);
                    ctc_ca.add(Calendar.DATE, -1);
                    expired_day = DF.format(ctc_ca.getTime());
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + yy + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    System.out.println("  CTC新增:3  CTPMemberYear      MY001:" + uid2 + "  MY002年限:" + yy + "  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    if (period == 48) {
                        yy = yy + 1;
                        ctc_ca.add(Calendar.DATE, +1);
                        enable_day = DF.format(ctc_ca.getTime());
                        ctc_ca.add(Calendar.YEAR, +1);
                        ctc_ca.add(Calendar.DATE, -1);
                        expired_day = DF.format(ctc_ca.getTime());
                        uid3 = uid3.randomUUID();
                        st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + yy + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                        System.out.println("  CTC新增    CTPMemberYear      MY001:" + uid2 + "  MY002年限:" + yy + "  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    }
                } else {
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + 1 + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    //System.out.println("  CTC新增   CTPMemberYear      MY000:" + uid3 + "  MY001:" + uid2 + "  MY002年限:" + 1 + "  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    System.out.println("  CTC新增    CTPMemberYear      MY001:" + uid2 + "  MY002年限:" + 1 + "  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                }

                UUID uid4 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d");
                int yy = 1;
                if (period == 36) // 3年合約
                {
                    while (yy < 4) {
                        try (Connection msconn_renew36 = DriverManager.getConnection(connectionCTSDB); Statement stmt = msconn_renew36.createStatement();) {
                            String SQL = "SELECT 類別,代碼,管制條件代碼,名稱,管制條件名稱,條件值,會員種類代碼 FROM dbo.VCTPMTypeLimit WHERE 會員種類代碼='" + ctc_code + "' ";
                            ResultSet rss = stmt.executeQuery(SQL);
                            while (rss.next()) {
                                f_getrow = String.format(formatStr, rss.getRow());
                                m1 = rss.getString(1);
                                m2 = rss.getString(2);
                                m3 = rss.getString(3);
                                try (Connection msc33 = DriverManager.getConnection(connectionCTSDB); Statement stt = msc33.createStatement();) {
                                    uid4 = uid4.randomUUID();
                                    stt.executeUpdate("INSERT INTO dbo.CTPMemberYearLimit(ML000,ML001,ML002,ML003,ML004,ML005,ML006,ML007,ML008) VALUES ('" + uid4 + "','" + uid2 + "','" + yy + "','" + m3 + "','0','0','" + m1 + "','" + m2 + "','N' )");
                                    //System.out.println("  CTC新增:" + rss.getRow() + " CTPMemberYearLimit ML000:" + uid4 + "  ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " ML006類別:" + m1 + " ML007代碼:" + m2);
                                    System.out.println("  CTC新增:" + f_getrow + " CTPMemberYearLimit ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " ML006類別:" + m1 + " ML007代碼:" + m2);
                                }
                            }
                        } catch (SQLException ex2) {
                            System.out.println("  msconn_renew36 error");
                            System.out.println(ex2.getMessage());
                        }
                        yy++;
                    }
                }
                if (period == 48) // 4年合約
                {
                    while (yy < 5) {
                        try (Connection msconn_renew48 = DriverManager.getConnection(connectionCTSDB); Statement stmt = msconn_renew48.createStatement();) {
                            String SQL = "SELECT 類別,代碼,管制條件代碼,名稱,管制條件名稱,條件值,會員種類代碼 FROM dbo.VCTPMTypeLimit WHERE 會員種類代碼='" + ctc_code + "' ";
                            ResultSet rss = stmt.executeQuery(SQL);
                            while (rss.next()) {
                                f_getrow = String.format(formatStr, rss.getRow());
                                m1 = rss.getString(1);
                                m2 = rss.getString(2);
                                m3 = rss.getString(3);
                                try (Connection msc33 = DriverManager.getConnection(connectionCTSDB); Statement stt = msc33.createStatement();) {
                                    uid4 = uid4.randomUUID();
                                    stt.executeUpdate("INSERT INTO dbo.CTPMemberYearLimit(ML000,ML001,ML002,ML003,ML004,ML005,ML006,ML007,ML008) VALUES ('" + uid4 + "','" + uid2 + "','" + yy + "','" + m3 + "','0','0','" + m1 + "','" + m2 + "','N' )");
                                    System.out.println("  CTC新增:" + f_getrow + " CTPMemberYearLimit ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " ML006類別:" + m1 + " ML007代碼:" + m2);
                                }
                            }
                        } catch (SQLException ex2) {
                            System.out.println("  msconn_renew48 error");
                            System.out.println(ex2.getMessage());
                        }
                        yy++;
                    }
                }
                if (period == 12) {
                    try (Connection msconn_renew12 = DriverManager.getConnection(connectionCTSDB); Statement stmt = msconn_renew12.createStatement();) {
                        String SQL = "SELECT 類別,代碼,管制條件代碼,名稱,管制條件名稱,條件值,會員種類代碼 FROM dbo.VCTPMTypeLimit WHERE 會員種類代碼='" + ctc_code + "' ";
                        ResultSet rss = stmt.executeQuery(SQL);
                        while (rss.next()) {
                            f_getrow = String.format(formatStr, rss.getRow());
                            m1 = rss.getString(1);
                            m2 = rss.getString(2);
                            m3 = rss.getString(3);
                            try (Connection msc33 = DriverManager.getConnection(connectionCTSDB); Statement stt = msc33.createStatement();) {
                                uid4 = uid4.randomUUID();
                                stt.executeUpdate("INSERT INTO dbo.CTPMemberYearLimit(ML000,ML001,ML002,ML003,ML004,ML005,ML006,ML007,ML008) VALUES ('" + uid4 + "','" + uid2 + "','" + 1 + "','" + m3 + "','0','0','" + m1 + "','" + m2 + "','N' )");
                                //System.out.println("  CTC新增:" + rss.getRow() + " CTPMemberYearLimit ML000:" + uid4 + "  ML001:" + uid2 + "  ML002年限:1  ML003管制條件代碼:" + m3 + " ML006類別:" + m1 + " ML007代碼:" + m2);
                                System.out.println("  CTC新增:" + f_getrow + " CTPMemberYearLimit ML001:" + uid2 + "  ML002年限:1  ML003管制條件代碼:" + m3 + " ML006類別:" + m1 + " ML007代碼:" + m2);
                            }
                        }
                    } catch (SQLException ex2) {
                        System.out.println("  msconn_renew12 error");
                        System.out.println(ex2.getMessage());
                    }
                }
                msc2.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println("  --------------- End of CTC Process-----------------");
            return S1;
        } else {
            //ERP 帳號存在CTC   續約
            UUID uid2 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00e");
            try (Connection msc3 = DriverManager.getConnection(connectionCTSDB); Statement st = msc3.createStatement();) {
                uid2 = uid2.randomUUID();
                st.executeUpdate("INSERT INTO dbo.CTPContractInfo(CI000,CI001,CI002,CI003,CI004,CI005,CI006,CI007,CI008,CI009,CI010,CI011,CI012,CI013,CI014,CI015,CI016,CI017,CI018,CI019,CI020,CI021) "
                        + "VALUES ('" + uid2 + "','" + mi000 + "',  'M','" + ctc_code + "','" + enable_day + "','" + expired_day + "','" + buy_day + "','" + year + "',0,'Y','Y','0','9','N','管理平台','管理平台','管理平台','','','',0,0  )");
                //System.out.println("  CTC續約   CTPContractInfo    CI000:" + uid2 + "  CI001:" + mi000 + "  CTC_CODE:" + ctc_code + " 購買日:" + buy_day + " 啟用日:" + enable_day + " 到期日:" + expired_day);
                System.out.println("  CTC續約    CTPContractInfo    CI001:" + mi000 + "  CTC_CODE:" + ctc_code + " 購買日:" + buy_day + " 啟用日:" + enable_day + " 到期日:" + expired_day);
                int ctc_year = Integer.parseInt(enable_day.substring(0, 4));      //續約客戶到期年20201228
                int ctc_month = Integer.parseInt(enable_day.substring(4, 6));     //續約客戶到期月
                int ctc_date = Integer.parseInt(enable_day.substring(6, 8));      //續約客戶到期日
                ctc_ca.set(ctc_year, ctc_month - 1, ctc_date);                    //設定日為到期日

                UUID uid3 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00f");
                if (period == 36 || period == 48) // 3  or 4年合約
                {
                    ctc_ca.add(Calendar.YEAR, +1);
                    expired_day = DF.format(ctc_ca.getTime());
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + 1 + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    System.out.println("  CTC續約    CTPMemberYear      MY001:" + uid2 + "  MY002年限:1  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    ctc_ca.add(Calendar.DATE, +1);
                    enable_day = DF.format(ctc_ca.getTime());
                    ctc_ca.add(Calendar.YEAR, +1);
                    ctc_ca.add(Calendar.DATE, -1);
                    expired_day = DF.format(ctc_ca.getTime());
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + 2 + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    System.out.println("  CTC續約    CTPMemberYear      MY001:" + uid2 + "  MY002年限:2  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    ctc_ca.add(Calendar.DATE, +1);
                    enable_day = DF.format(ctc_ca.getTime());
                    ctc_ca.add(Calendar.YEAR, +1);
                    ctc_ca.add(Calendar.DATE, -1);
                    expired_day = DF.format(ctc_ca.getTime());
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + 3 + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    System.out.println("  CTC續約    CTPMemberYear      MY001:" + uid2 + "  MY002年限:3  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    if (period == 48) {                 // 4年合約
                        ctc_ca.add(Calendar.DATE, +1);
                        enable_day = DF.format(ctc_ca.getTime());
                        ctc_ca.add(Calendar.YEAR, +1);
                        ctc_ca.add(Calendar.DATE, -1);
                        expired_day = DF.format(ctc_ca.getTime());
                        uid3 = uid3.randomUUID();
                        st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + 4 + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                        System.out.println("  CTC續約    CTPMemberYear      MY001:" + uid2 + "  MY002年限:4  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                    }
                } else {
                    uid3 = uid3.randomUUID();
                    st.executeUpdate("INSERT INTO dbo.CTPMemberYear(MY000,MY001,MY002,MY003,MY004,Creator) VALUES ('" + uid3 + "','" + uid2 + "','" + 1 + "','" + enable_day + "','" + expired_day + "','管理平台' )");
                    System.out.println("  CTC續約    CTPMemberYear      MY001:" + uid2 + "  MY002年限:1  MY003啟用日:" + enable_day + "  MY004到期日:" + expired_day);
                }
                msc3.close();
            } catch (SQLException ex2) {
                System.out.println("  msc3 error");
                System.out.println(ex2.getMessage());
            }

            UUID uid4 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef000");
            int yy = 1;
            if (period == 36) // 3年合約
            {
                while (yy < 4) {
                    try (Connection msconn_renew36 = DriverManager.getConnection(connectionCTSDB); Statement stmt = msconn_renew36.createStatement();) {
                        String SQL = "SELECT 類別,代碼,管制條件代碼,名稱,管制條件名稱,條件值,會員種類代碼 FROM dbo.VCTPMTypeLimit WHERE 會員種類代碼='" + ctc_code + "' ";
                        ResultSet rss = stmt.executeQuery(SQL);
                        while (rss.next()) {
                            f_getrow = String.format(formatStr, rss.getRow());
                            m1 = rss.getString(1);
                            m2 = rss.getString(2);
                            m3 = rss.getString(3);
                            try (Connection msc33 = DriverManager.getConnection(connectionCTSDB); Statement stt = msc33.createStatement();) {
                                uid4 = uid4.randomUUID();
                                stt.executeUpdate("INSERT INTO dbo.CTPMemberYearLimit(ML000,ML001,ML002,ML003,ML004,ML005,ML006,ML007,ML008) VALUES ('" + uid4 + "','" + uid2 + "','" + yy + "','" + m3 + "','0','0','" + m1 + "','" + m2 + "','N' )");
                                //System.out.println("  CTC續約:" + rss.getRow() + " CTPMemberYearLimit ML000:" + uid4 + "  ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " " + rss.getString(5) + " ML006類別:" + m1 + " ML007代碼:" + m2);
                                System.out.println("  CTC續約:" + f_getrow + " CTPMemberYearLimit ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " " + rss.getString(5) + " ML006類別:" + m1 + " ML007代碼:" + m2);
                            }
                        }
                    } catch (SQLException ex2) {
                        System.out.println("  msconn_renew36 error");
                        System.out.println(ex2.getMessage());
                    }
                    yy++;
                }
            }
            if (period == 48) // 4年合約
            {
                while (yy < 5) {
                    try (Connection msconn_renew48 = DriverManager.getConnection(connectionCTSDB); Statement stmt = msconn_renew48.createStatement();) {
                        String SQL = "SELECT 類別,代碼,管制條件代碼,名稱,管制條件名稱,條件值,會員種類代碼 FROM dbo.VCTPMTypeLimit WHERE 會員種類代碼='" + ctc_code + "' ";
                        ResultSet rss = stmt.executeQuery(SQL);
                        while (rss.next()) {
                            f_getrow = String.format(formatStr, rss.getRow());
                            m1 = rss.getString(1);
                            m2 = rss.getString(2);
                            m3 = rss.getString(3);
                            try (Connection msc33 = DriverManager.getConnection(connectionCTSDB); Statement stt = msc33.createStatement();) {
                                uid4 = uid4.randomUUID();
                                stt.executeUpdate("INSERT INTO dbo.CTPMemberYearLimit(ML000,ML001,ML002,ML003,ML004,ML005,ML006,ML007,ML008) VALUES ('" + uid4 + "','" + uid2 + "','" + yy + "','" + m3 + "','0','0','" + m1 + "','" + m2 + "','N' )");
                                //System.out.println("  CTC續約:" + rss.getRow() + " CTPMemberYearLimit ML000:" + uid4 + "  ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " " + rss.getString(5) + " ML006類別:" + m1 + " ML007代碼:" + m2);
                                System.out.println("  CTC續約:" + f_getrow + " CTPMemberYearLimit ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " " + rss.getString(5) + " ML006類別:" + m1 + " ML007代碼:" + m2);
                            }
                        }
                    } catch (SQLException ex2) {
                        System.out.println("  msconn_renew48 error");
                        System.out.println(ex2.getMessage());
                    }
                    yy++;
                }
            }
            if (period == 12) // 1年合約
            {
                try (Connection msconn_renew12 = DriverManager.getConnection(connectionCTSDB); Statement stmt = msconn_renew12.createStatement();) {
                    String SQL = "SELECT 類別,代碼,管制條件代碼,名稱,管制條件名稱,條件值,會員種類代碼 FROM dbo.VCTPMTypeLimit WHERE 會員種類代碼='" + ctc_code + "' ";
                    ResultSet rss = stmt.executeQuery(SQL);
                    while (rss.next()) {
                        f_getrow = String.format(formatStr, rss.getRow());
                        m1 = rss.getString(1);
                        m2 = rss.getString(2);
                        m3 = rss.getString(3);
                        try (Connection msc33 = DriverManager.getConnection(connectionCTSDB); Statement stt = msc33.createStatement();) {
                            uid4 = uid4.randomUUID();
                            stt.executeUpdate("INSERT INTO dbo.CTPMemberYearLimit(ML000,ML001,ML002,ML003,ML004,ML005,ML006,ML007,ML008) VALUES ('" + uid4 + "','" + uid2 + "','" + 1 + "','" + m3 + "','0','0','" + m1 + "','" + m2 + "','N' )");
                            //System.out.println("  CTC續約:" + rss.getRow() + " CTPMemberYearLimit ML000:" + uid4 + "  ML001:" + uid2 + "  ML002年限:1  ML003管制條件代碼:" + m3 + " " + rss.getString(5) + " ML006類別:" + m1 + " ML007代碼:" + m2);
                            System.out.println("  CTC續約:" + f_getrow + " CTPMemberYearLimit ML001:" + uid2 + "  ML002年限:" + yy + "  ML003管制條件代碼:" + m3 + " " + rss.getString(5) + " ML006類別:" + m1 + " ML007代碼:" + m2);
                        }
                    }
                } catch (SQLException ex2) {
                    System.out.println("  msconn_renew12 error");
                    System.out.println(ex2.getMessage());
                }
            }
            System.out.println("  --------------- End of CTC Process-----------------");
            return mi001;
        }
    }

    public static String manage_ai_report(int get0, String auto, String buy_mode, String get1, String get2, String get3, String get4, String get5, String get6, String get7, String get8, String get9, String get12, String get13, String get14, String get20, String get21, String get22, String cell_phone) throws SQLException, ClassNotFoundException {
        System.out.println("  --------------- Start manage_ai_report Process-----------------");
        String old_expired_day = "";
        String old_product_main_name = "";
        String acn = "";
        int period_t3 = 0;
        String enable = null, expired = null;

        String url2 = "jdbc:mysql://10.20.0.9:3306/car?characterEncoding=UTF-8";
        String user2 = "intit-20200303";
        String pass2 = "jes#2354";

        String a_id = check5_account_id(get0, get2, get3, get4, cell_phone);

        String str_t0 = "SELECT account_id,account_customer_number "
                + "FROM accounts WHERE account_id='" + a_id + "' ORDER BY account_id DESC";
        try (Connection con_t0 = DriverManager.getConnection(url2, user2, pass2);
                Statement st0 = con_t0.createStatement();
                ResultSet rs_t0 = st0.executeQuery(str_t0)) {
            while (rs_t0.next()) {
                acn = rs_t0.getString(2);
            }
            con_t0.close();
        } catch (SQLException ex) {
            System.out.println(get0 + " str_t0 account_customer_number 錯誤 " + ex.getMessage());
        }
        String str_t1 = "SELECT account_id,account_product_expire_datetime as expire,a.product_main_id,product_main_name "
                + "FROM account_products a "
                + "join product_main     b on a.product_main_id=b.product_main_id WHERE account_id='" + a_id + "' ORDER BY expire DESC LIMIT 1,1";
        try (Connection con_t1 = DriverManager.getConnection(url2, user2, pass2);
                Statement st1 = con_t1.createStatement();
                ResultSet rs_t1 = st1.executeQuery(str_t1)) {
            while (rs_t1.next()) {
                old_expired_day = rs_t1.getString(2).substring(0, 10);
                old_product_main_name = rs_t1.getString(4);
                //System.out.println("  " + rs_t.getRow() + " account_id:" + rs_t.getInt(1) + " 前到期日:" + expire1 + " 前配套:" + rs_t.getString(4));
            }
            con_t1.close();
        } catch (SQLException ex) {
            System.out.println(get0 + " str_t1 expire1 錯誤 " + ex.getMessage());
        }
        String str_t2 = "SELECT account_product_enable_datetime,account_product_expire_datetime "
                + "FROM account_products WHERE account_id='" + a_id + "' ORDER BY account_product_expire_datetime DESC LIMIT 0,1";
        try (Connection con_t2 = DriverManager.getConnection(url2, user2, pass2);
                Statement st2 = con_t2.createStatement();
                ResultSet rs_t2 = st2.executeQuery(str_t2)) {
            while (rs_t2.next()) {
                enable = rs_t2.getString(1).substring(0, 10);
                expired = rs_t2.getString(2).substring(0, 10);
            }
            con_t2.close();
        } catch (SQLException ex) {
            System.out.println(get0 + " str_t2 錯誤 " + ex.getMessage());
        }
        String url_managedb = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
        String user_managedb = "intit20200303"; //for localhost
        String pass_managedb = "jes#2354";
        String str_t3 = "SELECT period FROM erp_ai WHERE TT_ERP_ID= '" + get12 + "' ";
        try (Connection con_t3 = DriverManager.getConnection(url_managedb, user_managedb, pass_managedb);
                Statement st3 = con_t3.createStatement();
                ResultSet rs4 = st3.executeQuery(str_t3)) {
            rs4.next();
            period_t3 = rs4.getInt(1);
            con_t3.close();
        } catch (SQLException ex) {
            System.out.println(get0 + " 無配套 " + str_t3);
        }
        //*  save to manage_ai_report for web query
        ////////      從 ERP訂單   新增進  manage_ai_report
        int manage_air_id;
        String myUrl = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
        String query_report = "INSERT INTO manage_ai_report ("
                + "air00,air01,air02,air03,air04,air05,air06,air07,air08,air09,air10,air11,air12,air13,air14,air15,air16,air17,air18,air19,air20,air21)"
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //        "VALUES (0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,2,2,2)";
        //                 0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0,1

        try (Connection connai = DriverManager.getConnection(myUrl, "intit20200303", "jes#2354")) {
            //## create the mysql insert preparedstatement
            PreparedStatement ps2 = connai.prepareStatement(query_report);
            ps2.setInt(1, 0);                           //air00
            ps2.setString(2, auto + buy_mode);          //air01 1+buy_mode
            ps2.setString(3, old_product_main_name);    //AIR02 old_product_main_name
            ps2.setString(4, get13);                    //air03 now
            ps2.setString(5, String.valueOf(period_t3));//air04 period
            ps2.setString(6, acn);     //air05 CTC:
            ps2.setString(7, get9);    //air06 OLD客編
            ps2.setString(8, get3);    //air07 XX汽車
            ps2.setString(9, get2);    //air08 ERP客編
            ps2.setString(10, get4);   //air09 聯絡人
            ps2.setString(11, get5);   //air10 TEL
            ps2.setString(12, get7);   //air11 FAX
            ps2.setString(13, get8);   //air12 地址
            ps2.setString(14, get6);   //air13 手機號碼
            ps2.setString(15, get21);  //air14 SALES
            ps2.setString(16, get20);  //air15 AREA
            ps2.setString(17, get14.substring(0, 10));  //air16 合約日
            ps2.setString(18, get1);   //air17 SX202-XXXXXXX
            ps2.setString(19, enable); //air18 啟用
            ps2.setString(20, expired);//air19 到期;
            ps2.setString(21, old_expired_day);//air20 舊約到期日);
            ps2.setString(22, get22);  //air21 remark
            ps2.executeUpdate();
            ResultSet air_id = ps2.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY   KEY
            air_id.next();                             //read
            manage_air_id = air_id.getInt(1); // 字串轉數字
            System.out.println(get0 + " 新增 manage_ai_report_id : " + manage_air_id);
            connai.close();
        } catch (SQLException ex) {
            //System.out.println(rs.getRow() + " manage_ai_report 錯誤 " + ex.getMessage() +" "+query_report);
            System.out.println(get0 + " manage_ai_report 開通錯誤 " + ex.getMessage());
        }
        System.out.println("  --------------- End of manage_ai_report Process-----------------");
        return null;
    }

    public static String manage_erp(int get0, String sx, String enable, String expire, String expire1) throws SQLException, ClassNotFoundException {
        int manage_erp_id;
        try {
            //#create a mysql database connection
            //String myDriver = "org.gjt.mm.mysql.Driver";
            String myUrl = "jdbc:mysql://10.20.0.17:3306/managedb?characterEncoding=UTF-8";
            Class.forName("com.mysql.jdbc.Driver");
            try (Connection conn = DriverManager.getConnection(myUrl, "intit20200303", "jes#2354")) {
                String query1 = "INSERT INTO manage_erp (erp00,erp14,erp15,erp16,erp17,erp18,erp19,erp20,erp21,erp22)"
                        + "VALUES (?,?,?,?,?,?,?,?,?,?)";
                //        "VALUES (0,0,0,0,0,0,0,0,0,0)";
                //                 0,1,2,3,4,5,6,7,8,9
                //## create the mysql insert preparedstatement
                PreparedStatement ps2 = conn.prepareStatement(query1);
                ps2.setInt(1, 0);           //erp00
                ps2.setString(2, sx);       //erp14 合約
                ps2.setString(3, "");       //erp15
                ps2.setString(4, enable);   //erp16
                ps2.setString(5, expire);   //erp17 到期日
                ps2.setString(6, expire1);  //erp18 舊約到期日
                ps2.setString(7, "");       //erp19 NEXT BUY    
                ps2.setString(8, "");       //erp20 NEXT MISC-1200
                ps2.setString(9, "");       //erp21 PRODUCT_MAIN
                ps2.setString(10, "");
                ps2.executeUpdate();
                ResultSet erp_id = ps2.getGeneratedKeys(); // 取得產生的AUTO_INCREMENY   KEY
                erp_id.next();                             //read
                manage_erp_id = erp_id.getInt(1);      // 字串轉數字
                System.out.println(get0 + " 新增 manage_erp_id : " + manage_erp_id);
            }
        } catch (SQLException ex) {
            System.out.println(get0 + " INSERT INTO manage_erp 錯誤 " + ex.getMessage());
        }
        return "0";
    }

}
