import os
import re       # regular express module
import cx_Oracle
import requests,json
import pymysql
import MysqlToExcel
import datetime

#os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.ZHT16BIG5' #zh_TW.BIG5
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.AL32UTF8'  #zh_TW.UTF-8

#LOCATION = "C:\Oracle\instantclient_19_5" # Oracle 11.2.0.4 depent on carstar Oracle version 
LOCATION = "C:\Oracle\instantclient_11_2" # Oracle 11.2.0.4 depent on carstar Oracle version 
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]

def training_login():
    token=0

def check_expired(erp_num):
    ai_expired_cmd =("SELECT \
    accounts.account_number AS ERP, date_format(account_products.account_product_expire_datetime,'%%Y-%%m-%%d') AS EXPIRE \
    FROM accounts JOIN account_products ON account_products.account_id = accounts.account_id \
    WHERE accounts.account_number = '%s' \
    ORDER BY EXPIRE DESC LIMIT 0,1" %erp_num
    )
    try:
        db = pymysql.connect(host='10.20.0.9',user='intit-20200303', password='jes#2354', database='car') 
        cursor=db.cursor()
        cursor.execute(ai_expired_cmd)
        data = cursor.fetchall()
        for ex in data:
            print('         合約到期日：%s'%ex[1])
            expire=ex[1]
        if not data:  #查詢結果為空
            print('         %s 查詢合約到期日結果為空'%erp_num)
            expire=''
    except Exception as e:
        print('        合約到期日 Error',e)
        
    return expire

def insert_manage_training(id,buy_date,package,expire,account_number,account_name,userName,tel,cell,addr,c_sales,p,year,regiok,register,actok,activate,contract):
    connn = pymysql.connect(host='10.20.0.17',user="intit20200303",password="jes#2354",database="managedb")
    city = ('id','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','','','','','')
    try: 
        with connn.cursor() as cur:

            cur.execute('select train00 from manage_training')
            row = cur.fetchall()  # 為了INSERT 獲取最後的ID 

            cur.execute('INSERT INTO manage_training VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (id, buy_date, package, expire, account_number,account_name,userName,tel,cell,addr,c_sales, p,year,regiok,register,actok,activate,contract, city[17], city[18], city[19]))
            #print('         new cur id inserted: ',cur.lastrowid)
            #print('         conn.insert_id():',connn.insert_id())
            connn.commit() 
            
    finally:
        connn.close()

def training_register(account,userName):
                    ###################################################
                    ### 1 register  account username account account
                    ###################################################
                    print ('         *****register')
                    url_reg   ='https://training.carstar.com.tw:8443/carstar/api/register'                 
                    data   = {'adminAccount': 'admin', 'password': '123456'}
   
                    headers_reg={'Content-Type':'application/json'} 
                    body_reg   ={'account': account,'userName': userName,'password':account,'passwordRetype':account,'imei':'0000000','mobileOs':1} 
                    rs = requests.post(url=url_reg,json=body_reg,headers=headers_reg)
                    if rs.status_code==200:
                        texts = json.loads(rs.text)
                        #print ('         ',texts)
                        message=texts['message']
                        print ('         register:',message)
                    return  message
# end of training_register
def training_actCode(opac_id,account_name,sales):
    '''
    '''
# End of traning_atCode


def check1_ai_phone(erp_num):
    print('         --- ERP Number:',erp_num)
    print('         --- start check device_phone')
    mysql_command =("SELECT \
    accounts.account_id, \
    accounts.account_customer_number AS 會員編號, \
    accounts.account_number AS ERP, \
    accounts.account_name AS 保養廠名稱, \
    accounts.account_person_in_charge_name AS 會員名稱, \
    accounts.account_cell_phone AS 會員手機, \
    account_devices.account_device_phone AS 註冊手機, \
    product_main.product_main_name AS 配套名稱, \
    date_format(account_products.account_product_buy_datetime,'%%Y-%%m-%%d') AS BUY, \
    date_format(account_products.account_product_expire_datetime,'%%Y-%%m-%%d') AS EXPIRE \
    FROM account_devices \
    LEFT JOIN accounts ON accounts.account_id = account_devices.account_id \
    LEFT JOIN account_products ON account_products.account_id = accounts.account_id \
    LEFT JOIN product_main ON product_main.product_main_id = account_products.product_main_id \
    WHERE accounts.account_number = '%s' \
    ORDER BY EXPIRE DESC " %erp_num
    )

    db = pymysql.connect(host='10.20.0.9',user='intit-20200303', password='jes#2354', database='car') 
    cursor=db.cursor()
    cursor.execute(mysql_command)
    data = cursor.fetchall()
    c=0
    aiphone=phone1=phone2=phone3=phone4=0
    for ai in data:
        c+=1
        print('         AI:',c,ai[0],ai[1],ai[2],ai[3],ai[4],ai[5],ai[6],ai[7],ai[8])
        if c==1:
            phone1=ai[6]
            print('         AI:',c,'--->',ai[6],ai[7],ai[8])
            aiphone=phone1
        if c==2:
            phone2=ai[6]
            if phone1==phone2:
                print('         AI:',c,'--->',ai[6],ai[7],ai[8])
                aiphone=phone2
            else:
                print('         AI:',c,'phone Err.....')
        if c==3:
            phone3=ai[6]
            if phone1==phone2:
                if phone2==phone3:
                    print('         AI:',c,'--->',ai[6],ai[7],ai[8])
                    aiphone=phone3
                else:
                    print('         AI:',c,'phone Err.....')
        if c==4:
            phone4=ai[6]
            if phone1==phone2:
                if phone2==phone3:
                    if phone3==phone4:
                        print('         AI:',c,'--->',ai[6],ai[7],ai[8])
                        aiphone=phone4
                    else:
                        print('         AI:',c,'phone Err.....')
    print('         AI phone:',aiphone)
    print('         --- end of device_phone')
    return aiphone
# end check1_ai_phone

try:
    conn=cx_Oracle.connect('ca1_db','ca1_db','10.20.0.241:1521/topprod')
    cursor=conn.cursor()
except:
    print('Connect Oracle error.')
#ver = conn.version.split(".") print("頂新資料庫 Oracle version:") print(ver)
#**********************************************************************************************************************
cmd0 = ("SELECT oea01,ta_oea19,NVL(occud04,'0'),occ28,NVL(tc_oeba21,'0'), "   # 0 1 2 3 4 
"NVL(ta_oea03,'0'),NVL(a1.ima02 ,'0'),NVL(to_char(OEA02,'yyyy-mm-dd'),'0'), "       # 5 6 7  
"tc_oebb05,a2.ima02, NVL(to_char(tc_oeba11,'yyyy-mm-dd'),'0'), "                    # 8 9 10
"NVL(to_char(tc_oeba12,'yyyy-mm-dd'),'0'),NVL(ta_oea06,'0'),"                       # 11 12
"NVL(gen02,'0'),occ261,occ241,NVL(gem02,'0') " #,NVL(oao06 ,'0')                    # 13 14 15 16
"FROM ca1_db.oea_file "
"LEFT JOIN ca1_db.occ_file on TA_OEA19=OCC01 LEFT JOIN ca1_db.ima_file a1 on ta_oea03=a1.ima01 "
"LEFT JOIN ca1_db.tc_oebb_file on tc_oebb01=oea01 "
"LEFT JOIN ca1_db.ima_file a2 on a2.ima01=tc_oebb05 " 
"LEFT JOIN ca1_db.tc_oeba_file on tc_oeba01=tc_oebb01 and tc_oeba02=tc_oebb02 "
"LEFT JOIN ca1_db.gen_file  on oea14=gen01  LEFT JOIN ca1_db.gem_file on oea15=gem01 "#,LEFT JOIN ca1_db.oao_file on oea01=oao01 " 
"WHERE oea00='0' AND oeaconf='Y' AND to_char(OEA02,'yyyy-mm-dd') = to_char(sysdate-3,'yyyy-mm-dd') AND tc_oebb05='MISC-1100001' "
#"WHERE oea00='0' AND oeaconf='Y' AND to_char(OEA02,'yyyy-mm-dd') >= '2021-01-01' AND tc_oebb05='MISC-1100001' "
"ORDER BY oea01" 
) # //MISC-1100001 會員服務
cmd1 = ("SELECT oea01,ta_oea19,NVL(occud04,'0'),occ28,NVL(tc_oeba21,'0'), "   # 0 1 2 3 4 
"NVL(ta_oea03,'0'),NVL(a1.ima02 ,'0'),NVL(to_char(OEA02,'yyyy-mm-dd'),'0'), "       # 5 6 7  
"tc_oebb05,a2.ima02, NVL(to_char(tc_oeba11,'yyyy-mm-dd'),'0'), "                    # 8 9 10
"NVL(to_char(tc_oeba12,'yyyy-mm-dd'),'0'),NVL(ta_oea06,'0'),"                       # 11 12
"NVL(gen02,'0'),occ261,occ241,NVL(gem02,'0') " #,NVL(oao06 ,'0')                    # 13 14 15 16
"FROM ca1_db.oea_file "
"LEFT JOIN ca1_db.occ_file on TA_OEA19=OCC01 LEFT JOIN ca1_db.ima_file a1 on ta_oea03=a1.ima01 "
"LEFT JOIN ca1_db.tc_oebb_file on tc_oebb01=oea01 "
"LEFT JOIN ca1_db.ima_file a2 on a2.ima01=tc_oebb05 " 
"LEFT JOIN ca1_db.tc_oeba_file on tc_oeba01=tc_oebb01 and tc_oeba02=tc_oebb02 "
"LEFT JOIN ca1_db.gen_file  on oea14=gen01  LEFT JOIN ca1_db.gem_file on oea15=gem01 "#,LEFT JOIN ca1_db.oao_file on oea01=oao01 " 
"WHERE oea00='0' AND oeaconf='Y' AND to_char(OEA02,'yyyy-mm-dd') = to_char(sysdate-1,'yyyy-mm-dd') AND tc_oebb05='MISC-1100001' "
#"WHERE oea00='0' AND oeaconf='Y' AND to_char(OEA02,'yyyy-mm-dd') >= '2021-01-01' AND tc_oebb05='MISC-1100001' "
"ORDER BY oea01" 
) # //MISC-1100001 會員服務

weekdate=datetime.date.today().strftime("%A")
if 'Monday' in weekdate:
    oracle_cmd=cmd0
else: 
    oracle_cmd=cmd1
try:
    cursor.execute(oracle_cmd)
except Exception as e:
    print('Oracle_cmd error !!!',e)
data = cursor.fetchall()
#rownum=int(cursor.rowcount)
#print(rownum)
'''
record=0;
for erp in data:
    record+=1
    print(record,erp[0],erp[1],erp[2],erp[3],erp[4],erp[5],erp[6],erp[7],erp[8],erp[9],erp[10],erp[11],erp[12],erp[13],erp[14]))
print('total:',record)
'''
record=0;
plus=noplus=nophone=aiphone=account=0
for erp in data:
    record+=1
    if record < 50:  # and record >2:
        contract=erp[0] 
        account_number=erp[1]   
        account_name=erp[2] #'薪譯汽車'  
        userName=erp[3]     #'張富墉'
        cell=erp[4]         #'0963096898'      
        misc_code=erp[5]
        package=erp[6]      #'2021 輕資會員Plus A-FUSTER'
        buy_date=erp[7]     
        sales=erp[13]       #'陳裕文'
        c_sales=erp[13]     # for db
        tel=erp[14]         # for db
        addr=erp[15]        # for db
        #package=package.replace(' ','')    # 拿掉空白字元
        #print('%2d'%record,'     ',  erp[0],        erp[1],      erp[2],erp[3],   erp[4],erp[5],erp[6],erp[7],erp[13],erp[14],erp[15])    
        #print('%2d'%record,'     ',contract,buy_date,account_number,account_name,userName,  cell,misc_code,package,sales,tel,addr)
        #if record == 4:
        #    cell='0932105188'

        fullstring = package
        plusstring = 'Plus'
        if plusstring in fullstring or '家族' in fullstring:
            #print('%2d'%record,'    ',contract,account_number,account_name,userName,cell,misc_code,package,sales,buy_date)
            if(cell=='0'):
                print('%2d'%record,'     ',contract,buy_date,account_number,account_name,userName,cell,misc_code,package,sales)
                print('         ERP沒有手機號碼，查AI手機號碼')                
                aiphone=check1_ai_phone(account_number)   # check ai mobilephone by erp[1] EPP  account_number
                if aiphone==0:
                    nophone+=1
                    print('         \033[4;40m!!!查無 AI 手機號碼\033[0m')
                cell=aiphone                
            if(cell!='0'):   #ERP 有手機號碼
                try:
                    phone = re.sub('\-', '', cell) #regular express 第一個參數是要取代的,第二個參數是想變成的 #第二個參數若為空，則可以變成取代功能 #第三個參數為原本的字串                
                    if len(phone) == 10 and re.match(r'0\d{9}',phone): #輸入手機號碼，判斷手機號碼是否為10位，是否為0開頭的數值
                        account = phone

                except Exception as e:  #else
                    print('        \033[4;40m!!!!! 手機 ERROR\033[0m ',contract,buy_date,account_number,account_name,userName,cell,misc_code,package,sales,e)
                    account=phone=0
                    
                if 'Plus A' in fullstring:
                    plus+=1
                    opac_id=71
                    prod_id=68
                    plus_code='PLUSA0001'   #Plus A 一年
                    p='A'
                    year=1
                    print('%2d'%record,'     ',contract,buy_date,account_number,account_name,userName,  cell,misc_code,package,sales)
                    print('%2d'%record,'%2d'%plus,'  ',account,account_name,userName,misc_code,sales,opac_id,prod_id,plus_code)

                if 'Plus E' in fullstring:
                    plus+=1
                    opac_id=70
                    prod_id=70
                    plus_code='PLUSE0001'   #Plus E 一年
                    p='E'
                    year=1
                    if 'G-Scan' in fullstring:
                        opac_id=73
                        prod_id=71
                        plus_code='PLUSP0002' #Plus P 三年
                        p='+'
                        year=3
                    print('%2d'%record,'     ',contract,buy_date,account_number,account_name,userName,  cell,misc_code,package,sales)
                    print('%2d'%record,'%2d'%plus,'  ',account,account_name,userName,misc_code,sales,opac_id,prod_id,plus_code)

                if 'Plus +' in fullstring:
                    plus+=1
                    opac_id=69
                    prod_id=72
                    plus_code='PLUSP0001'    #Plus P 一年
                    p='+'
                    year=1
                    if 'G-Scan' in fullstring:
                        opac_id=72
                        prod_id=73
                        plus_code='PLUSP0002' #Plus P 三年
                        p='+'
                        year=3
                    print('%2d'%record,'     ',contract,buy_date,account_number,account_name,userName,  cell,misc_code,package,sales)
                    print('%2d'%record,'%2d'%plus,'  ',account,account_name,userName,misc_code,sales,opac_id,prod_id,plus_code)

                if '家族' in fullstring:
                    plus+=1
                    opac_id=72
                    prod_id=73
                    plus_code='PLUSP0002'     #Plus P 三年
                    p='+'
                    year=3
                    print('%2d'%record,'     ',contract,buy_date,account_number,account_name,userName,  cell,misc_code,package,sales)
                    print('%2d'%record,'%2d'%plus,'  ',account,account_name,userName,misc_code,sales,opac_id,prod_id,plus_code)                

                ###################################################
                # login to get token
                ###################################################
                print('         start..............................')
                #print("*****'/carstar/web/api/login' to get token ***")
                #print('         ***** login *****')
                #url    ='http://172.105.219.92:80/carstar/web/api/login'
                #data   ={'adminAccount': 'admin', 'password': '12345'}

                url    ='https://training.carstar.com.tw:8443/carstar/web/api/login'
                data   ={'adminAccount': 'jesse.chen', 'password': 'jes#2354'}
                headers={'Content-Type':'application/json','charset':'utf-8'}
                rrr = requests.post(url=url,json=data,headers=headers)
                if rrr.status_code==200:
                    #print ("         status_code:",rrr.status_code) 
                    #print (rrr.json())
                    texts = json.loads(rrr.text)                
                    message=texts['message']
                    token=texts['token']
                    admin=texts['admin']
                    adminName=admin['adminName']
                    print ('         message:',message,adminName,token)  

                    ########################################################################################
                    # register='Succes'
                    ########################################################################################
                    register=training_register(account,userName)
                    if register == 'Success' or register == 'Account already exist':
                        regiok='Y'
                    else:   
                        regiok='N'

                    #'''
                    #-----------
                    # SALES CODE
                    #-----------
                    #'''
                    sales = "%"+sales+"%"
                    mysql_sales_cmd=("SELECT sales_code, sales_name FROM tb_sales WHERE sales_name LIKE '%s' " %sales )
                    try:                        
                        db = pymysql.connect(host='training.carstar.com.tw',user="root",password="#csit@carstar#",database="carstar")
                        cursor=db.cursor()
                        cursor.execute(mysql_sales_cmd)
                       
                        data = cursor.fetchall()
                        for s in data:
                            print('         業務代號：%s'%s[0],sales)
                        sales=s[0]
                    except Exception as e:
                        print('        Sales Code Error',e)
                    #'''
                    
                    ########################################################################################
                    # 產生激活碼  先login 取得token 即可以產生激活碼  參數 token opca_id Counts PlantName sales
                    ########################################################################################
                    #'''
                    print('         *****addActCode')    
                    url   ='https://training.carstar.com.tw:8443/carstar/web/api-asi/actcode/addActCode'
                    data  = {'token':texts['token'], 'opacId':opac_id, 'actCodeCounts':'1', 'maintenancePlantName':account_name, 'salesCode':sales}
                    radd    = requests.post(url=url,json=data,headers=headers)
                    ActCode=json.loads(radd.text)
                    addActCode=ActCode['message']
                    if addActCode == 'Success':
                        addActok='Y'
                    else: 
                        addActok='N'
                    print('         產生激活碼：',addActCode)
                    #'''
                    #-----------------
                    #  select act_code 
                    # ----------------                      
                    #'''
                    mysql_command=("SELECT d.opac_id,d.act_code FROM tb_product a \
                        join tb_off_product b ON a.prod_id=b.prod_id \
                        join tb_off_product_act_code c ON b.off_prod_id=c.off_prod_id \
                        join tb_act_code d ON c.opac_id=d.opac_id WHERE a.prod_id='%s' and creator_id='35' AND exchange_status='0' " %prod_id )
                    try:
                        db = pymysql.connect(host='training.carstar.com.tw',user="root",password="#csit@carstar#",database="carstar")
                        cursor=db.cursor()
                        cursor.execute(mysql_command)
                        data = cursor.fetchall()
                        for act in data:
                            act_code=act[1]
                        print('         激活碼  ：',act_code)
                        #act_code=act[1]
                    except Exception as e:
                        print('         act_code Error',e)
                    
                    mysql_command=("SELECT user_id,user_name,user_account from tb_users where user_account ='%s' " %account )
                    try:
                        db = pymysql.connect(host='training.carstar.com.tw',user="root",password="#csit@carstar#",database="carstar")
                        cursor=db.cursor()
                        cursor.execute(mysql_command)
                        data = cursor.fetchall()
                        for user in data:
                            print('         用戶代碼：',user[0],user[1])
                            print('         開通帳號：',user[2])
                        user_id=user[0]
                    except Exception as e:
                        print('         user_id Error',e)

                    #'''
                    ##########################################################
                    # 激活碼開通兌換
                    ##########################################################
                    #'''
                    print('         *****activateActCode')    
                    #url   ='https://172.105.219.92:80/carstar/web/api-asi/actcode/activateActCodeByWeb'
                    url   ='https://training.carstar.com.tw:8443/carstar/web/api-asi/actcode/activateActCodeByWeb'
                    data = {'token':texts['token'], 'userId':user_id,'actCode':act_code}
                    r    = requests.post(url=url,json=data,headers=headers)
                    #print(r)
                    actCodeByweb=json.loads(r.text)
                    activate=actCodeByweb['message']
                    #'''
                    if activate == 'Success' or activate == '此線下產品仍在觀看效期內無法重複開通':
                        actok='Y'
                    else: 
                        actok='N'
                        if addActok =='N':             #產生激活碼失敗時 加上訊息 example 此業務代碼不存在
                            activate=activate+'+'+addActCode                            
                    print('         啟用激活碼：',activate)
                    db.close()

                    expire=check_expired(account_number)
                    print('         end................................')
                    insert_manage_training(id,buy_date,package,expire,account_number,account_name,userName,tel,cell,addr,c_sales,p,year,regiok,register,actok,activate,contract)
        else:  #非創育的配套
            noplus+=1            
            #print('%2d'%record,'     ',contract,buy_date,account_number,account_name,userName,  cell,misc_code,package,sales)
print('=============')                
print('創育開通啟用:')
print("total :",record,'plus:',plus,'noplus:',noplus,'fail:',nophone)
#MysqlToExcel.MysqlToExcel().generate_table()
