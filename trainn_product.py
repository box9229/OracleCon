import requests,json
import pymysql

#url = 'http://172.105.219.92:80/carstar/web/api/login'
#data = {'adminAccount': 'admin', 'password': '12345'}
#headers={'Content-Type':'application/json','charset':'utf-8'}
#r = requests.post(url=url,json=data,headers=headers)
#r = requests.get('https://api.github.com/user', auth=('user', 'pass'))
#r = requests.get('172.105.219.92:80/carstar/web/api/login',auth=('jesse.chen','jes#2354')
#r = requests.post(url='http://10.20.0.89:80//admin_sys/index/main', json={'adminAccount': 'root','password':'ptygtq'}, headers={'Content-Type':'application/json','charset':'utf-8'})
#r = requests.get(url='http://10.20.0.89:80/carstar/web/api/login', json={'adminAccount': 'jesse.chen','password':'jes#2354'}, headers={'Content-Type':'application/json','charset':'utf-8'})
#r = requests.post(url='http://172.105.219.92:80/carstar/web/api/login', json={'adminAccount': 'admin','password':'12345'}, headers={'Content-Type':'application/json','charset':'utf-8'})
#print ("header:",r.headers) print (r.headers['Content-Type']) print (r.headers['Transfer-Encoding']) print (r.headers['Date'])

######################
###   PRODUCTION   ###

print ("*****'/carstar/web/api/login'**************************")
url    ='https://training.carstar.com.tw:8443/carstar/web/api/login' 
data   ={'adminAccount': 'jesse.chen', 'password': 'jes#2354'}

headers={'Content-Type':'application/json','charset':'utf-8'}
r = requests.post(url=url,json=data,headers=headers)

if r.status_code==200:
    print ("status_code:",r.status_code) 
    #print (r.headers['Content-Type']) #print (r.content)
    #print ("\njson:\n")     
    #print (re.json())
    texts = json.loads(r.text)
    print ("token: " + texts['token'])

    '''
    print ("*****'/carstar/api/register'***************************")
    url_reg   ='http://172.105.219.92:80/carstar/api/register' 
    #url_reg   ='https://training.carstar.com.tw:8443/carstar/api/register'                 
    #url    ='https://10.20.0.87:8080/carstar/web/api/login' 
    #data   = {'adminAccount': 'admin', 'password': '123456'}
 
    headers_reg={'Content-Type':'application/json'} 
    body_reg   ={'account': account,'userName': userName,'password':account,'passwordRetype':account,'imei':'0000000','mobileOs':1} 
    rs = requests.post(url=url_reg,json=body_reg,headers=headers_reg)
    if rs.status_code==200:
        print (rs.json())
    else:
        print (rs.json())
    #'''

    ##########    
    opac_id=70  #Plus E
    '''
    #產生激活碼  先login 取得token 即可以產生激活碼  參數 token opca_id Counts PlantName sales
    url   ='https://training.carstar.com.tw:8443/carstar/web/api-asi/actcode/addActCode'
    data  = {'token':texts['token'], 'opacId':opac_id, 'actCodeCounts':'1', 'maintenancePlantName':'保養廠', 'salesCode':'00001'}
    r    = requests.post(url=url,json=data,headers=headers)
    addactCode=json.loads(r.text)
    print(addactCode)
    print (r.json())
    #'''
    #'''
    #mysql_command=("SELECT a.prod_id,b.off_prod_id,c.opac_id,d.act_code_id,d.phone_no,d.act_code,exchange_status, send_status "
    #" FROM tb_product a join tb_off_product b ON a.prod_id=b.prod_id join tb_off_product_act_code c ON b.off_prod_id=c.off_prod_id "
    #" join tb_act_code d ON c.opac_id=d.opac_id WHERE a.prod_id='66' and creator_id='35' AND exchange_status='0'  AND send_status='0' ")

    mysql_command=("SELECT d.opac_id,d.act_code FROM tb_product a \
join tb_off_product b ON a.prod_id=b.prod_id \
join tb_off_product_act_code c ON b.off_prod_id=c.off_prod_id \
join tb_act_code d ON c.opac_id=d.opac_id WHERE a.prod_id='70' and creator_id='35'" )

    #db = pymysql.connect(host='210.65.89.179',user='root', password='#csit@carstar#', database='carstar') 
    db = pymysql.connect(host='training.carstar.com.tw',user="root",password="#csit@carstar#",database="carstar")
    cursor=db.cursor()
    cursor.execute(mysql_command)
    data = cursor.fetchall()
    for a in data:
        #print(a[0],a[1])
        print(a)
    act_code=a[1]

    mysql_command=("SELECT user_id,user_name,user_account from tb_users where user_account ='%s' " %account )
    db = pymysql.connect(host='training.carstar.com.tw',user="root",password="#csit@carstar#",database="carstar")
    cursor=db.cursor()
    cursor.execute(mysql_command)
    data = cursor.fetchall()
    for b in data:
        print(b[0],b[1],b[2])
    user_id=b[0] 

    '''
    #################
    # 激活碼開通兌換
    #################
    print ("*****'/web/api-asi/actcode/activateActCodeByWeb'*******")    
    url   ='https://training.carstar.com.tw:8443/carstar/web/api-asi/actcode/activateActCodeByWeb'
    data = {'token':texts['token'], 'userId':user_id,'actCode':act_code}
    r    = requests.post(url=url,json=data,headers=headers)
    print(r)
    actCode=json.loads(r.text)
    print(actCode)    
    db.close()
    #'''

    
