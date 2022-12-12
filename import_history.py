from glob import glob
import sys

from pandas import DataFrame, read_csv
import pandas as pd
import psycopg2
import datetime

def calcAQI(Cp, Ih, Il, BPh, BPl):
    a = Ih - Il
    b = BPh - BPl
    c = Cp - BPl
    return round((a/b) * c + Il)

def aqi_convert(pm):
    #print (pm)
    pm = float(pm)

    if (pm > 350.5):
        return calcAQI(pm, 500, 401, 500.4, 350.5) #Hazardous
    elif (pm > 250.5):
        return calcAQI(pm, 400, 301, 350.4, 250.5) #Hazardous
    elif (pm > 150.5):
        return calcAQI(pm, 300, 201, 250.4, 150.5) #Very Unhealthy
    elif (pm > 55.5):
        return calcAQI(pm, 200, 151, 150.4, 55.5) #Unhealthy
    elif pm > 35.5:
        return calcAQI(pm, 150, 101, 55.4, 35.5) #Unhealthy for Sensitive Groups
    elif pm > 12.1:
        return calcAQI(pm, 100, 51, 35.4, 12.1) #Moderate
    elif pm >= 0:
        return calcAQI(pm, 50, 0, 12, 0) #Good
    elif pm >= 1000:
        return None   
    else:
        return None
          

pg_password = sys.argv[2]
#open the db
try:
    conn = psycopg2.connect("dbname='db_ts_obs' host='x.x.x.x' port='5432' user='postgres' password='"+pg_password+"'")
except:
    print ("Failed to open the db.")

#Initiate the cursor
cursor = conn.cursor()

file = sys.argv[1]


allFiles = sorted(glob(sys.argv[1]+"/*.csv"))
for file in allFiles:

    #e = datetime.datetime.now()
    print(datetime.datetime.now())
    print (file)

    df = pd.read_csv(file, skiprows=1,header=None)
    
    for i in df.index:


        #print (i)
        
        row_id = str(df[0][i])
        row_id = row_id.zfill(2)
        #print (row_id)

        this_ts = str(df[0][i])
        sensor_id = str(df[1][i])
        dataset_id = 'purpleair_'+sensor_id
        
        pm25_a = str(df[2][i])
        pm25_b = str(df[3][i])        

        #print (this_ts+','+sensor_id+','+pm25_a+','+pm25_b)

        aqi_a = aqi_convert(pm25_a)
        #print (aqi_a)

        aqi_b = aqi_convert(pm25_b)
        #print (aqi_b)
  
        #can run below with assumed commit per file OR try/except for individual problem/duplicate rows
        query = "INSERT INTO ts_obs (dataset_id,m_date,m_type,m_tag,m_value,m_value_2,m_value_3,m_value_4) VALUES ('"+dataset_id+"','"+this_ts+"','pm_25','all',"+pm25_a+","+pm25_b+","+str(aqi_a)+","+str(aqi_b)+")"            
        #values = (dataset_id,this_ts,'pm_25','all',pm25_a,pm25_b,str(aqi_a),str(aqi_b))
        #print (query)
        #cursor.execute(query)

        #'''
        try:
            cursor.execute(query)
        except psycopg2.IntegrityError:
            conn.rollback()
        else:
            conn.commit()
        #'''
        
    #conn.commit()

# Close the cursor
cursor.close()

# Close the database connection
conn.close()

'''
function aqiFromPM(pm) {
    if (isNaN(pm)) return "-"; 
    if (pm == undefined) return "-";
    if (pm < 0) return pm; 
    if (pm > 1000) return "-"; 
    /*                                  AQI         RAW PM2.5
    Good                               0 - 50   |   0.0 – 12.0
    Moderate                          51 - 100  |  12.1 – 35.4
    Unhealthy for Sensitive Groups   101 – 150  |  35.5 – 55.4
    Unhealthy                        151 – 200  |  55.5 – 150.4
    Very Unhealthy                   201 – 300  |  150.5 – 250.4
    Hazardous                        301 – 400  |  250.5 – 350.4
    Hazardous                        401 – 500  |  350.5 – 500.4
    */
'''
  

