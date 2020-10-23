# To calculate aqi value for each incoming record from the kinesis stream
def aqi_calculator(sensor_list):
    category_range=["Good","Moderate","Unhealthy for sensitive groups","Unhealthy","Very Unhealthy","Hazardous"]
    aqi_range=[[0,50],[51,100],[101,150],[151,200],[201,300],[301,500]]
    pm2_range=[[0,12],[12.1,35.4],[35.5,55.4],[55.5,150.4],[150.5,250.4],[250.5,500.4]]
    pm10_range=[[0,54],[55,154],[155,254],[255,354],[355,424],[425,604]]
    no2_range=[[0,53],[54,100],[101,360],[361,649],[650,1249],[1250,2049]]
    so2_range=[[0,35],[36,75],[76,185],[186,304],[305,604],[605,1004]]
    co_range=[[0,4.4],[4.5,9.4],[9.5,12.4],[12.5,15.4],[15.5,30.4],[30.5,50.4]]
    o3_range=[[0,54],[55,70],[71,85],[86,105],[106,200]]

    # aqi formula
    def aqi(r1,r2,val):
        return float((r1[1]-r1[0]))/(r2[1]-r2[0])*(val-r2[0])+(r1[0])

    aqi_pm2,aqi_pm10,aqi_no2,aqi_so2,aqi_co,aqi_o3,max_aqi=0,0,0,0,0,0,0
    aqi_list=sensor_list.copy()

    for sensor_dict in aqi_list:
        for i,j in sensor_dict.items():
            if i=="PM25":
                for x,y in enumerate(pm2_range):
                    if y[0]<=j<=y[1]:
                        aqi_pm2=aqi(aqi_range[x],y,j)

            elif i=="PM10":
                for x,y in enumerate(pm10_range):
                    if y[0]<=j<=y[1]:
                        aqi_pm10=aqi(aqi_range[x],y,j)
            elif i=="NO2":
                for x,y in enumerate(no2_range):
                    if y[0]<=j<=y[1]:
                        aqi_no2=aqi(aqi_range[x],y,j)

            elif i=="SO2":
                for x,y in enumerate(so2_range):
                    if y[0]<=j<=y[1]:
                        aqi_so2=aqi(aqi_range[x],y,j)

            elif i=="CO":
                for x,y in enumerate(co_range):
                    if y[0]<=j<=y[1]:
                        aqi_co=aqi(aqi_range[x],y,j)

            elif i=="O3":
                for x,y in enumerate(o3_range):
                    if y[0]<=j<=y[1]:
                        aqi_o3=aqi(aqi_range[x],y,j)

        max_aqi=max(aqi_pm2,aqi_pm10,aqi_no2,aqi_so2,aqi_co,aqi_o3)

        for x,y in enumerate(aqi_range):
            if y[0]<=max_aqi<=y[1]:
                sensor_dict["AQI_Category"]=category_range[x]
                sensor_dict["AQI_Value"]=float("{:.2f}".format(max_aqi))

    return aqi_list