from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import subprocess
subprocess.run("sudo pip3 install matplotlib", stdout=subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
import matplotlib.pyplot as plt
import numpy as np

def avgCarVelocity(dataFrameReader, src, pickup, dropoff, dist):
    return dataFrameReader.option("header","true")\
                    .option("inferSchema", value = True)\
                    .csv(src)\
                    .withColumn("Velocity",col(dist)/((unix_timestamp(dropoff)\
                    -unix_timestamp(pickup))/3600))\
                    .select(avg("Velocity")).head()[0]

if __name__ == "__main__":
    session = SparkSession.builder.appName("Average Velocity").getOrCreate()
    
    dataFrameReader = session.read
    greenTaxiVelocity = []
    yellowTaxiVelocity = []
    dateArray = ["2019-05", "2020-05"]
    greenTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/green_tripdata_2019-05.csv","lpep_pickup_datetime","lpep_dropoff_datetime","trip_distance"))
    #print("green taxi 2019 velocity:",greenTaxiVelocity[0], "km/h")
    greenTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/green_tripdata_2020-05.csv","lpep_pickup_datetime","lpep_dropoff_datetime","trip_distance"))
    #print("green taxi 2020 velocity:",greenTaxiVelocity[1], "km/h")

    yellowTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/yellow_tripdata_2019-05.csv","tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance"))
    #print("yellow taxi 2019 velocity:",yellowTaxiVelocity[0], "km/h")
    yellowTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/yellow_tripdata_2020-05.csv","tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance"))
    #print("yellow taxi 2020 velocity:",yellowTaxiVelocity[1], "km/h")
    session.stop()
    

    x = np.arange(len(dateArray))
    width = 0.35

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, greenTaxiVelocity, width,label="green taxi",color = "g",)
    rects2 = ax.bar(x + width/2, yellowTaxiVelocity, width,label="yellow taxi", color="y")


    ax.set_ylabel("Velocity (km/h)")
    ax.set_xlabel("Date (year-month)")
    ax.set_title("Velocity of taxis according to May of 2019 and 2020")
    ax.set_xticks(x)
    ax.set_xticklabels(dateArray)
    ax.legend()

    ax.bar_label(rects1, padding=3)
    ax.bar_label(rects2, padding=3)

    fig.tight_layout()

    plt.show()
    
    plt.savefig('taxi_chart.png')
    subprocess.run("aws s3 cp taxi_chart.png s3://aws-project-2c-ww-dw-jk/", stdout=subprocess.PIPE, stderr = subprocess.PIPE, shell = True)