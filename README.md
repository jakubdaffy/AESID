# Average Velocity
Project using AWS to calculate average velocity of New York's taxis and visualising them on bar diagram.

#Used Technologies

*Python
*Spark
*Aws

# Data Structure
Data set contains the following informations:

* pick-up and drop-off dates/times
* pick-up and drop-off locations
* trip distances
* itemized fares
* rate types
* payment types
* and driver-reported passenger counts

# Main Program
Creating Spark Session  
```python
session = SparkSession.builder.appName("Average Velocity").getOrCreate()
``` 
Creating Data Frame Reader
```python
dataFrameReader = session.read
```
Creating Arrays for velocities of green and yellow taxis where element [0] is for "2019-05" year and [1] for "2020-05".
```python
greenTaxiVelocity = []
yellowTaxiVelocity = []
dateArray = ["2019-05", "2020-05"]
```

Definning function returning average velocity for given data set. Fuction needs Data Frame Reader, s3n source for .csv file, names of columns for pickup, dropoff and distance.  
To calculate average velocity, program uses Data Frame method to create column "Velocity" that contains distance divided by time period between pickup date and dropoff date.  
Outcome is given in [km/s] so it is needed to multiply it by 3600, so outcome will be given in [km/h]. Column "Velocity" is added to table containing data from .csv file.  
Next operation is selecting only calculated mean of all rows off "Velocity" column. That way, outcome table has only one column and row having average velocity.  
To return average velocity as value, it is needed to add ".head()[0]" as refer to data of that table. Program puts output to concrete arrays.  
```python
def avgCarVelocity(dataFrameReader, src, pickup, dropoff, dist):
    return dataFrameReader.option("header","true")\
                    .option("inferSchema", value = True)\
                    .csv(src)\
                    .withColumn("Velocity",col(dist)/((unix_timestamp(dropoff)\
                    -unix_timestamp(pickup))/3600))\
                    .select(avg("Velocity")).head()[0]
```

Pushing outputs to arrays with taxis velocities.  
```python
greenTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/green_tripdata_2019-05.csv","lpep_pickup_datetime","lpep_dropoff_datetime","trip_distance"))
greenTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/green_tripdata_2020-05.csv","lpep_pickup_datetime","lpep_dropoff_datetime","trip_distance"))
yellowTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/yellow_tripdata_2019-05.csv","tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance"))
yellowTaxiVelocity.append( avgCarVelocity(dataFrameReader ,"s3n://aws-project-2c-ww-dw-jk/yellow_tripdata_2020-05.csv","tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance"))
```

Stopping Session  
```python
session.stop()
```

Displaying outputs on bar diagram and saving image   
```python
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
```

Upload image with bar diagram on s3 bucket  
```python
subprocess.run("aws s3 cp taxi_chart.png s3://aws-project-2c-ww-dw-jk/", stdout=subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
```

# Bar Diagram Output for Project

![diagram](https://github.com/WozniakDominik/AESID/blob/master/taxi_chart.png)

#File organization
In AWS console, in s3 storage, create folder for files (so called "bucket") and updload .csv files and program to this folder.

#Creating Cluster
In AWS console, go to EMR and click "Create cluster" button. Follow the instructions. It is important to select "Spark" in Applications in Software configuration. Important
is also creating SSH Key.

#Connect with Cluster
To connect with EMR cluster open PuTTY. In Category window go to Connection/SSH/Auth and browse for generated .ppk key file
in Category window go to Session and fill Host Name (or IP address) inputbox with address that can be found in chosen cluster's Summary in tab "Connect to the Master Node Using SSH"  
Next Click "Open". On PuTTY Security Alert popup window choose "Accept".

#Running Program
After connecting with EMR cluster by PuTTY, input following command to copy .py program from bucket to cluster.  
```python
aws s3 cp s3://aws-project-2c-ww-dw-jk/aws_project_2c_ww_dw_jk.py .
```

To launch program in cluster, input following command.  
```console
spark-submit aws_project_2c_ww_dw_jk.py
```

#Conclusion
Using Pyspark provides user with fast working and manipulating very large data sets with small time amount.
