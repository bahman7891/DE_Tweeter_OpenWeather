# DE_Tweeter_OpenWeather
A first project to deliver results on the statistics and data driven modeling of tweet text correlated with the weather conditions. Please refer to jpeg files in this repo for a brief introduction to the project.



Designing Big Data Systems
-----------------

Project: Tweet Sentiment vs. Weather Condition 
--------------------------
### Data: 
> Collected from two streaming sources:
> Tweeter API 
> Openweather API



### Problem:

> Given the worlwide stream of social nework data, here specifically tweets what information can be extracted from such massive amount of data? One type of important information obtained from such data is related to the social aspect of them, specifically the human psychology. In this project we aim at recognizing what effects the weather conditions may have on human psyche in different locations. For this purpose weather data will be also obtained from streaming output (openweather service). Using natural language processing and data analysis tools, for the proof of concept and as an initial approach to solving such problem we combine the social network data and weather data to gain insight on how the weather conditions affect the human sentiment.


### Robustness and fault tolerance:

> This app is using distributed computation system Spark. Spark with its inherent RDD structure is robust against the fault in computing nodes. We used Spark DataFrame and Spark SQL wraps of the RDD structure for feature extraction and later, using map-reduce algorithms we exctract information.


### Low latency reads and updates:

> This project is not heavily sensitive to the latency. The best results in fact will be achieved using historical data in the form of batches. The results will be updated every few days or weeks. However it is possible to use other modes of our app to achieve faster feature extraction. This of course needs more resources namely scaling which we explain below.


### Scalability:

> Spark, the main computing distrbuted system being used in our project has the ability to scale up or down based on the load demand. In case a faster computation and less latent results to be reached the distribution of the app in spark cluster can be enhanced by increasing the number of nodes without changing the core computation code of the app.

### Generalization:

> With few steps it may be possible to generalize the functionality of the core computing code of this system to other applications such as machine learning for weather forecast or tweet sentiment prediction based on weather condition.

### Extensibility:

> Our app is easily extensible to more weather or tweet feature extraction and analysis. It is also possible to easily implement other releational models in the core code for example : time of the tweet and time at which a weather condition happened. 


### Ad hoc queries

> The data is stored in AWS S3 storage and can be queried using spark sql query. Also it can be read into RDD structures.


### Minimal maintenance

> The main part of our project which requires maintenance is where the data are being received from the source API's. The core computing code is able to maintain throughout computation regardless of the stream problems.


### Debuggability

> there are three major part in our app which may cause errors or show bugs. First is the data streaming. AWS Kinesis firehose and S3 provide tools to recognize bugs in the stream of data through log files. Second the core comupting code which is written in python standard coding scheme and although not yet having the best score but it is at this point clear in algorithm. Proper try and excepts have been added. In this particular work the use of dictionaries has been minimized in order to minimize Keyerrors which is a major problem with tweeter data. Finally the storage of the results and the output are utilizing again AWS S3 system (with boto module) which can be traced using the AWS error log structure to a practical level.

