

```python
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("yarn") \
    .appName("chris-adhoc2") \
    .config('java.library.path', "file:<--hdppath-->/hadoop/lib/native") \
    .config('spark.driver.cores', "8") \
    .config('spark.driver.memory', "8g") \
    .config('spark.executor.cores', "8") \
    .config('spark.executor.memory', "16g") \
    .config('spark.executor.instances', "16") \
    .config('spark.serializer', "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.warehouse.dir", "<--warehouse-location-->") \
    .enableHiveSupport()\
    .getOrCreate()
spark.conf.set("spark.sql.orc.filterPushdown", "true")
```


```python
%matplotlib inline
```


```python
spark.read.parquet("<--data-location-->/sessionized").printSchema()
```

    root
     |-- clientId: string (nullable = true)
     |-- duration: integer (nullable = true)
     |-- accesses: integer (nullable = true)
     |-- uniqUrls: integer (nullable = true)
     |-- startTime: integer (nullable = true)
     |-- endTime: integer (nullable = true)
     |-- datehour: string (nullable = true)
    



```python
spark.read.parquet("<--output-location-->/sessionized").createOrReplaceTempView("sessionized")
```

# Session Distribution


```python
spark.sql("""
select count(*), datehour from sessionized
group by datehour
order by datehour
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count(1)</th>
      <th>datehour</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14871</td>
      <td>2015-07-22T03</td>
    </tr>
    <tr>
      <th>1</th>
      <td>24066</td>
      <td>2015-07-22T05</td>
    </tr>
    <tr>
      <th>2</th>
      <td>21228</td>
      <td>2015-07-22T07</td>
    </tr>
    <tr>
      <th>3</th>
      <td>32309</td>
      <td>2015-07-22T09</td>
    </tr>
    <tr>
      <th>4</th>
      <td>63934</td>
      <td>2015-07-22T10</td>
    </tr>
    <tr>
      <th>5</th>
      <td>62876</td>
      <td>2015-07-22T11</td>
    </tr>
    <tr>
      <th>6</th>
      <td>131</td>
      <td>2015-07-22T12</td>
    </tr>
    <tr>
      <th>7</th>
      <td>174</td>
      <td>2015-07-22T13</td>
    </tr>
    <tr>
      <th>8</th>
      <td>84029</td>
      <td>2015-07-22T16</td>
    </tr>
    <tr>
      <th>9</th>
      <td>40762</td>
      <td>2015-07-22T17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>76254</td>
      <td>2015-07-22T18</td>
    </tr>
    <tr>
      <th>11</th>
      <td>97</td>
      <td>2015-07-22T19</td>
    </tr>
    <tr>
      <th>12</th>
      <td>16825</td>
      <td>2015-07-22T21</td>
    </tr>
  </tbody>
</table>
</div>



# Average Session Time

## Per hour


```python
spark.sql("""
select cast(avg(duration) as int), datehour from sessionized
group by datehour
order by datehour
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CAST(avg(duration) AS INT)</th>
      <th>datehour</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>6</td>
      <td>2015-07-22T03</td>
    </tr>
    <tr>
      <th>1</th>
      <td>9</td>
      <td>2015-07-22T05</td>
    </tr>
    <tr>
      <th>2</th>
      <td>13</td>
      <td>2015-07-22T07</td>
    </tr>
    <tr>
      <th>3</th>
      <td>13</td>
      <td>2015-07-22T09</td>
    </tr>
    <tr>
      <th>4</th>
      <td>15</td>
      <td>2015-07-22T10</td>
    </tr>
    <tr>
      <th>5</th>
      <td>26</td>
      <td>2015-07-22T11</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1</td>
      <td>2015-07-22T12</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2</td>
      <td>2015-07-22T13</td>
    </tr>
    <tr>
      <th>8</th>
      <td>19</td>
      <td>2015-07-22T16</td>
    </tr>
    <tr>
      <th>9</th>
      <td>18</td>
      <td>2015-07-22T17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>18</td>
      <td>2015-07-22T18</td>
    </tr>
    <tr>
      <th>11</th>
      <td>0</td>
      <td>2015-07-22T19</td>
    </tr>
    <tr>
      <th>12</th>
      <td>4</td>
      <td>2015-07-22T21</td>
    </tr>
  </tbody>
</table>
</div>



## All time


```python
spark.sql("""
select cast(avg(duration) as int) from sessionized
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CAST(avg(duration) AS INT)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>17</td>
    </tr>
  </tbody>
</table>
</div>



# Unique URL visits per session

## Average per hour


```python
spark.sql("""
select cast(avg(uniqUrls) as int), datehour from sessionized
group by datehour
order by datehour
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CAST(avg(uniqUrls) AS INT)</th>
      <th>datehour</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2015-07-22T03</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2015-07-22T05</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>2015-07-22T07</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>2015-07-22T09</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
      <td>2015-07-22T10</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2</td>
      <td>2015-07-22T11</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1</td>
      <td>2015-07-22T12</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1</td>
      <td>2015-07-22T13</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2</td>
      <td>2015-07-22T16</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2</td>
      <td>2015-07-22T17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2</td>
      <td>2015-07-22T18</td>
    </tr>
    <tr>
      <th>11</th>
      <td>1</td>
      <td>2015-07-22T19</td>
    </tr>
    <tr>
      <th>12</th>
      <td>1</td>
      <td>2015-07-22T21</td>
    </tr>
  </tbody>
</table>
</div>



## Total per hour


```python
spark.sql("""
select cast(sum(uniqUrls) as int), datehour from sessionized
group by datehour
order by datehour
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CAST(sum(uniqUrls) AS INT)</th>
      <th>datehour</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>24696</td>
      <td>2015-07-22T03</td>
    </tr>
    <tr>
      <th>1</th>
      <td>57247</td>
      <td>2015-07-22T05</td>
    </tr>
    <tr>
      <th>2</th>
      <td>58223</td>
      <td>2015-07-22T07</td>
    </tr>
    <tr>
      <th>3</th>
      <td>89315</td>
      <td>2015-07-22T09</td>
    </tr>
    <tr>
      <th>4</th>
      <td>178321</td>
      <td>2015-07-22T10</td>
    </tr>
    <tr>
      <th>5</th>
      <td>179085</td>
      <td>2015-07-22T11</td>
    </tr>
    <tr>
      <th>6</th>
      <td>144</td>
      <td>2015-07-22T12</td>
    </tr>
    <tr>
      <th>7</th>
      <td>188</td>
      <td>2015-07-22T13</td>
    </tr>
    <tr>
      <th>8</th>
      <td>181874</td>
      <td>2015-07-22T16</td>
    </tr>
    <tr>
      <th>9</th>
      <td>90589</td>
      <td>2015-07-22T17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>160872</td>
      <td>2015-07-22T18</td>
    </tr>
    <tr>
      <th>11</th>
      <td>97</td>
      <td>2015-07-22T19</td>
    </tr>
    <tr>
      <th>12</th>
      <td>22935</td>
      <td>2015-07-22T21</td>
    </tr>
  </tbody>
</table>
</div>



## Average all time


```python
spark.sql("""
select cast(avg(uniqUrls) as int)from sessionized
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CAST(avg(uniqUrls) AS INT)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



## Total all time


```python
spark.sql("""
select cast(sum(uniqUrls) as int)from sessionized
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CAST(sum(uniqUrls) AS INT)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1043586</td>
    </tr>
  </tbody>
</table>
</div>



# User Leader board

## Ranking on total duration


```python
spark.sql("""
select sum(duration) as total_duration, clientId from sessionized
group by clientId
order by total_duration desc
limit 20
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_duration</th>
      <th>clientId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4726</td>
      <td>203.191.34.178:10400</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4112</td>
      <td>54.169.191.85:15462</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3026</td>
      <td>52.74.219.71:58226</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2948</td>
      <td>52.74.219.71:33576</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2743</td>
      <td>52.74.219.71:46492</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2358</td>
      <td>119.81.61.166:35995</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2334</td>
      <td>119.81.61.166:37795</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2325</td>
      <td>119.81.61.166:39102</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2323</td>
      <td>119.81.61.166:40602</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2291</td>
      <td>52.74.219.71:59692</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2264</td>
      <td>52.74.219.71:54439</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2225</td>
      <td>52.74.219.71:57623</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2089</td>
      <td>52.74.219.71:51841</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2086</td>
      <td>52.74.219.71:53157</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2065</td>
      <td>103.29.159.138:57045</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2065</td>
      <td>213.239.204.204:35094</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2064</td>
      <td>78.46.60.71:58504</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2064</td>
      <td>52.74.219.71:59516</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2064</td>
      <td>52.74.219.71:57649</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2060</td>
      <td>103.29.159.186:27174</td>
    </tr>
  </tbody>
</table>
</div>



## Ranking on total accesses


```python
spark.sql("""
select sum(accesses) as total_accesses, clientId from sessionized
group by clientId
order by total_accesses desc
limit 20
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_accesses</th>
      <th>clientId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1946</td>
      <td>112.196.25.164:55986</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1752</td>
      <td>112.196.25.164:42792</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1429</td>
      <td>112.196.25.164:37516</td>
    </tr>
    <tr>
      <th>3</th>
      <td>768</td>
      <td>54.169.191.85:15462</td>
    </tr>
    <tr>
      <th>4</th>
      <td>595</td>
      <td>106.51.132.54:5048</td>
    </tr>
    <tr>
      <th>5</th>
      <td>550</td>
      <td>54.169.191.85:3328</td>
    </tr>
    <tr>
      <th>6</th>
      <td>461</td>
      <td>106.51.132.54:4508</td>
    </tr>
    <tr>
      <th>7</th>
      <td>324</td>
      <td>106.51.132.54:5049</td>
    </tr>
    <tr>
      <th>8</th>
      <td>319</td>
      <td>106.51.132.54:4489</td>
    </tr>
    <tr>
      <th>9</th>
      <td>275</td>
      <td>14.102.53.58:4637</td>
    </tr>
    <tr>
      <th>10</th>
      <td>273</td>
      <td>106.51.132.54:5037</td>
    </tr>
    <tr>
      <th>11</th>
      <td>255</td>
      <td>106.51.132.54:4219</td>
    </tr>
    <tr>
      <th>12</th>
      <td>252</td>
      <td>106.51.132.54:4974</td>
    </tr>
    <tr>
      <th>13</th>
      <td>251</td>
      <td>106.51.132.54:4221</td>
    </tr>
    <tr>
      <th>14</th>
      <td>240</td>
      <td>118.102.239.85:44128</td>
    </tr>
    <tr>
      <th>15</th>
      <td>240</td>
      <td>106.51.132.54:4212</td>
    </tr>
    <tr>
      <th>16</th>
      <td>239</td>
      <td>88.198.69.103:47828</td>
    </tr>
    <tr>
      <th>17</th>
      <td>237</td>
      <td>78.46.60.71:58504</td>
    </tr>
    <tr>
      <th>18</th>
      <td>235</td>
      <td>106.51.132.54:4235</td>
    </tr>
    <tr>
      <th>19</th>
      <td>234</td>
      <td>213.239.204.204:35094</td>
    </tr>
  </tbody>
</table>
</div>



## Ranking on total unique urls


```python
spark.sql("""
select sum(uniqUrls) as total_uniqUrls, clientId from sessionized
group by clientId
order by total_uniqUrls desc
limit 20
""").toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_uniqUrls</th>
      <th>clientId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>407</td>
      <td>106.51.132.54:5048</td>
    </tr>
    <tr>
      <th>1</th>
      <td>320</td>
      <td>106.51.132.54:5049</td>
    </tr>
    <tr>
      <th>2</th>
      <td>290</td>
      <td>106.51.132.54:4508</td>
    </tr>
    <tr>
      <th>3</th>
      <td>271</td>
      <td>106.51.132.54:5037</td>
    </tr>
    <tr>
      <th>4</th>
      <td>251</td>
      <td>106.51.132.54:4974</td>
    </tr>
    <tr>
      <th>5</th>
      <td>239</td>
      <td>88.198.69.103:47828</td>
    </tr>
    <tr>
      <th>6</th>
      <td>237</td>
      <td>78.46.60.71:58504</td>
    </tr>
    <tr>
      <th>7</th>
      <td>234</td>
      <td>213.239.204.204:35094</td>
    </tr>
    <tr>
      <th>8</th>
      <td>226</td>
      <td>106.51.132.54:4841</td>
    </tr>
    <tr>
      <th>9</th>
      <td>186</td>
      <td>106.51.132.54:5035</td>
    </tr>
    <tr>
      <th>10</th>
      <td>178</td>
      <td>188.40.94.195:46918</td>
    </tr>
    <tr>
      <th>11</th>
      <td>164</td>
      <td>144.76.99.19:58987</td>
    </tr>
    <tr>
      <th>12</th>
      <td>159</td>
      <td>78.46.60.71:46308</td>
    </tr>
    <tr>
      <th>13</th>
      <td>154</td>
      <td>188.40.135.194:43628</td>
    </tr>
    <tr>
      <th>14</th>
      <td>153</td>
      <td>188.40.135.194:59090</td>
    </tr>
    <tr>
      <th>15</th>
      <td>150</td>
      <td>176.9.154.132:50326</td>
    </tr>
    <tr>
      <th>16</th>
      <td>145</td>
      <td>106.51.132.54:4489</td>
    </tr>
    <tr>
      <th>17</th>
      <td>127</td>
      <td>66.249.71.110:41229</td>
    </tr>
    <tr>
      <th>18</th>
      <td>110</td>
      <td>141.8.143.205:42162</td>
    </tr>
    <tr>
      <th>19</th>
      <td>107</td>
      <td>141.8.143.205:57227</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
