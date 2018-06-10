
Read citibike data and write to SQL database from python


```python
# define function to read in a citibike file (already downloaded and unzipped), do some 
# cleaning, and add some fields
import pandas as pd
#dat = pd.read_csv('data/2013-09 - Citi Bike trip data.csv',parse_dates=True)
def load_citibike_monthly(fname):
    dat = pd.read_csv(fname,parse_dates=True)
    dat.starttime=pd.to_datetime(dat.starttime)
    dat.stoptime=pd.to_datetime(dat.stoptime)
    dat['gender']=dat['gender'].astype('category')
    dat.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    dat['day']=dat.starttime.dt.day
    dat['month']=dat.starttime.dt.month
    dat['year']=dat.starttime.dt.year
    dat['yday']=dat.starttime.dt.dayofyear
    dat['wkday']=dat.starttime.dt.dayofweek
    return dat

```


```python
# test it out
fname='data/2013-09 - Citi Bike trip data.csv'
dat = load_citibike_monthly(fname)
dat.head()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>tripduration</th>
      <th>starttime</th>
      <th>stoptime</th>
      <th>start_station_id</th>
      <th>start_station_name</th>
      <th>start_station_latitude</th>
      <th>start_station_longitude</th>
      <th>end_station_id</th>
      <th>end_station_name</th>
      <th>end_station_latitude</th>
      <th>end_station_longitude</th>
      <th>bikeid</th>
      <th>usertype</th>
      <th>birth_year</th>
      <th>gender</th>
      <th>day</th>
      <th>month</th>
      <th>year</th>
      <th>yday</th>
      <th>wkday</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1010</td>
      <td>2013-09-01 00:00:02</td>
      <td>2013-09-01 00:16:52</td>
      <td>254</td>
      <td>W 11 St &amp; 6 Ave</td>
      <td>40.735324</td>
      <td>-73.998004</td>
      <td>147</td>
      <td>Greenwich St &amp; Warren St</td>
      <td>40.715422</td>
      <td>-74.011220</td>
      <td>15014</td>
      <td>Subscriber</td>
      <td>1974</td>
      <td>1</td>
      <td>1</td>
      <td>9</td>
      <td>2013</td>
      <td>244</td>
      <td>6</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1443</td>
      <td>2013-09-01 00:00:09</td>
      <td>2013-09-01 00:24:12</td>
      <td>151</td>
      <td>Cleveland Pl &amp; Spring St</td>
      <td>40.721816</td>
      <td>-73.997203</td>
      <td>497</td>
      <td>E 17 St &amp; Broadway</td>
      <td>40.737050</td>
      <td>-73.990093</td>
      <td>19393</td>
      <td>Customer</td>
      <td>\N</td>
      <td>0</td>
      <td>1</td>
      <td>9</td>
      <td>2013</td>
      <td>244</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1387</td>
      <td>2013-09-01 00:00:16</td>
      <td>2013-09-01 00:23:23</td>
      <td>352</td>
      <td>W 56 St &amp; 6 Ave</td>
      <td>40.763406</td>
      <td>-73.977225</td>
      <td>405</td>
      <td>Washington St &amp; Gansevoort St</td>
      <td>40.739323</td>
      <td>-74.008119</td>
      <td>16160</td>
      <td>Subscriber</td>
      <td>1992</td>
      <td>1</td>
      <td>1</td>
      <td>9</td>
      <td>2013</td>
      <td>244</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>405</td>
      <td>2013-09-01 00:00:18</td>
      <td>2013-09-01 00:07:03</td>
      <td>490</td>
      <td>8 Ave &amp; W 33 St</td>
      <td>40.751551</td>
      <td>-73.993934</td>
      <td>459</td>
      <td>W 20 St &amp; 11 Ave</td>
      <td>40.746745</td>
      <td>-74.007756</td>
      <td>14997</td>
      <td>Subscriber</td>
      <td>1973</td>
      <td>1</td>
      <td>1</td>
      <td>9</td>
      <td>2013</td>
      <td>244</td>
      <td>6</td>
    </tr>
    <tr>
      <th>4</th>
      <td>270</td>
      <td>2013-09-01 00:00:20</td>
      <td>2013-09-01 00:04:50</td>
      <td>236</td>
      <td>St Marks Pl &amp; 2 Ave</td>
      <td>40.728419</td>
      <td>-73.987140</td>
      <td>393</td>
      <td>E 5 St &amp; Avenue C</td>
      <td>40.722992</td>
      <td>-73.979955</td>
      <td>19609</td>
      <td>Subscriber</td>
      <td>1984</td>
      <td>1</td>
      <td>1</td>
      <td>9</td>
      <td>2013</td>
      <td>244</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>




```python

```


```python

```


```python

```


```python

```


```python

```

Now write a script to read all monthly files and put in database


```python
# Get list of the citibke csv files we have
import glob
flist = glob.glob('data/*data.csv')
flist
```




    ['data/2013-07 - Citi Bike trip data.csv',
     'data/2013-08 - Citi Bike trip data.csv',
     'data/2013-09 - Citi Bike trip data.csv',
     'data/2013-10 - Citi Bike trip data.csv',
     'data/2013-11 - Citi Bike trip data.csv',
     'data/2013-12 - Citi Bike trip data.csv',
     'data/2014-01 - Citi Bike trip data.csv',
     'data/2014-02 - Citi Bike trip data.csv',
     'data/2014-03 - Citi Bike trip data.csv',
     'data/2014-04 - Citi Bike trip data.csv',
     'data/2014-05 - Citi Bike trip data.csv',
     'data/2014-06 - Citi Bike trip data.csv',
     'data/2014-07 - Citi Bike trip data.csv',
     'data/2014-08 - Citi Bike trip data.csv',
     'data/201409-citibike-tripdata.csv',
     'data/201410-citibike-tripdata.csv',
     'data/201411-citibike-tripdata.csv',
     'data/201412-citibike-tripdata.csv']




```python
# Then loop  and load each one
# if this is too much (some files are pretty large), could also read each file in in chunks

import sqlite3
con = sqlite3.connect("/Users/Andy/Desktop/test2.db3")
for fname in flist:
    print('loading ' + fname)
    dat = load_citibike_monthly(fname)
    print('adding ' + fname + ' to sql database')
    dat.to_sql("rides",con,if_exists='append',chunksize=10000)
    del dat
print('done')
 
```

    loading data/2013-07 - Citi Bike trip data.csv
    adding data/2013-07 - Citi Bike trip data.csv to sql database
    loading data/2013-08 - Citi Bike trip data.csv
    adding data/2013-08 - Citi Bike trip data.csv to sql database
    loading data/2013-09 - Citi Bike trip data.csv
    adding data/2013-09 - Citi Bike trip data.csv to sql database
    loading data/2013-10 - Citi Bike trip data.csv
    adding data/2013-10 - Citi Bike trip data.csv to sql database
    loading data/2013-11 - Citi Bike trip data.csv
    adding data/2013-11 - Citi Bike trip data.csv to sql database
    loading data/2013-12 - Citi Bike trip data.csv
    adding data/2013-12 - Citi Bike trip data.csv to sql database
    loading data/2014-01 - Citi Bike trip data.csv
    adding data/2014-01 - Citi Bike trip data.csv to sql database
    loading data/2014-02 - Citi Bike trip data.csv
    adding data/2014-02 - Citi Bike trip data.csv to sql database
    loading data/2014-03 - Citi Bike trip data.csv
    adding data/2014-03 - Citi Bike trip data.csv to sql database
    loading data/2014-04 - Citi Bike trip data.csv
    adding data/2014-04 - Citi Bike trip data.csv to sql database
    loading data/2014-05 - Citi Bike trip data.csv
    adding data/2014-05 - Citi Bike trip data.csv to sql database
    loading data/2014-06 - Citi Bike trip data.csv
    adding data/2014-06 - Citi Bike trip data.csv to sql database
    loading data/2014-07 - Citi Bike trip data.csv
    adding data/2014-07 - Citi Bike trip data.csv to sql database
    loading data/2014-08 - Citi Bike trip data.csv
    adding data/2014-08 - Citi Bike trip data.csv to sql database
    loading data/201409-citibike-tripdata.csv
    adding data/201409-citibike-tripdata.csv to sql database
    loading data/201410-citibike-tripdata.csv
    adding data/201410-citibike-tripdata.csv to sql database
    loading data/201411-citibike-tripdata.csv
    adding data/201411-citibike-tripdata.csv to sql database
    loading data/201412-citibike-tripdata.csv
    adding data/201412-citibike-tripdata.csv to sql database
    done



```python
# count the total number of rows: there are over 13 million rows, no way we could load that all in Pandas
pd.read_sql_query("SELECT COUNT(*) FROM rides", con)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>COUNT(*)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>13118401</td>
    </tr>
  </tbody>
</table>
</div>



First i'll examine the number of rides per month, for 2014. The plot below shows that there is a strong seasonal cycle, with more rides in the summer months. My hypothesis is that this is mainly driven by the temperature; I will get weather data later and test this.


```python
# count rides per month
df = pd.read_sql_query("SELECT year,month,count(*) as num_rides FROM rides WHERE year=2014 GROUP BY month", con)
df
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>month</th>
      <th>num_rides</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2014</td>
      <td>1</td>
      <td>300400</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2014</td>
      <td>2</td>
      <td>224736</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2014</td>
      <td>3</td>
      <td>439117</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2014</td>
      <td>4</td>
      <td>670780</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2014</td>
      <td>5</td>
      <td>866117</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2014</td>
      <td>6</td>
      <td>936880</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2014</td>
      <td>7</td>
      <td>968842</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2014</td>
      <td>8</td>
      <td>963489</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2014</td>
      <td>9</td>
      <td>953887</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2014</td>
      <td>10</td>
      <td>828711</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2014</td>
      <td>11</td>
      <td>529188</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2014</td>
      <td>12</td>
      <td>399069</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Plot rides vs month
# There is a very strong seasonal cycle
import matplotlib.pyplot as plt
%matplotlib inline
plt.plot(df.month,df.num_rides)
plt.scatter(df.month,df.num_rides)
plt.grid()
plt.xlabel('month')
plt.ylabel('# rides')
```




    <matplotlib.text.Text at 0x11e7ffeb8>




![png](database_python_files/database_python_14_1.png)


Next i'll look at the number of rides each day in 2014. This shows that there is a strong seasonal pattern, but also some big residuals from that pattern. These could be days that were unseasonably cold/warm, or due to other factors. 


```python
# plot rides/day for 2014
df = pd.read_sql_query("select yday, count(*) as num_rides from rides where year=2014 group by yday",con)
```


```python
plt.plot(df.yday,df.num_rides)
plt.grid()
plt.xlabel('day of year')
plt.ylabel('# rides')
```




    <matplotlib.text.Text at 0x120348b00>




![png](database_python_files/database_python_17_1.png)



```python

```

Which stations were the most used?


```python
df = pd.read_sql_query("select start_station_id, start_station_name, count(*) as num_rides from rides where year=2014 group by start_station_id order by num_rides desc",con)
```


```python
df.head(10)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>start_station_id</th>
      <th>start_station_name</th>
      <th>num_rides</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>521</td>
      <td>8 Ave &amp; W 31 St</td>
      <td>100498</td>
    </tr>
    <tr>
      <th>1</th>
      <td>519</td>
      <td>Pershing Square North</td>
      <td>92137</td>
    </tr>
    <tr>
      <th>2</th>
      <td>293</td>
      <td>Lafayette St &amp; E 8 St</td>
      <td>86692</td>
    </tr>
    <tr>
      <th>3</th>
      <td>497</td>
      <td>E 17 St &amp; Broadway</td>
      <td>80166</td>
    </tr>
    <tr>
      <th>4</th>
      <td>435</td>
      <td>W 21 St &amp; 6 Ave</td>
      <td>73448</td>
    </tr>
    <tr>
      <th>5</th>
      <td>285</td>
      <td>Broadway &amp; E 14 St</td>
      <td>65852</td>
    </tr>
    <tr>
      <th>6</th>
      <td>426</td>
      <td>West St &amp; Chambers St</td>
      <td>63221</td>
    </tr>
    <tr>
      <th>7</th>
      <td>402</td>
      <td>Broadway &amp; E 22 St</td>
      <td>60932</td>
    </tr>
    <tr>
      <th>8</th>
      <td>151</td>
      <td>Cleveland Pl &amp; Spring St</td>
      <td>60092</td>
    </tr>
    <tr>
      <th>9</th>
      <td>382</td>
      <td>University Pl &amp; E 14 St</td>
      <td>60072</td>
    </tr>
  </tbody>
</table>
</div>




```python
plt.boxplot(df.num_rides)
```




    {'boxes': [<matplotlib.lines.Line2D at 0x1223a4588>],
     'caps': [<matplotlib.lines.Line2D at 0x1223ba518>,
      <matplotlib.lines.Line2D at 0x1223cf208>],
     'fliers': [<matplotlib.lines.Line2D at 0x120348cf8>],
     'means': [],
     'medians': [<matplotlib.lines.Line2D at 0x11f8f9c18>],
     'whiskers': [<matplotlib.lines.Line2D at 0x1223a44a8>,
      <matplotlib.lines.Line2D at 0x12142ec18>]}




![png](database_python_files/database_python_22_1.png)



```python
# There are a small number of stations with many more rides; where are they? Plot on map?

```


```python

```


```python

```


```python

```

To-do:
- fit seasonal cycle and remove to examine anomalies


```python

```


```python

```


```python

```


```python
# How many rides were taken by men vs women?
df = pd.read_sql_query("SELECT gender,count(*) as num_rides FROM rides GROUP BY gender ORDER BY num_rides DESC", con)
df.head()
```


```python

```


```python
# write a separate function to read the data and write to sql in chunks, see if it is faster. 
```


```python

```


```python

```
