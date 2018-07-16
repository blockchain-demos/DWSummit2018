
### Pyspark kernel
allows you to now have to run %%spark in every cell.
Once a livy session is started, all cells communicate to that livy



```python
%load_ext sparkmagic.magics
```

#### Py virtual env


```python
%%spark config 
{
  "name":"Parquet_dump",
  "driverMemory":"2G",
  "numExecutors":1,
  "proxyUser":"noobie",
  "archives": ["/user/noobie/gethdemo.tar.gz"],
  "files": ["/user/noobie/etherdelta_abi.json"],
  "queue": "noobies",
  "conf": {"spark.yarn.appMasterEnv.PYSPARK_PYTHON":"gethdemo.tar.gz/demo/bin/python3.5",
          "spark.jars.packages":"org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0"}
}
```


```python
%spark add -s ethlogsdw -l python -u http://hdp-3.evilcorp.trade:8999 --auth Kerberos
```

    Skip
    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>313</td><td>application_1529358819933_0003</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://hdp-3.evilcorp.trade:8088/proxy/application_1529358819933_0003/">Link</a></td><td><a target="_blank" href="http://hdp-5.evilcorp.trade:8042/node/containerlogs/container_e60_1529358819933_0003_01_000002/noobie">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.


## Start consumer



```python
%%spark
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(bootstrap_servers=['hdp-3.evilcorp.trade:6667',
                                           'hdp-5.evilcorp.trade:6667',
                                           'hdp-6.evilcorp.trade:6667'],
                        security_protocol="SASL_PLAINTEXT",
                        sasl_mechanism="GSSAPI",
                        auto_offset_reset='earliest',
                        client_id='SparlSqlEthLogs',
                        request_timeout_ms=501,
                        consumer_timeout_ms=500,
                        max_poll_interval_ms=500)
#,session_timeout_ms=500
ETH_KAFKA_TOPIC='selfpump_trades_sat'
ETH_KAFKA_TOPIC='selfpump_trades_stream'
```


```python
%%spark
consumer.subscribe([ETH_KAFKA_TOPIC])
```

Sample data in consumermsg
```
{'transactionIndex': 68, 
           'logIndex': 63, 
           'blockHash': '0x9aa8982af8df5b99e967bc59694a64bb27bdc6f689cad01096b7e894e5e01a0a', 
           'event': 'Trade', 
           'args': {'get': '0x01DD9cD663A17f72115304F75014D169Ae5E6494', 
                    'tokenGet': '0x9214eC02CB71CbA0ADA6896b8dA260736a67ab10', 
                    'tokenGive': '0x0000000000000000000000000000000000000000', 
                    'amountGive': 8512000000000000, 'amountGet': 16000000000000000000, 
                    'give': '0x01DD9cD663A17f72115304F75014D169Ae5E6494'}, 
           'transactionHash': '0x651c4b6ad804415bb4155ea115d9927a011d9b2edc3d3a2eb21738ccfa3eeb50', 
           'address': '0x8d12A197cB00D4747a1fe03395095ce2A5CC6819', 
           'blockNumber': 5684183}
```


```python
%%spark
import json
from pyspark.sql import HiveContext
from pyspark.sql.types import DecimalType

hc = HiveContext(sc)

ed_trades_dict = [ ]
for message in consumer:
    consumermsg=json.loads(message.value.decode('utf-8'))   
    ed_trades_dict.append(consumermsg)
    
nested_df = hc.read.json(sc.parallelize(ed_trades_dict))
pumps_df = nested_df.select(nested_df.blockNumber, 
                           nested_df.event,
                           nested_df.args.amountGet.cast(DecimalType(38,2)).alias("amountGet"),
                           nested_df.args.amountGive.cast(DecimalType(38,2)).alias("amountGive"),
                           nested_df.args.get.alias("buyer"),
                           nested_df.args.give.alias("seller"),
                           nested_df.args.tokenGive.alias("tokenGive"),
                           nested_df.args.tokenGet.alias("tokenGet"),
                           nested_df.transactionHash)
                           #"blockNumber", "event", "args.*", "address", "transactionHash")
#pumps_df.collect()

```


```python
%%spark
pumps_df.printSchema()
```


```python
%%spark
pumps_df.createOrReplaceTempView("SelfPumpToday")
#pumps_df.write.csv("/user/noobie/visualization_data.csv")

#pumps_df.write.parquet("/user/noobie/pumps.parquet")
```


```python
%%spark -c sql
select count(*) from SelfPumpToday
```


```python
# fix sparkmagic output https://github.com/jupyter-incubator/sparkmagic/issues/319
import pandas as pd
pd.set_option('display.max_colwidth', 100)
```


```python
%%spark -c sql 
select seller, buyer, amountGive, transactionHash from SelfPumpToday where tokenGive LIKE '0x000000000000000%' \
order by amountGive desc
```

                                            buyer  \
    0  0xAFB7340211f56618789858A73D34a67e837624C4   
    1  0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A   
    2  0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A   
    
                                           seller  \
    0  0xAFB7340211f56618789858A73D34a67e837624C4   
    1  0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A   
    2  0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A   
    
                                                          transactionHash  \
    0  0x065499b3d3467e82b745b0403c4c5eba9e8222d1acbbb967ab34dbf00fe42bef   
    1  0x4d0548d15e7f4518810e258545d3135f02662c4bb23b0da9d32e5824c95313ce   
    2  0x171ff7ec5f05e4d7c0d8a8f7b94fa13b65a6a330ffe6b7689d52fd1f4c25c1d4   
    
         amountGive  
    0  7.350000e+16  
    1  4.727799e+16  
    2  3.011100e+16  





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
      <th>buyer</th>
      <th>seller</th>
      <th>transactionHash</th>
      <th>amountGive</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x065499b3d3467e82b745b0403c4c5eba9e8222d1acbbb967ab34dbf00fe42bef</td>
      <td>7.350000e+16</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A</td>
      <td>0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A</td>
      <td>0x4d0548d15e7f4518810e258545d3135f02662c4bb23b0da9d32e5824c95313ce</td>
      <td>4.727799e+16</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A</td>
      <td>0x1a0A311527414A94c971104DBC8dd84b0ecc1d4A</td>
      <td>0x171ff7ec5f05e4d7c0d8a8f7b94fa13b65a6a330ffe6b7689d52fd1f4c25c1d4</td>
      <td>3.011100e+16</td>
    </tr>
  </tbody>
</table>
</div>




```python
%%spark -c sql -o marketmanipulated
select buyer, seller, amountGet, transactionHash from SelfPumpToday where tokenGet LIKE '0x000000000000000%' \
order by amountGet desc
```

                                             buyer  \
    0   0xAFB7340211f56618789858A73D34a67e837624C4   
    1   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    2   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    3   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    4   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    5   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    6   0xAFB7340211f56618789858A73D34a67e837624C4   
    7   0xAFB7340211f56618789858A73D34a67e837624C4   
    8   0xAFB7340211f56618789858A73D34a67e837624C4   
    9   0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    10  0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    11  0xAFB7340211f56618789858A73D34a67e837624C4   
    12  0xf1C7cA754F5Cd2DF308d19632763d292cF406060   
    13  0xf1C7cA754F5Cd2DF308d19632763d292cF406060   
    14  0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    15  0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    16  0xAFB7340211f56618789858A73D34a67e837624C4   
    17  0xAFB7340211f56618789858A73D34a67e837624C4   
    18  0xAFB7340211f56618789858A73D34a67e837624C4   
    19  0xAFB7340211f56618789858A73D34a67e837624C4   
    20  0x4D25819e9De2A2EEEdDca953D1eF0182680a7054   
    21  0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6   
    22  0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9   
    23  0xf1C7cA754F5Cd2DF308d19632763d292cF406060   
    24  0x736a1120d12f1D19E0cEF35426C7ec7B90514be4   
    25  0x736a1120d12f1D19E0cEF35426C7ec7B90514be4   
    26  0xAFB7340211f56618789858A73D34a67e837624C4   
    27  0xe84a294E045535e39CFFc81D2Dc74b2839035388   
    28  0xe84a294E045535e39CFFc81D2Dc74b2839035388   
    29  0xAFB7340211f56618789858A73D34a67e837624C4   
    30  0x6ec8247cdc03F336118B96f4e862b24B1F947fa2   
    31  0xF23db283c6b43feCe3b1215cF5507B6d78fc2215   
    32  0x6ec8247cdc03F336118B96f4e862b24B1F947fa2   
    33  0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF   
    34  0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c   
    35  0x8Dd44F5ef0d1765a428616E943351d009D7B1088   
    36  0x8Dd44F5ef0d1765a428616E943351d009D7B1088   
    37  0xbc1323B62Bdd371C38A5f7904c705d833A836d2E   
    
                                            seller  \
    0   0xAFB7340211f56618789858A73D34a67e837624C4   
    1   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    2   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    3   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    4   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    5   0x09e2D651cE800B048611B5DFca8613452F670F7d   
    6   0xAFB7340211f56618789858A73D34a67e837624C4   
    7   0xAFB7340211f56618789858A73D34a67e837624C4   
    8   0xAFB7340211f56618789858A73D34a67e837624C4   
    9   0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    10  0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    11  0xAFB7340211f56618789858A73D34a67e837624C4   
    12  0xf1C7cA754F5Cd2DF308d19632763d292cF406060   
    13  0xf1C7cA754F5Cd2DF308d19632763d292cF406060   
    14  0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    15  0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0   
    16  0xAFB7340211f56618789858A73D34a67e837624C4   
    17  0xAFB7340211f56618789858A73D34a67e837624C4   
    18  0xAFB7340211f56618789858A73D34a67e837624C4   
    19  0xAFB7340211f56618789858A73D34a67e837624C4   
    20  0x4D25819e9De2A2EEEdDca953D1eF0182680a7054   
    21  0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6   
    22  0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9   
    23  0xf1C7cA754F5Cd2DF308d19632763d292cF406060   
    24  0x736a1120d12f1D19E0cEF35426C7ec7B90514be4   
    25  0x736a1120d12f1D19E0cEF35426C7ec7B90514be4   
    26  0xAFB7340211f56618789858A73D34a67e837624C4   
    27  0xe84a294E045535e39CFFc81D2Dc74b2839035388   
    28  0xe84a294E045535e39CFFc81D2Dc74b2839035388   
    29  0xAFB7340211f56618789858A73D34a67e837624C4   
    30  0x6ec8247cdc03F336118B96f4e862b24B1F947fa2   
    31  0xF23db283c6b43feCe3b1215cF5507B6d78fc2215   
    32  0x6ec8247cdc03F336118B96f4e862b24B1F947fa2   
    33  0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF   
    34  0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c   
    35  0x8Dd44F5ef0d1765a428616E943351d009D7B1088   
    36  0x8Dd44F5ef0d1765a428616E943351d009D7B1088   
    37  0xbc1323B62Bdd371C38A5f7904c705d833A836d2E   
    
                                                           transactionHash  \
    0   0xfdbf2ac5aea9ce211d5ea9a4cb3bf614f9fafa9f798fd17d53246cc23c223ee6   
    1   0xe103223d7677973e2ef563b9ce02eddd19edbecbcdf653f6b6945556c4a3685a   
    2   0xe103223d7677973e2ef563b9ce02eddd19edbecbcdf653f6b6945556c4a3685a   
    3   0xb70817e327930e8be0a97a2b78024c7e1aaa38112622a7442bf8c0aa3d7614c2   
    4   0xb70817e327930e8be0a97a2b78024c7e1aaa38112622a7442bf8c0aa3d7614c2   
    5   0xb4f8b7f17a107b222bccf53c2765b87dfdf050373b9da77c842ffde17ed17291   
    6   0x860fa1785201135caf8e5740f7463e4233cecb2297bd4565fc0f7030b0a63eb2   
    7   0x860fa1785201135caf8e5740f7463e4233cecb2297bd4565fc0f7030b0a63eb2   
    8   0x8139ceb1e4955e3674bdda2639eab0a250d02a8f9015981cd24db3f0426af98e   
    9   0x04c7418ec98ad67beb7cba9c5964c4bedef745d87c752d21965fa0a46b7492b7   
    10  0x04c7418ec98ad67beb7cba9c5964c4bedef745d87c752d21965fa0a46b7492b7   
    11  0x9deca97007af078d5032cb41b7a189831f7fde2b9b794559cece8501139fd159   
    12  0xbb2ad92d6a3ced02ad12f12af2b95382877a9e9f8e62f4421b246a30dd75d2e4   
    13  0x9e0cca325509ad15368c43f6ed72cce5132c454254bdbbe10128f125e10f0ca4   
    14  0x8b5b4f7af47fb8513922987286994f2bf9fbb5af0db3a718e7de55e785a33c12   
    15  0x8b5b4f7af47fb8513922987286994f2bf9fbb5af0db3a718e7de55e785a33c12   
    16  0x581848c7f42ceae4f8bbc4b04af806cff7f3b29f8b404fe4363bcd47f7d91642   
    17  0x871782ab1e4655397a0189ea583f3d4f1f24c3429b6329e59bccca3e1e8e8e66   
    18  0xeac7c904f47fdd16ac468f03eaa6a7ce51636ec2417ec572dd1fb2b950e2b3f8   
    19  0xeac7c904f47fdd16ac468f03eaa6a7ce51636ec2417ec572dd1fb2b950e2b3f8   
    20  0x741622a99fc74628f95f16c7a0d21f45987c794d4e3d1fa9ad652d28cfa9972a   
    21  0xfb0cedf40d166b254df13202f006f58c643d381b218a161e78a7b42da2ebe76a   
    22  0xbd001b16e7d656c7147feb26603b593ce3ac5de8bf2002aaf444792f07bc6d0f   
    23  0x04e488c750c0c14aec13471da3af575dedb05b44a740004c83e845f90f85c991   
    24  0x0e67e6ff66528195b012a9cf45ec72fe3bbfce5a072432902698baeb807415a9   
    25  0x0e67e6ff66528195b012a9cf45ec72fe3bbfce5a072432902698baeb807415a9   
    26  0x31e5df84b750840dd405e1ef6628be70dd64f4d94126b9ec41675e5862ed9971   
    27  0x0e256e9f02a6bb71159e335a644232777db5e82e7536666b36b5938c2187f44f   
    28  0x0e256e9f02a6bb71159e335a644232777db5e82e7536666b36b5938c2187f44f   
    29  0xa1c8541fea7626b533a8baebc6285ef1126509566c23de8ed22a7c465c8fb725   
    30  0x37e5193268761e183115cade9d154b85c11abd7297b324882e491c22685d8706   
    31  0x2f7d3e397f7649c4419f45ad31bfd02efefe614c773cecd7e5ce138f8e21974d   
    32  0xcf9f654ed8fd7c433b820038f97f9829b02bcb1bbbde5e962faf6743fab89dc8   
    33  0xefd19c5d87ab949e9223a4538a9b3a37a2efdc660e51047bf4eb882b70ff260e   
    34  0xf8ec106ea196241fa348e3fcc6b321b535ac83144fa9ce3236da25850777ccaa   
    35  0x5704dbbf1f74ae46f5f531b4ff1b724d5d9cbf1abb85f1e469a9d603b1dfe302   
    36  0xaae2b943f8d3bd544fdea11bfef9dc7ac0002fc0e7b8cb064baf2b7af1d8bafb   
    37  0xf2d6eff2b6f2ea64d33b37b19e7294ce46ca3d837c9937127c722c1ee892eb53   
    
           amountGet  
    0   5.270000e+18  
    1   3.050000e+18  
    2   3.050000e+18  
    3   1.860000e+18  
    4   1.860000e+18  
    5   1.049000e+18  
    6   8.100000e+17  
    7   8.100000e+17  
    8   7.200000e+17  
    9   5.549000e+17  
    10  5.549000e+17  
    11  4.988000e+17  
    12  4.928000e+17  
    13  2.492000e+17  
    14  2.327000e+17  
    15  2.327000e+17  
    16  2.050000e+17  
    17  1.826000e+17  
    18  1.804000e+17  
    19  1.804000e+17  
    20  1.420000e+17  
    21  1.279875e+17  
    22  7.048800e+16  
    23  6.764000e+16  
    24  5.689858e+16  
    25  5.689858e+16  
    26  4.200000e+16  
    27  4.153184e+16  
    28  4.153184e+16  
    29  6.461000e+15  
    30  5.130000e+15  
    31  2.990729e+15  
    32  2.760000e+15  
    33  1.400000e+15  
    34  1.000000e+15  
    35  4.131883e+14  
    36  4.119526e+14  
    37  8.800000e+13  





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
      <th>buyer</th>
      <th>seller</th>
      <th>transactionHash</th>
      <th>amountGet</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xfdbf2ac5aea9ce211d5ea9a4cb3bf614f9fafa9f798fd17d53246cc23c223ee6</td>
      <td>5.270000e+18</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xe103223d7677973e2ef563b9ce02eddd19edbecbcdf653f6b6945556c4a3685a</td>
      <td>3.050000e+18</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xe103223d7677973e2ef563b9ce02eddd19edbecbcdf653f6b6945556c4a3685a</td>
      <td>3.050000e+18</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xb70817e327930e8be0a97a2b78024c7e1aaa38112622a7442bf8c0aa3d7614c2</td>
      <td>1.860000e+18</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xb70817e327930e8be0a97a2b78024c7e1aaa38112622a7442bf8c0aa3d7614c2</td>
      <td>1.860000e+18</td>
    </tr>
    <tr>
      <th>5</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xb4f8b7f17a107b222bccf53c2765b87dfdf050373b9da77c842ffde17ed17291</td>
      <td>1.049000e+18</td>
    </tr>
    <tr>
      <th>6</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x860fa1785201135caf8e5740f7463e4233cecb2297bd4565fc0f7030b0a63eb2</td>
      <td>8.100000e+17</td>
    </tr>
    <tr>
      <th>7</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x860fa1785201135caf8e5740f7463e4233cecb2297bd4565fc0f7030b0a63eb2</td>
      <td>8.100000e+17</td>
    </tr>
    <tr>
      <th>8</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x8139ceb1e4955e3674bdda2639eab0a250d02a8f9015981cd24db3f0426af98e</td>
      <td>7.200000e+17</td>
    </tr>
    <tr>
      <th>9</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x04c7418ec98ad67beb7cba9c5964c4bedef745d87c752d21965fa0a46b7492b7</td>
      <td>5.549000e+17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x04c7418ec98ad67beb7cba9c5964c4bedef745d87c752d21965fa0a46b7492b7</td>
      <td>5.549000e+17</td>
    </tr>
    <tr>
      <th>11</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x9deca97007af078d5032cb41b7a189831f7fde2b9b794559cece8501139fd159</td>
      <td>4.988000e+17</td>
    </tr>
    <tr>
      <th>12</th>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xbb2ad92d6a3ced02ad12f12af2b95382877a9e9f8e62f4421b246a30dd75d2e4</td>
      <td>4.928000e+17</td>
    </tr>
    <tr>
      <th>13</th>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0x9e0cca325509ad15368c43f6ed72cce5132c454254bdbbe10128f125e10f0ca4</td>
      <td>2.492000e+17</td>
    </tr>
    <tr>
      <th>14</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x8b5b4f7af47fb8513922987286994f2bf9fbb5af0db3a718e7de55e785a33c12</td>
      <td>2.327000e+17</td>
    </tr>
    <tr>
      <th>15</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x8b5b4f7af47fb8513922987286994f2bf9fbb5af0db3a718e7de55e785a33c12</td>
      <td>2.327000e+17</td>
    </tr>
    <tr>
      <th>16</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x581848c7f42ceae4f8bbc4b04af806cff7f3b29f8b404fe4363bcd47f7d91642</td>
      <td>2.050000e+17</td>
    </tr>
    <tr>
      <th>17</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x871782ab1e4655397a0189ea583f3d4f1f24c3429b6329e59bccca3e1e8e8e66</td>
      <td>1.826000e+17</td>
    </tr>
    <tr>
      <th>18</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xeac7c904f47fdd16ac468f03eaa6a7ce51636ec2417ec572dd1fb2b950e2b3f8</td>
      <td>1.804000e+17</td>
    </tr>
    <tr>
      <th>19</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xeac7c904f47fdd16ac468f03eaa6a7ce51636ec2417ec572dd1fb2b950e2b3f8</td>
      <td>1.804000e+17</td>
    </tr>
    <tr>
      <th>20</th>
      <td>0x4D25819e9De2A2EEEdDca953D1eF0182680a7054</td>
      <td>0x4D25819e9De2A2EEEdDca953D1eF0182680a7054</td>
      <td>0x741622a99fc74628f95f16c7a0d21f45987c794d4e3d1fa9ad652d28cfa9972a</td>
      <td>1.420000e+17</td>
    </tr>
    <tr>
      <th>21</th>
      <td>0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6</td>
      <td>0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6</td>
      <td>0xfb0cedf40d166b254df13202f006f58c643d381b218a161e78a7b42da2ebe76a</td>
      <td>1.279875e+17</td>
    </tr>
    <tr>
      <th>22</th>
      <td>0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9</td>
      <td>0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9</td>
      <td>0xbd001b16e7d656c7147feb26603b593ce3ac5de8bf2002aaf444792f07bc6d0f</td>
      <td>7.048800e+16</td>
    </tr>
    <tr>
      <th>23</th>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0x04e488c750c0c14aec13471da3af575dedb05b44a740004c83e845f90f85c991</td>
      <td>6.764000e+16</td>
    </tr>
    <tr>
      <th>24</th>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x0e67e6ff66528195b012a9cf45ec72fe3bbfce5a072432902698baeb807415a9</td>
      <td>5.689858e+16</td>
    </tr>
    <tr>
      <th>25</th>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x0e67e6ff66528195b012a9cf45ec72fe3bbfce5a072432902698baeb807415a9</td>
      <td>5.689858e+16</td>
    </tr>
    <tr>
      <th>26</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x31e5df84b750840dd405e1ef6628be70dd64f4d94126b9ec41675e5862ed9971</td>
      <td>4.200000e+16</td>
    </tr>
    <tr>
      <th>27</th>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0x0e256e9f02a6bb71159e335a644232777db5e82e7536666b36b5938c2187f44f</td>
      <td>4.153184e+16</td>
    </tr>
    <tr>
      <th>28</th>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0x0e256e9f02a6bb71159e335a644232777db5e82e7536666b36b5938c2187f44f</td>
      <td>4.153184e+16</td>
    </tr>
    <tr>
      <th>29</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xa1c8541fea7626b533a8baebc6285ef1126509566c23de8ed22a7c465c8fb725</td>
      <td>6.461000e+15</td>
    </tr>
    <tr>
      <th>30</th>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0x37e5193268761e183115cade9d154b85c11abd7297b324882e491c22685d8706</td>
      <td>5.130000e+15</td>
    </tr>
    <tr>
      <th>31</th>
      <td>0xF23db283c6b43feCe3b1215cF5507B6d78fc2215</td>
      <td>0xF23db283c6b43feCe3b1215cF5507B6d78fc2215</td>
      <td>0x2f7d3e397f7649c4419f45ad31bfd02efefe614c773cecd7e5ce138f8e21974d</td>
      <td>2.990729e+15</td>
    </tr>
    <tr>
      <th>32</th>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0xcf9f654ed8fd7c433b820038f97f9829b02bcb1bbbde5e962faf6743fab89dc8</td>
      <td>2.760000e+15</td>
    </tr>
    <tr>
      <th>33</th>
      <td>0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF</td>
      <td>0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF</td>
      <td>0xefd19c5d87ab949e9223a4538a9b3a37a2efdc660e51047bf4eb882b70ff260e</td>
      <td>1.400000e+15</td>
    </tr>
    <tr>
      <th>34</th>
      <td>0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c</td>
      <td>0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c</td>
      <td>0xf8ec106ea196241fa348e3fcc6b321b535ac83144fa9ce3236da25850777ccaa</td>
      <td>1.000000e+15</td>
    </tr>
    <tr>
      <th>35</th>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0x5704dbbf1f74ae46f5f531b4ff1b724d5d9cbf1abb85f1e469a9d603b1dfe302</td>
      <td>4.131883e+14</td>
    </tr>
    <tr>
      <th>36</th>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0xaae2b943f8d3bd544fdea11bfef9dc7ac0002fc0e7b8cb064baf2b7af1d8bafb</td>
      <td>4.119526e+14</td>
    </tr>
    <tr>
      <th>37</th>
      <td>0xbc1323B62Bdd371C38A5f7904c705d833A836d2E</td>
      <td>0xbc1323B62Bdd371C38A5f7904c705d833A836d2E</td>
      <td>0xf2d6eff2b6f2ea64d33b37b19e7294ce46ca3d837c9937127c722c1ee892eb53</td>
      <td>8.800000e+13</td>
    </tr>
  </tbody>
</table>
</div>




```python
marketmanipulated
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
      <th>buyer</th>
      <th>seller</th>
      <th>transactionHash</th>
      <th>amountGet</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xfdbf2ac5aea9ce211d5ea9a4cb3bf614f9fafa9f798fd17d53246cc23c223ee6</td>
      <td>5.270000e+18</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xe103223d7677973e2ef563b9ce02eddd19edbecbcdf653f6b6945556c4a3685a</td>
      <td>3.050000e+18</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xe103223d7677973e2ef563b9ce02eddd19edbecbcdf653f6b6945556c4a3685a</td>
      <td>3.050000e+18</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xb70817e327930e8be0a97a2b78024c7e1aaa38112622a7442bf8c0aa3d7614c2</td>
      <td>1.860000e+18</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xb70817e327930e8be0a97a2b78024c7e1aaa38112622a7442bf8c0aa3d7614c2</td>
      <td>1.860000e+18</td>
    </tr>
    <tr>
      <th>5</th>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>0xb4f8b7f17a107b222bccf53c2765b87dfdf050373b9da77c842ffde17ed17291</td>
      <td>1.049000e+18</td>
    </tr>
    <tr>
      <th>6</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x860fa1785201135caf8e5740f7463e4233cecb2297bd4565fc0f7030b0a63eb2</td>
      <td>8.100000e+17</td>
    </tr>
    <tr>
      <th>7</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x860fa1785201135caf8e5740f7463e4233cecb2297bd4565fc0f7030b0a63eb2</td>
      <td>8.100000e+17</td>
    </tr>
    <tr>
      <th>8</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x8139ceb1e4955e3674bdda2639eab0a250d02a8f9015981cd24db3f0426af98e</td>
      <td>7.200000e+17</td>
    </tr>
    <tr>
      <th>9</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x04c7418ec98ad67beb7cba9c5964c4bedef745d87c752d21965fa0a46b7492b7</td>
      <td>5.549000e+17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x04c7418ec98ad67beb7cba9c5964c4bedef745d87c752d21965fa0a46b7492b7</td>
      <td>5.549000e+17</td>
    </tr>
    <tr>
      <th>11</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x9deca97007af078d5032cb41b7a189831f7fde2b9b794559cece8501139fd159</td>
      <td>4.988000e+17</td>
    </tr>
    <tr>
      <th>12</th>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xbb2ad92d6a3ced02ad12f12af2b95382877a9e9f8e62f4421b246a30dd75d2e4</td>
      <td>4.928000e+17</td>
    </tr>
    <tr>
      <th>13</th>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0x9e0cca325509ad15368c43f6ed72cce5132c454254bdbbe10128f125e10f0ca4</td>
      <td>2.492000e+17</td>
    </tr>
    <tr>
      <th>14</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x8b5b4f7af47fb8513922987286994f2bf9fbb5af0db3a718e7de55e785a33c12</td>
      <td>2.327000e+17</td>
    </tr>
    <tr>
      <th>15</th>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>0x8b5b4f7af47fb8513922987286994f2bf9fbb5af0db3a718e7de55e785a33c12</td>
      <td>2.327000e+17</td>
    </tr>
    <tr>
      <th>16</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x581848c7f42ceae4f8bbc4b04af806cff7f3b29f8b404fe4363bcd47f7d91642</td>
      <td>2.050000e+17</td>
    </tr>
    <tr>
      <th>17</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x871782ab1e4655397a0189ea583f3d4f1f24c3429b6329e59bccca3e1e8e8e66</td>
      <td>1.826000e+17</td>
    </tr>
    <tr>
      <th>18</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xeac7c904f47fdd16ac468f03eaa6a7ce51636ec2417ec572dd1fb2b950e2b3f8</td>
      <td>1.804000e+17</td>
    </tr>
    <tr>
      <th>19</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xeac7c904f47fdd16ac468f03eaa6a7ce51636ec2417ec572dd1fb2b950e2b3f8</td>
      <td>1.804000e+17</td>
    </tr>
    <tr>
      <th>20</th>
      <td>0x4D25819e9De2A2EEEdDca953D1eF0182680a7054</td>
      <td>0x4D25819e9De2A2EEEdDca953D1eF0182680a7054</td>
      <td>0x741622a99fc74628f95f16c7a0d21f45987c794d4e3d1fa9ad652d28cfa9972a</td>
      <td>1.420000e+17</td>
    </tr>
    <tr>
      <th>21</th>
      <td>0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6</td>
      <td>0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6</td>
      <td>0xfb0cedf40d166b254df13202f006f58c643d381b218a161e78a7b42da2ebe76a</td>
      <td>1.279875e+17</td>
    </tr>
    <tr>
      <th>22</th>
      <td>0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9</td>
      <td>0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9</td>
      <td>0xbd001b16e7d656c7147feb26603b593ce3ac5de8bf2002aaf444792f07bc6d0f</td>
      <td>7.048800e+16</td>
    </tr>
    <tr>
      <th>23</th>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>0x04e488c750c0c14aec13471da3af575dedb05b44a740004c83e845f90f85c991</td>
      <td>6.764000e+16</td>
    </tr>
    <tr>
      <th>24</th>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x0e67e6ff66528195b012a9cf45ec72fe3bbfce5a072432902698baeb807415a9</td>
      <td>5.689858e+16</td>
    </tr>
    <tr>
      <th>25</th>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>0x0e67e6ff66528195b012a9cf45ec72fe3bbfce5a072432902698baeb807415a9</td>
      <td>5.689858e+16</td>
    </tr>
    <tr>
      <th>26</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0x31e5df84b750840dd405e1ef6628be70dd64f4d94126b9ec41675e5862ed9971</td>
      <td>4.200000e+16</td>
    </tr>
    <tr>
      <th>27</th>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0x0e256e9f02a6bb71159e335a644232777db5e82e7536666b36b5938c2187f44f</td>
      <td>4.153184e+16</td>
    </tr>
    <tr>
      <th>28</th>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>0x0e256e9f02a6bb71159e335a644232777db5e82e7536666b36b5938c2187f44f</td>
      <td>4.153184e+16</td>
    </tr>
    <tr>
      <th>29</th>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>0xa1c8541fea7626b533a8baebc6285ef1126509566c23de8ed22a7c465c8fb725</td>
      <td>6.461000e+15</td>
    </tr>
    <tr>
      <th>30</th>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0x37e5193268761e183115cade9d154b85c11abd7297b324882e491c22685d8706</td>
      <td>5.130000e+15</td>
    </tr>
    <tr>
      <th>31</th>
      <td>0xF23db283c6b43feCe3b1215cF5507B6d78fc2215</td>
      <td>0xF23db283c6b43feCe3b1215cF5507B6d78fc2215</td>
      <td>0x2f7d3e397f7649c4419f45ad31bfd02efefe614c773cecd7e5ce138f8e21974d</td>
      <td>2.990729e+15</td>
    </tr>
    <tr>
      <th>32</th>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>0xcf9f654ed8fd7c433b820038f97f9829b02bcb1bbbde5e962faf6743fab89dc8</td>
      <td>2.760000e+15</td>
    </tr>
    <tr>
      <th>33</th>
      <td>0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF</td>
      <td>0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF</td>
      <td>0xefd19c5d87ab949e9223a4538a9b3a37a2efdc660e51047bf4eb882b70ff260e</td>
      <td>1.400000e+15</td>
    </tr>
    <tr>
      <th>34</th>
      <td>0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c</td>
      <td>0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c</td>
      <td>0xf8ec106ea196241fa348e3fcc6b321b535ac83144fa9ce3236da25850777ccaa</td>
      <td>1.000000e+15</td>
    </tr>
    <tr>
      <th>35</th>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0x5704dbbf1f74ae46f5f531b4ff1b724d5d9cbf1abb85f1e469a9d603b1dfe302</td>
      <td>4.131883e+14</td>
    </tr>
    <tr>
      <th>36</th>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>0xaae2b943f8d3bd544fdea11bfef9dc7ac0002fc0e7b8cb064baf2b7af1d8bafb</td>
      <td>4.119526e+14</td>
    </tr>
    <tr>
      <th>37</th>
      <td>0xbc1323B62Bdd371C38A5f7904c705d833A836d2E</td>
      <td>0xbc1323B62Bdd371C38A5f7904c705d833A836d2E</td>
      <td>0xf2d6eff2b6f2ea64d33b37b19e7294ce46ca3d837c9937127c722c1ee892eb53</td>
      <td>8.800000e+13</td>
    </tr>
  </tbody>
</table>
</div>




```python
%%spark -c sql -o pew
select seller, sum(amountGet) as total, count(transactionHash) as num_trans \
from SelfPumpToday where tokenGet LIKE '0x000000000000000%' \
group by seller \
order by total, num_trans desc
```

        num_trans                                      seller         total
    0           1  0xbc1323B62Bdd371C38A5f7904c705d833A836d2E  8.800000e+13
    1           2  0x8Dd44F5ef0d1765a428616E943351d009D7B1088  8.251409e+14
    2           1  0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c  1.000000e+15
    3           1  0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF  1.400000e+15
    4           1  0xF23db283c6b43feCe3b1215cF5507B6d78fc2215  2.990729e+15
    5           2  0x6ec8247cdc03F336118B96f4e862b24B1F947fa2  7.890000e+15
    6           1  0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9  7.048800e+16
    7           2  0xe84a294E045535e39CFFc81D2Dc74b2839035388  8.306368e+16
    8           2  0x736a1120d12f1D19E0cEF35426C7ec7B90514be4  1.137972e+17
    9           1  0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6  1.279875e+17
    10          1  0x4D25819e9De2A2EEEdDca953D1eF0182680a7054  1.420000e+17
    11          3  0xf1C7cA754F5Cd2DF308d19632763d292cF406060  8.096400e+17
    12          4  0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0  1.575200e+18
    13         11  0xAFB7340211f56618789858A73D34a67e837624C4  8.905661e+18
    14          5  0x09e2D651cE800B048611B5DFca8613452F670F7d  1.086900e+19





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
      <th>num_trans</th>
      <th>seller</th>
      <th>total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>0xbc1323B62Bdd371C38A5f7904c705d833A836d2E</td>
      <td>8.800000e+13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>8.251409e+14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c</td>
      <td>1.000000e+15</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF</td>
      <td>1.400000e+15</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>0xF23db283c6b43feCe3b1215cF5507B6d78fc2215</td>
      <td>2.990729e+15</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2</td>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>7.890000e+15</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1</td>
      <td>0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9</td>
      <td>7.048800e+16</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2</td>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>8.306368e+16</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2</td>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>1.137972e+17</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1</td>
      <td>0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6</td>
      <td>1.279875e+17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>1</td>
      <td>0x4D25819e9De2A2EEEdDca953D1eF0182680a7054</td>
      <td>1.420000e+17</td>
    </tr>
    <tr>
      <th>11</th>
      <td>3</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>8.096400e+17</td>
    </tr>
    <tr>
      <th>12</th>
      <td>4</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>1.575200e+18</td>
    </tr>
    <tr>
      <th>13</th>
      <td>11</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>8.905661e+18</td>
    </tr>
    <tr>
      <th>14</th>
      <td>5</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>1.086900e+19</td>
    </tr>
  </tbody>
</table>
</div>




```python
pew
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
      <th>num_trans</th>
      <th>seller</th>
      <th>total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>0xbc1323B62Bdd371C38A5f7904c705d833A836d2E</td>
      <td>8.800000e+13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>0x8Dd44F5ef0d1765a428616E943351d009D7B1088</td>
      <td>8.251409e+14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>0x2ce8F2EA67288e90339bCB4a1B883a9f78bCA78c</td>
      <td>1.000000e+15</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>0x6DBD7240D360BE22CBa37e207a727e5a59FbaAAF</td>
      <td>1.400000e+15</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>0xF23db283c6b43feCe3b1215cF5507B6d78fc2215</td>
      <td>2.990729e+15</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2</td>
      <td>0x6ec8247cdc03F336118B96f4e862b24B1F947fa2</td>
      <td>7.890000e+15</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1</td>
      <td>0xa81EB39A59fB926c81F260A6539e1C7e85fa71b9</td>
      <td>7.048800e+16</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2</td>
      <td>0xe84a294E045535e39CFFc81D2Dc74b2839035388</td>
      <td>8.306368e+16</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2</td>
      <td>0x736a1120d12f1D19E0cEF35426C7ec7B90514be4</td>
      <td>1.137972e+17</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1</td>
      <td>0x73ef6A3b70CD95C775Faa1eEF0691AA6800584e6</td>
      <td>1.279875e+17</td>
    </tr>
    <tr>
      <th>10</th>
      <td>1</td>
      <td>0x4D25819e9De2A2EEEdDca953D1eF0182680a7054</td>
      <td>1.420000e+17</td>
    </tr>
    <tr>
      <th>11</th>
      <td>3</td>
      <td>0xf1C7cA754F5Cd2DF308d19632763d292cF406060</td>
      <td>8.096400e+17</td>
    </tr>
    <tr>
      <th>12</th>
      <td>4</td>
      <td>0x1c17d6CEF711ea2b1B3dF19ffa5E8Efc1467daE0</td>
      <td>1.575200e+18</td>
    </tr>
    <tr>
      <th>13</th>
      <td>11</td>
      <td>0xAFB7340211f56618789858A73D34a67e837624C4</td>
      <td>8.905661e+18</td>
    </tr>
    <tr>
      <th>14</th>
      <td>5</td>
      <td>0x09e2D651cE800B048611B5DFca8613452F670F7d</td>
      <td>1.086900e+19</td>
    </tr>
  </tbody>
</table>
</div>



## Store to parquet


```python
%%spark
pumps_df.write.parquet("/tmp/test.parquet")

```
