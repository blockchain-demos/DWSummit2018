
# Remote_Eth_Producer

- 1. Start a remote Spark Driver - running in YARN.
- 2. Parse all transaction logs[] from a current block on the ethereum chain
- 3. Produce all ethereum logs to a kafka topic

### 1.0 Load sparkmagic
https://github.com/jupyter-incubator/sparkmagic

Sparkmagic is a set of tools for interactively working with remote Spark clusters through Livy, a Spark REST server.

Any cells ran with `%%spark` will execute against a remote spark.


```python
%load_ext sparkmagic.magics
```

### 1.1 Create a remote livy session

Define spark configuration to use for this session.

- We can leverage a **python virtual environment** in our remote Spark Session:
  - `gethdemo.tar.gz` contains a conda virtual environment created from `./py_kafka_reqs.txt`
  - `gethdemo.tar.gz` is available on `hdfs://user/noobie/`

- Since kafka does not fully support [Delegation Tokens](https://cwiki.apache.org/confluence/display/KAFKA/KIP-48+Delegation+token+support+for+Kafka#KIP-48DelegationtokensupportforKafka-APIsandrequest/responseclasses), we can also pass in a keytab through `--files`, if connecting to a kerberized Kafka Broker.

- Livy sessions can take up to 60 seconds to start. Be patient.




```python
%%spark config
{
  "name":"remote_eth_producer",
  "driverMemory":"1G",
  "numExecutors":1,
  "proxyUser":"noobie",
  "archives": ["hdfs:///user/noobie/gethdemo.tar.gz"],
  "files" : ["hdfs:///user/noobie/noobie.keytab"],
  "queue": "streaming",
  "conf": {"spark.yarn.appMasterEnv.PYSPARK_PYTHON":"gethdemo.tar.gz/demo/bin/python3.5",
          "PYSPARK_PYTHON":"gethdemo.tar.gz/demo/bin/python3.5"
          }
}
```


```python
%spark add -s ethlogproducer -l python -u http://hdp-3.demo.url:8999 --auth Kerberos
```

    Skip
    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>290</td><td>application_1527994885375_0068</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://hdp-3.demo.url:8088/proxy/application_1527994885375_0068/">Link</a></td><td><a target="_blank" href="http://hdp-4.demo.url:8042/node/containerlogs/container_e56_1527994885375_0068_01_000001/noobie">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.


### 1.2 Obtain a keberos ticket for connecting to Kafka



```python
%%spark
import subprocess
kinit = '/usr/bin/kinit'
kinit_args = [kinit, '-kt', "noobie.keytab" , "noobie"]
subprocess.check_output(kinit_args)
```

    b''

---
### 2.0 Connect to Ethereum using Web3 from within the spark session

Web3 is a python library for interacting with Ethereum http://web3py.readthedocs.io/en/stable/.
Its API is derived from the [Web3.js](https://github.com/ethereum/wiki/wiki/JavaScript-API) Javascript API


```python
%%spark -s ethlogproducer
from web3 import Web3, HTTPProvider, IPCProvider

gethRPCUrl='http://10.132.86.5:8545'
web3 = Web3(HTTPProvider(gethRPCUrl))

# Retrieve the last block number available from geth
currentblock = web3.eth.getBlock('latest').number
print("Latest block: " + str(currentblock))
```

    Latest block: 5762966

#### 2.1 Define a HexJsonEncoder to cleanse Web3 response

Web3 returns an AttributeDict containing `HexBytes`, which is not recognized by Json or Kafka.
https://github.com/ethereum/web3.py/issues/782

See cls in https://docs.python.org/2/library/json.html#basic-usage

```
usage:
  blockjson = json.dumps(somePydDict, cls=HexJsonEncoder)    
```  


```python
%%spark -s ethlogproducer
from hexbytes import HexBytes
import threading, logging, time, json

class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        return super().default(obj)
```

## 2.2 Define some helper methods to pull logs[] from the eth chain

#### 2.2.1 Note the schema returned from a getBlock() call

- `web3.eth.getBlock(5682605,full_transactions=True).keys()`
```
dict_keys(['sealFields', 'mixHash', 'timestamp', 'number', 'nonce', 'gasUsed', 'gasLimit', 'size', 'totalDifficulty', 'transactions', 'extraData', 'difficulty', 'miner', 'sha3Uncles', 'transactionsRoot', 'parentHash', 'hash', 'stateRoot', 'logsBloom', 'author', 'receiptsRoot', 'uncles'])
```

#### 2.2.2 Which contains some nested fields, such as `transactions`:
- `dict(web3.eth.getBlock(5682605,full_transactions=True)['transactions'][0]).keys()`
```
dict_keys(['raw', 'creates', 'condition', 'value', 'blockHash', 'gas', 'r', 'v', 'chainId', 'to', 'blockNumber', 'input', 'transactionIndex', 'standardV', 'publicKey', 'gasPrice', 's', 'nonce', 'from', 'hash'])
```

But still does not include logs which the transactions may have generated

#### 2.2.3 Retrieve transaction['logs']


Note, the `logs` are available from a [getTransactionReceipt](https://github.com/ethereum/wiki/wiki/JSON-RPC#returns-31), but not from a `getBlock(full_transactions=True)`

- `dict(web3.eth.getTransactionReceipt(transaction_hash=sample_tx)).keys()`
```
dict_keys(['transactionIndex', 'cumulativeGasUsed', 'root', 'logs', 'blockHash', 'logsBloom', 'status', 'transactionHash', 'blockNumber', 'contractAddress', 'gasUsed'])
```

#### 2.2.4 **logs** itself is a nested field, containing the `data` used for this transaction.
- `dict(web3.eth.getTransactionReceipt(transaction_hash=sample_tx))['logs'].keys()`
```
dict_keys(['transactionIndex', 'logIndex', 'data', 'topics', 'blockHash', 'transactionHash', 'transactionLogIndex', 'type', 'blockNumber', 'address'])
```


Thus, we will define 2 methods:

- **getTransactionsInBlock(BLOCKNUM)**
    - Return a JSON with all transaction shown in 2.2.2 for the specified BLOCKNUM
- **produceAllEventLogs(BLOCKNUM,GETH_EVENTS_KAFKA_TOPIC)**  
    - Retrieves all event logs **(2.2.4)** and produces it to GETH_EVENTS_KAFKA_TOPIC


```python
%%spark
def getTransactionsInBlock(BLOCKNUM):
    transactions_in_range=[]
    transactions_in_block = web3.eth.getBlock(BLOCKNUM,full_transactions=True)['transactions']     
    for transaction in transactions_in_block:
        if transaction is not None:
            cleansesed_transactions=json.dumps(dict(transaction),cls=HexJsonEncoder)     
            transactions_in_range.append(cleansesed_transactions)
    return transactions_in_range                

def produceAllEventLogs(BLOCKNUM,GETH_EVENTS_KAFKA_TOPIC):  
    for transaction in getTransactionsInBlock(BLOCKNUM):
        tx_event=dict(web3.eth.getTransactionReceipt(transaction_hash=json.loads(transaction)['hash']))
        if(tx_event is not None):
            if(tx_event['logs'] is not None and tx_event['logs']):
                # Decode every nested tx_log in the tx_event[logs]
                for tx_log in tx_event['logs']:
                    tx_json=json.dumps(dict(tx_log), cls=HexJsonEncoder)
                    producer.send(GETH_EVENTS_KAFKA_TOPIC, tx_json)              
```

#### produceAllEventLogs will produce all transactions with logs[] to a kafka topic.

**sample data produced:**
```
{'address': '0xdd974D5C2e2928deA5F71b9825b8b646686BD200',
 'blockHash': '0x8c11efca021f3260fab2f4736718d94acb6530a567d5462e57c484ff2e04aa3d',
 'blockNumber': 5682604,
 'data': '0x00000000000000000000000000000000000000000000007b1a070a274c6a8000',
 'logIndex': 1,
 'topics': ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
  '0x000000000000000000000000b7fcadc9a9023b553594e607e12b40b7f8f0a670',
  '0x0000000000000000000000002f41ea745c67724fdc65ff909318edeb73cfd6e7'],
 'transactionHash': '0x78f7abd332f508350b8a5c5d3e0e77b4d34629059efea1ff9592a7929d311210',
 'transactionIndex': 4,
 'transactionLogIndex': '0x0',
 'type': 'mined'},
 {'address': '0xdd974D5C2e2928deA5F71b9825b8b646686BD200',
 'blockHash': '0x8c11efca021f3260fab2f4736718d94acb6530a567d5462e57c484ff2e04aa3d',
 'blockNumber': 5682604,
 'data': '0x00000000000000000000000000000000000000000000007cda210a234c6c5420',
 'logIndex': 2,
 ...
 ```

---
## 3. Kafka Producer
Use the **HexJsonEncoder** decoder from **2.1** in the kafkaProducer **value_serializer** to perform data sanitation on producer.send


```python
%%spark
from kafka import KafkaConsumer, KafkaProducer
producer = KafkaProducer(bootstrap_servers=['hdp-4.demo.url:6667',
                                            'hdp-5.demo.url:6667',
                                            'hdp-6.demo.url:6667'],
                        security_protocol="SASL_PLAINTEXT",
                        sasl_mechanism="GSSAPI",
                        value_serializer=lambda m: json.dumps(m, cls=HexJsonEncoder).encode('utf-8'))
```

## 3.1 Smoketest producing 1 block's event logs to kafka


```python
%%spark
kafkatopic="eth_eventlogs"

currentblock = web3.eth.getBlock('latest').number
produceAllEventLogs(BLOCKNUM= currentblock,
                    GETH_EVENTS_KAFKA_TOPIC = kafkatopic )

# Print metrics to verify producer connected successfuly
producer.metrics()
```

    {'producer-metrics': {'metadata-age': 2.923041259765625, 'batch-size-avg': 1052.0108695652175, 'io-ratio': 0.001169609662193618, 'record-size-avg': 720.9565217391304, 'io-time-ns-avg': 145312.07534502138, 'response-rate': 2.8647041684510257, 'bufferpool-wait-ratio': 0.0, 'batch-size-max': 11493.0, 'byte-rate': 2939.8883841457987, 'request-rate': 2.8357456660087172, 'incoming-byte-rate': 336.30373047826953, 'record-queue-time-avg': 0.0029542083325593367, 'connection-creation-rate': 0.05788735491010056, 'record-send-rate': 3.8880908137602885, 'connection-count': 1.0, 'io-wait-ratio': 0.09714018325392146, 'compression-rate-avg': 1.0, 'network-io-rate': 5.700440358650351, 'outgoing-byte-rate': 3010.6253046303113, 'record-queue-time-max': 0.00562286376953125, 'requests-in-flight': 0.0, 'request-size-max': 11569.0, 'produce-throttle-time-avg': 0.0, 'produce-throttle-time-max': 0.0, 'record-retry-rate': 0.0, 'request-latency-max': 101.8209457397461, 'select-rate': 8.04884422848802, 'request-size-avg': 1061.6530612244899, 'records-per-request-avg': 1.391304347826087, 'record-size-max': 1249.0, 'io-wait-time-ns-avg': 12068852.838480247, 'connection-close-rate': 0.02895032544696359, 'record-error-rate': 0.0, 'request-latency-avg': 2.054204746168487}, 'kafka-metrics-count': {'count': 56.0}, 'producer-node-metrics.node-1003': {'request-size-avg': 1082.875, 'request-rate': 2.779395609533765, 'outgoing-byte-rate': 3009.7227035167684, 'response-rate': 2.779430265143889, 'request-latency-max': 101.8209457397461, 'request-size-max': 11569.0, 'request-latency-avg': 2.0250827074050903, 'incoming-byte-rate': 250.03017667831085}, 'producer-node-metrics.node-bootstrap': {'request-rate': 0.05787228939286981, 'request-size-avg': 43.0, 'outgoing-byte-rate': 2.488546593163211, 'incoming-byte-rate': 86.4040587129231, 'request-latency-max': 6.540536880493164, 'request-size-max': 45.0, 'response-rate': 0.08680913811425713, 'request-latency-avg': 3.4520626068115234}, 'producer-topic-metrics.eth_eventlogs': {'record-send-rate': 3.8880401533170224, 'byte-rate': 2939.860456219719, 'record-error-rate': 0.0, 'record-retry-rate': 0.0, 'compression-rate': 1.0}}

## 3.2 Run a producer for a given number of blocks


```python
%%spark
import sys

blockstart= web3.eth.getBlock('latest').number-1
blockend  = web3.eth.getBlock('latest').number+5000

kafkatopic="eth_eventlogs"

print("Start at block: " + str(blockstart))
try:
    global blockstart
    while blockstart < blockend:
        currentblock = web3.eth.getBlock('latest').number
        if currentblock < blockstart:
            time.sleep(0.2)
            pass
        else:
            produceAllEventLogs(BLOCKNUM= currentblock,
                                GETH_EVENTS_KAFKA_TOPIC = kafkatopic )
            blockstart=blockstart+1
            time.sleep(0.2)               
except:
    print("Unexpected error:", sys.exc_info()[0])
    pass
print("Finished producing block :" + str(blockend))
```

    Start at block: 5762965
    Finished producing block :5762968

### When done, cleanup livy sessions...


```python
%spark delete -s ethlogproducer
```


```python
%spark cleanup

```
