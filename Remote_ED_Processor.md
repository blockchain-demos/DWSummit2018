
# Remote_ED_Processor

- 1.  Start a remote Spark Driver - running in YARN. This will run a long running Kafka Producer/Consumer.
- 2.  Load Web3 and the Smart Contract ABI for the Etherdelta Smart Contract
- 3.  Consume all kafka messages from [**Remote_Eth_Producer**](./Remote_Eth_Producer.ipynb)
- 4.  Detects Volume Manipulation occuring, and produce to a new Kafka "SelfPump" topic
    - Applies the ContractABI to messages consumed, to apply the proper Schema for that Ethereum Event log

---
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
  "name":"remoteEDProcessor",
  "driverMemory":"2G",
  "numExecutors":1,
  "proxyUser":"noobie",
  "archives": ["hdfs:///user/noobie/gethdemo.tar.gz"],
  "files": ["hdfs:///user/noobie/etherdelta_abi.json",
           "/user/noobie/noobie.keytab"],
  "queue": "streaming",
  "conf": {"spark.yarn.appMasterEnv.PYSPARK_PYTHON":"gethdemo.tar.gz/demo/bin/python3.5",
          "spark.jars.packages":"org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0"}
}
```


```python
%spark add -s edprocessor -l python -u http://hdp-3.demo.url:8999 --auth Kerberos
```

    Skip
    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>291</td><td>application_1527994885375_0069</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://hdp-3.demo.url:8088/proxy/application_1527994885375_0069/">Link</a></td><td><a target="_blank" href="http://hdp-5.demo.url:8042/node/containerlogs/container_e56_1527994885375_0069_01_000001/noobie">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.


### 1.2 Obtain a keberos ticket for connecting to Kafka
keytab was added to Yarn distributed cache via the `--files` option in the spark config


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
%%spark
from web3 import Web3, HTTPProvider, IPCProvider

gethRPCUrl='http://10.132.86.5:8545'
web3 = Web3(HTTPProvider(gethRPCUrl))

# Retrieve the last block number available from geth RPC
currentblock = web3.eth.getBlock('latest').number
print("Latest block: " + str(currentblock))
```

    Latest block: 5767704

#### 2.1 Define a HexJsonEncoder to cleanse Web3 response

Web3 returns an AttributeDict containing `HexBytes`, which is not recognized by Json or Kafka.
https://github.com/ethereum/web3.py/issues/782

See cls in https://docs.python.org/2/library/json.html#basic-usage

```
usage:
  blockjson = json.dumps(somePydDict, cls=HexJsonEncoder)    
```  


```python
%%spark
from hexbytes import HexBytes
import json
class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        return super().default(obj)
```

### 2.2 Define Known Hashes For Ethedelta's Contract

- **TOPIC IDs** - Methods within a Smart Contract have a **unique** [TOPIC](http://solidity.readthedocs.io/en/develop/contracts.html?highlight=topic#events) Id

- **Contract Address** - The smart contract address we are looking at, in this case EtherDelta https://etherscan.io/address/0x8d12a197cb00d4747a1fe03395095ce2a5cc6819#code


```python
%%spark
ETHERDELTA_CONTRACT_ADDR = '0x8d12A197cB00D4747a1fe03395095ce2A5CC6819'

ETHERDELTA_TRADE_METHOD_TOPICID='0x6effdda786735d5033bfad5f53e5131abcced9e52be6c507b62d639685fbed6d'
ETHERDELTA_CANCEL_METHOD_TOPICID='0x1e0b760c386003e9cb9bcf4fcf3997886042859d9b6ed6320e804597fcdb28b0'
ETHERDELTA_DEPOSIT_METHOD_TOPICID='0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
```

### 2.3 Load the ED Contract ABI

When a solidity contract is deployed to the Ethereum Blockchain, there is a corresponding ["Contract ABI"](https://web3js.readthedocs.io/en/1.0/web3-eth-abi.html#web3-eth-abi) created, which can be used to extract the schema and data for the [output of Events](https://web3js.readthedocs.io/en/1.0/web3-eth-contract.html#contract-events) being invoked from a smart contract.

- `hdfs:///user/noobie/etherdelta_abi.json` was retrieved from [etherscan](https://api.etherscan.io/api?module=contract&action=getabi&address=0x8d12a197cb00d4747a1fe03395095ce2a5cc6819)

[Example Parsing of 1 transaction for reference](./Images/process_eth_logs_with_abi.jpg)


```python
%%spark
with open('etherdelta_abi.json') as f:
    ETHERDELTA_CONTRACT_ABI = json.load(f)
# Create a local instance of the smart contract
ED_CONTRACT_OBJ = web3.eth.contract(address=ETHERDELTA_CONTRACT_ADDR,
                                    abi=ETHERDELTA_CONTRACT_ABI)
```

---
### 3.0 Create a consumer for eth_eventlogs


```python
%%spark
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(bootstrap_servers=['hdp-3.demo.url:6667',
                                           'hdp-5.demo.url:6667',
                                           'hdp-6.demo.url:6667'],
                         security_protocol="SASL_PLAINTEXT",
                         sasl_mechanism="GSSAPI",
                         auto_offset_reset='earliest',
                         client_id='edprocessor',
                         request_timeout_ms=501,
                         consumer_timeout_ms=500,
                         max_poll_interval_ms=500)
```


Sample Data Consumed:
```
{'address': '0x8d12A197cB00D4747a1fe03395095ce2A5CC6819',
 'blockHash': '0x8c11efca021f3260fab2f4736718d94acb6530a567d5462e57c484ff2e04aa3d',
 'blockNumber': 5682604,
 'data': '0x..',
 'logIndex': 1,
 'topics': ['0x6effdda786735d5033bfad5f53e5131abcced9e52be6c507b62d639685fbed6d'],
 'transactionHash': '0x..',
 'transactionIndex': 4,
 'transactionLogIndex': '0x0',
 'type': 'mined'},
 ```


When we subscribe to this topic, we'll need to filter for all transactions being executed from:

```
'address' : ETHERDELTA_CONTRACT_ADDR
'topics' : ETHERDELTA_TRADE_METHOD_TOPICID
```

---
### 4.0 Define the producer



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

### 4.1 Run the full consumer/producer to process ED Trades
Run the consumer for a given period of time

This will:

- Consume: `EVENT_TOPIC_TO_CONSUME`
- Filter For trades occuring on the ED Contract:
    ```
    'address' : ETHERDELTA_CONTRACT_ADDR
    'topics'  : ETHERDELTA_TRADE_METHOD_TOPICID
    ```
- Parse out trades who have the same `buyer` and `seller`
- Produce all "Self Pump" trades to a new topic: `SELF_PUMP_TOPIC`


```python
%%spark
import sys, time

CONSUMER_RUN_SEC = 10

# Topic To Consume
EVENT_TOPIC_TO_CONSUME="eth_eventlogs_sat"
consumer.subscribe([EVENT_TOPIC_TO_CONSUME])
print("Cosuming from:" + EVENT_TOPIC_TO_CONSUME)

# Topic To Produce Market Manipulation trades to
SELF_PUMP_TOPIC="selfpump_trades_sat"

try:
    global CONSUMER_RUN_SEC
    CONSUMER_START=0

    while CONSUMER_START < CONSUMER_RUN_SEC:
        for message in consumer:

            consumermsg = json.loads(json.loads(message.value.decode('utf-8')))
            # Filter For ED Smart Contract
            if consumermsg['address'] == ETHERDELTA_CONTRACT_ADDR:

                # Filter for ED TRADE METHOD
                if consumermsg['topics'][0] == ETHERDELTA_TRADE_METHOD_TOPICID:

                    # Query Web3 for the TransactionReceipt which includes full log hexdata
                    tx_receipt = web3.eth.getTransactionReceipt(consumermsg['transactionHash'])
                    try:
                        # Use the ED_CONTRACT_OBJ ABI To process the tx_receipt for the "Trade()" method
                        # Which returns the data with proper column headers
                        ed_etl_trade = dict(ED_CONTRACT_OBJ.events.Trade().processReceipt(tx_receipt)[0])
                    except:
                        print("Error:", sys.exc_info()[0])
                        break

                    # Skip Invalid transactions - Those posted to blockchain which do not include logs    
                    if ed_etl_trade is not None:
                        # ['args'] is Returned as AttributeDict -- Flatten by calling dict()
                        ed_etl_trade['args'] =dict(ed_etl_trade['args'])

                    # Check for Buyer == Seller
                        if ed_etl_trade['args']['get'] == ed_etl_trade['args']['give']:
                    # PRODUCE to SELF_PUMP_TOPIC
                            print("Found a Market Pumper Executing his own order!")
                            producer.send(SELF_PUMP_TOPIC, ed_etl_trade)                          

        CONSUMER_START=CONSUMER_START+1
        time.sleep(1)
except:
    print("Unexpected error:", sys.exc_info()[0])
    pass
```

    Cosuming from:eth_eventlogs_sat
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d7b0b70>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d77f4e0>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d77f908>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73f390>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73f198>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73f828>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73f2b0>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73f320>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d70f668>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d6fd6d8>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d70f710>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d6fd5f8>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d754358>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d70ff98>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d70f278>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d7545c0>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d754208>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d754cc0>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73fb38>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73fe10>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d754cc0>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d70f438>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d754d30>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d6fdef0>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d70fa90>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73fb00>
    Found a Market Pumper Executing his own order!
    <kafka.producer.future.FutureRecordMetadata object at 0x7fac2d73f0f0>
    Unexpected error: <class 'TypeError'>
