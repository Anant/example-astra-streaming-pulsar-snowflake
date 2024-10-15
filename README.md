# example-astra-streaming-pulsar-snowflake



Install the prerequisites 
```
pip install -r requirements.txt
```

Copy the env file
```
cp .env.example .env
```

Add your topic , pulsar endpoint, and auth token 


Run the script

```
python stock_producer.py data/stock-prices-10.csv 
python stock_producer.py data/stock-prices-1000.csv 
```