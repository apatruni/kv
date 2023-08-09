# kv
Distributed key value store

Run `sh startup.sh` 

1. Try `curl --location --request POST 'localhost:10001/put' \
--header 'Content-Type: application/json' \
--data '{
    "Key": "400",
    "Value" : "test"
}'`

2. Try `curl --location 'localhost:10001/get/400'`

3. Try `curl --location 'localhost:10000/get/400'`

4. You should see the same value on both 