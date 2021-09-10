prereq:
* run NATS server: `docker run -d -p4222:4222 -p6222:6222 -p8222:8222 -p6543:6543 --name=nats nats:2.4.0-alpine3.14 nats-server -js --port=4222 --http_port=8222 --profile=6543`

reproduce:
* `./run.sh 20 100`
* `cd consumer && ./run.sh 50 0.1s`
