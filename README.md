## Test Minikube Cluster with Kafka, Kafka-Connect, Mosquitto

This is just a little test deployment, of a minikube cluster with kafka and mosquitto.


---
#### Start the Minikube-Cluster
```shell
minikube start --memory=7000
```

#### Create Namespace

```shell
kubectl create namespace kafka
```

### Install Strimzi-Operator for Kafka
```shell
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka'
```

## Apply the Configs

#### Kafka
```shell
kubectl apply -f ./configs/kafka-single-node.yaml -n kafka
```

```shell
kubectl apply -f kafka-single-node.yaml -n kafka
```

#### Kafka-Connect

Dependent on 'kafka-single-node'
```shell
kubectl apply -f ./configs/kafka-connect.yaml -n kafka
```

#### MQTT

Not dependent:
```shell
kubectl apply -f ./configs/mqtt-deployment.yaml -n kafka
```

Dependent on Kafka-Connect:
```shell
kubectl apply -f ./configs/mqtt-source-connector.yaml -n kafka
```

## Test the Deployment
Forward Ports of MQTT:
```shell
kubectl port-forward service/mosquitto-service -n kafka  1883:1883
```


