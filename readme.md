#clin-pipelines
##Package
```sbt assembly```
##Execute
```scala /path/to/clin-pipelines.jar task-name```

or

```java -jar /path/to/clin-pipelines.jar task-name```

##Local Development

### Install a local docker registry
```docker run -d -p 5000:5000 --restart=always --name registry registry:2```

### Build and deploy the clin-pipeline image
```
docker build -t 192.168.0.16:5000/clin-pipelines:2020.1 .
docker push 192.168.0.16:5000/clin-pipelines:2020.1
```

### Test the image itself
```
docker run -e SERVICE_ENDPOINT=http://192.168.0.16:9000 localhost:5000/clin-pipelines:2020.1 check-new-files-on-s3
```