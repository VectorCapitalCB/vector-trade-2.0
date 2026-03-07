mvn clean package dockerfile:build

####### PUERTO

ngrok config add-authtoken 2XG9V4Xp8K3AulT2IxO9nOSrWEI_38vpThrdx5zdtEeeFTRAL


/home/Voultech/app/VECTOR-TRADE-QA2/blotter-services-qa/



###### DOCKER

mvn compile package -Pdistribution
docker build --no-cache --progress=plain -t service-blotter:latest .
docker tag service-blotter:latest 10.0.1.8:5000/service-blotter:latest
docker push 10.0.1.8:5000/service-blotter:latest

###### KEYCLOAK
#KEYCLOAK_USER: admin_keycloak
#KEYCLOAK_PASSWORD: lXaiQu7giooxosdddoXe1iS5ah777



sudo docker exec -it blotter-services-service-blotter-1 /bin/sh

sudo docker logs --tail 0 460b1d3da038



