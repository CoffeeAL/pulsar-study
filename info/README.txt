1. Launching Pulsar using Docker
    1.1. Pull image from Dockerhub
    docker pull apachepulsar/pulsar-standalone
    1.2. Run container
    docker run -d -p 6650:6650 -p 8080:8080 -v "C:\Users\aloiko" --name pulsar apachepulsar/pulsar-standalone
       (-v - folder where you want to store data, --name - container name, image name in the end)

2. Display all clusters and tenants
    docker exec -it pulsar /pulsar/bin/pulsar-admin clusters list
    docker exec -it pulsar /pulsar/bin/pulsar-admin tenants list

3. Create tenant, namespace and topic, display them
    docker exec -it pulsar /pulsar/bin/pulsar-admin tenants create my-tenant
    docker exec -it pulsar /pulsar/bin/pulsar-admin namespaces create my-tenant/my-namespace
    docker exec -it pulsar /pulsar/bin/pulsar-admin topics create persistent://my-tenant/my-namespace/my-topic

    docker exec -it pulsar /pulsar/bin/pulsar-admin namespaces list my-tenant
    docker exec -it pulsar /pulsar/bin/pulsar-admin topics list my-tenant/my-namespace

4. Subscribe consumer to topic
    docker exec -it pulsar /pulsar/bin/pulsar-client consume persistent://my-tenant/my-namespace/my-topic --num-messages 0 --subscription-name example-subscription --subscription-type Exclusive
      (--num-messages 0 means without limits)

5. Produce to topic
    docker exec -it pulsar /pulsar/bin/pulsar-client produce persistent://my-tenant/my-namespace/my-topic --num-produce 2 --messages "Hello Pulsar"
      (this command is from another (!) command line)
