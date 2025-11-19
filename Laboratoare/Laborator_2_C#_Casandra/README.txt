DOCKER CASSANDRA
----------------------------------------
docker run --name cassandra-node -d -p 9042:9042 cassandra:4.1
----------------------------------------
----------------------------------------
//if i want to populate werehouse

docker exec -it cassandra-node cqlsh
----------------------------------------
CREATE KEYSPACE warehouse 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE warehouse;

CREATE TABLE items_by_id (
    id int PRIMARY KEY, 
    name text
);

INSERT INTO items_by_id (id, name) VALUES (1, 'Toyota Corolla');
INSERT INTO items_by_id (id, name) VALUES (2, 'Honda Civic');
INSERT INTO items_by_id (id, name) VALUES (3, 'Ford Mustang');
INSERT INTO items_by_id (id, name) VALUES (4, 'Chevrolet Camaro');
INSERT INTO items_by_id (id, name) VALUES (5, 'BMW 3 Series');
INSERT INTO items_by_id (id, name) VALUES (6, 'Audi A4');
INSERT INTO items_by_id (id, name) VALUES (7, 'Mercedes-Benz C-Class');
INSERT INTO items_by_id (id, name) VALUES (8, 'Tesla Model 3');
INSERT INTO items_by_id (id, name) VALUES (9, 'Nissan Altima');
INSERT INTO items_by_id (id, name) VALUES (10, 'Volkswagen Golf');



11 | Hyundai Elantra
12 | Kia Optima
13 | Subaru Impreza
14 | Mazda 6
15 | Lexus IS
16 | Acura TLX
17 | BMW 5 Series
18 | Audi A6
19 | Volkswagen Passat
20 | Mercedes-Benz E-Class





SELECT * FROM items_by_id;

----------------------------------------

DOCKER REDIS
----------------------------------------



docker run --name redis-node -d -p 6379:6379 redis

<---redis cli for checking--->
docker exec -it redis-node redis-cli

SET testkey "Hello Redis"
GET testkey



redis check for working

docker exec -it redis-node redis-cli
expected result ---> 127.0.0.1:6379>
----------------------------------------
----------------------------------------
running project

dotnet run --launch-profile "DWNode1"
dotnet run --launch-profile "DWNode2"
dotnet run --project ProxyNode


checking project  DWNodes
http://localhost:5113/api/data/1    //json data
http://localhost:5114/api/data/1 

proxy
GET http://localhost:5171/proxy/data/3
GET /api/data?offset&limit
Accept: application/xml

PUT http://localhost:5113/api/data/{id}
POST http://localhost:5114/api/data/update
POST http://localhost:5114/api/data/push
----------------------------------------
COMENZI
----------------------------------------
Stop container:

docker stop cassandra-node
docker stop redis-node


Start container oprit:

docker start cassandra-node
docker start redis-node


Șterge container:

docker rm cassandra-node
docker rm redis-node


Vezi loguri:

docker logs cassandra-node
docker logs redis-node


