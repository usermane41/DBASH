# D-Bash

TODO:
- Vérifier que getLoadAverage() returns le premier nombre de la ligne
- Ajouter des sécurités sur les recv() (failure de recv, message mal formé)

# Installing ZeroMQ

```
sudo apt update
sudo apt install -y libzmq3-dev
```

# Compiling

```
g++ -std=c++20 server.cpp -o server -lzmq -pthread
```

# Launching the program

```
./server node_id
```

With `nb_nodes` being the number of lines in the `config_servers.txt` file and `node_id` being an integer such that 0 <= `node_id` < `nb_nodes`