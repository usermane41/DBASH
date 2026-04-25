# D-Bash — Shell distribué avec équilibrage de charge

## Prérequis

```bash
sudo apt update
sudo apt install -y libzmq3-dev libreadline-dev g++ make
```

## Compilation

```bash
make
```

Produit deux binaires : `server` et `dbash`.

## Configuration

Éditer `config_servers.txt` avec les adresses des nœuds :

```
tcp://127.0.0.1:5000
tcp://127.0.0.1:5001
tcp://127.0.0.1:5002
tcp://127.0.0.1:5003
```

Sur de vraies machines, remplacer par les vraies IPs :
```
tcp://192.168.1.10:5000
tcp://192.168.1.11:5000
```

## Lancement

**Terminaux 1-4 — un serveur par nœud :**
```bash
./server 0
./server 1
./server 2
./server 3
```

**Attendre ~10 secondes** que les heartbeats s'échangent.

**Terminal 5 — le shell :**
```bash
./dbash 0
```

## Tests

### Commandes de base
```bash
dbash> ls
dbash> echo hello world
dbash> ps
```

### Background et wait
```bash
dbash> sleep 5 &
dbash> ps              # status: 0 (en cours)
# attendre 5 secondes
dbash> ps              # status: 1 (termine)
dbash> sleep 3 &
dbash> sleep 5 &
dbash> wait            # attend que tout soit fini
dbash> ps              # tout a status: 1
```

### Kill distant
```bash
dbash> sleep 30 &
dbash> ps              # noter le global_pid
dbash> kill -9 <global_pid>
dbash> ps              # status passe a 1
```

### Forcer la redirection (test load balancing)
Dans `proto-nicolas/server.cpp`, case `ClientCommand`, remplacer :
```cpp
float my_load = std::stof(getLoadAverage());
// par :
float my_load = 999.0f;
```
Recompiler server 0 uniquement — les commandes seront redirigées vers les autres nœuds.
Vérifier dans `ps` que `node_id != 0`.

## Architecture

```
[dbash] --(ClientCommand)--> [server local]
                                    |
                         seuil dynamique ?
                         ma_charge > moyenne * 1.5
                                    |
                    OUI                        NON
                     |                          |
            jitter 0-300ms               fork/exec local
            recalcul cible               broadcast_load()
                     |
            RemoteCommand --> [server cible]
                                    |
                              fork/exec
                              RemoteReturnValue
```

## Types de messages ZeroMQ

| Type | Valeur | Description |
|------|--------|-------------|
| Heartbeat | 1 | Broadcast charge toutes les 3s |
| ClientHandshake | 2 | Connexion initiale client |
| ClientAcknowledgement | 3 | OK handshake |
| ClientCommand | 4 | Lancer une commande |
| ClientReturnValue | 5 | Résultat + global_pid |
| RemoteCommand | 6 | Déléguer vers un autre nœud |
| RemoteReturnValue | 7 | Résultat depuis nœud distant |
| RemoteKill | 8 | Kill sur nœud distant |
| JobFinished | 9 | Job background terminé |
| RemoteJobFinished | 10 | Job bg distant terminé |

## Structure des fichiers

```
DBASH/
├── Makefile
├── config_servers.txt
├── cluster_state.hpp       # PeerInfo, Liveness, variables globales
├── message_types.hpp       # Enum des types de messages
├── proto-nicolas/
│   └── server.cpp          # Démon serveur
└── src/
    ├── main.cpp            # Point d'entrée dbash
    ├── repl.cpp/hpp        # Shell REPL + ZeroMQ client
    ├── parser.cpp/hpp      # Parser de commandes
    ├── job_table.cpp/hpp   # Table des processus
    └── load_balancer.cpp/hpp  # select_target, get_global_load
```

## Ce qui reste à implémenter

- **Tolérance aux fautes** — relancer les processus des nœuds morts (thème choisi)
- **Démarrage sans argument** — détecter automatiquement le node_id depuis l'IP locale
- **ps -l** — format long avec stats CPU/mémoire
- **Interface modification du cluster** — ajouter/retirer des nœuds dynamiquement
