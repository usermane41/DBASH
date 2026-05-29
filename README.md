# D-Bash

> Shell distribué à répartition de charge sur un cluster de machines — M1 SAR, Sorbonne Université

**Abdulrahim Atteib Doutoum** · Encadrant : Pierre Sens · 2025–2026

---

## Présentation

D-Bash est un shell distribué qui répartit automatiquement les commandes lancées par l'utilisateur sur le nœud du cluster le moins chargé. L'objectif est d'éviter que certaines machines soient surchargées pendant que d'autres restent inactives.

Chaque machine du cluster exécute un `server` qui coopère avec les autres pour évaluer l'état global du système et prendre les décisions de placement. L'utilisateur interagit via `dbash`, un shell client interactif qui se connecte au server local.

---

## Architecture

```
Cluster D-Bash
┌──────────────────┐     heartbeat / RemoteCommand      ┌──────────────────┐
│     Nœud 0       │ ◄─────────────────────────────────► │     Nœud 1       │
│                  │                                     │                  │
│  ┌────────────┐  │                                     │  ┌────────────┐  │
│  │   server   │  │                                     │  │   server   │  │
│  │            │  │                                     │  │            │  │
│  │ recv loop  │  │                                     │  │ recv loop  │  │
│  │ heartbeat  │  │                                     │  │ heartbeat  │  │
│  │ watchdog   │  │                                     │  │ watchdog   │  │
│  │ cpu_monitor│  │                                     │  │ cpu_monitor│  │
│  └────────────┘  │                                     │  └────────────┘  │
│                  │                                     │                  │
│  ┌────────────┐  │                                     │  ┌────────────┐  │
│  │   dbash    │  │                                     │  │   dbash    │  │
│  └────────────┘  │                                     │  └────────────┘  │
└──────────────────┘                                     └──────────────────┘
```

### Les 4 threads du server

| Thread | Rôle |
|--------|------|
| **receive loop** | Thread principal — seul à toucher les sockets ZeroMQ. Traite tous les messages via `zmq::poll`. |
| **heartbeat thread** | Diffuse la charge CPU toutes les 3s. Détecte les nœuds morts (timeout 10s). Notifie via `inproc://nodedead`. |
| **watchdog thread** | Bloqué sur `waitpid(-1)`. Détecte la fin des processus enfants. Notifie via `inproc://jobdone`. |
| **cpu_monitor thread** | Lit `/proc/stat` toutes les 200ms. Stocke l'usage dans `std::atomic<float>`. |

### Protocole de messages (ZeroMQ)

Chaque message = 2 frames : **type (1 octet)** + **payload (texte)**

| # | Type | Direction | Description |
|---|------|-----------|-------------|
| 1 | `Heartbeat` | server → server | Charge CPU + détection pannes |
| 2 | `ClientHandshake` | client → server | Connexion initiale |
| 3 | `ClientAcknowledgement` | server → client | Confirmation connexion |
| 4 | `ClientCommand` | client → server | Commande à exécuter |
| 5 | `ClientReturnValue` | server → client | global_pid + output |
| 6 | `RemoteCommand` | server → server | Redirection vers nœud moins chargé |
| 7 | `RemoteReturnValue` | server → server | global_pid du job background distant |
| 8 | `RemoteKill` | server → server | Signal distant : `node:sig:local_pid` |
| 9 | `JobFinished` | server → client | Notification async job background |
| 10 | `RemoteJobFinished` | server → server → client | Job distant terminé |
| 11 | `NodeDead` | server → client | Broadcasté à tous les clients |
| 12 | `Stop` | client → server | Ctrl+C — sans payload |

---

## Algorithmes

### Load balancing dynamique

```
si  ma_charge > charge_globale × 1.5  ET  nœud_disponible_existe
    → jitter aléatoire [0–300ms]
    → recalcul + select_target()
    → RemoteCommand → nœud cible
sinon
    → exécution locale
```

- **Seuil dynamique × 1.5** — s'adapte à l'état courant du cluster. Évite les redirections inutiles pour les jobs courts.
- **Jitter 0–300ms** — désynchronise les décisions simultanées (anti-thundering herd).
- **Recalcul post-jitter** — réévalue la cible après le délai pour tenir compte des changements.
- **Approche client** — c'est la machine surchargée qui recherche activement une cible disponible.

### Évaluation de la charge

`/proc/stat` est utilisé à la place de `/proc/loadavg` (latence 20–30s) :

```
usage = 1 − Δidle / Δtotal     (fenêtre de 200ms)
```

La valeur est stockée dans un `std::atomic<float>` — lecture directe sans mutex depuis le receive loop.

`broadcast_load()` est appelé après chaque `fork_and_exec()` pour propager la nouvelle charge immédiatement, sans attendre le prochain heartbeat (3s).

### Tolérance aux fautes

1. Absence de heartbeat pendant > 10s → nœud marqué `Dead`
2. heartbeat thread → `inproc://nodedead` → receive loop
3. `NodeDead [11]` broadcasté à tous les clients connectés
4. Chaque client filtre sa `job_table` — jobs du nœud mort marqués `failed=true`
5. `execute_command()` relancé en background → load balancer choisit un nœud survivant

---

## Fonctionnalités

| Commande | Description |
|----------|-------------|
| `<cmd>` | Lancement foreground sur la machine la moins chargée |
| `<cmd> &` | Lancement background — retourne le `global_pid` immédiatement |
| `ps` | Liste tous les jobs lancés avec nœud, statut et global_pid |
| `wait` | Bloque jusqu'à la fin de tous les jobs background |
| `kill -<sig> <gpid>` | Envoie un signal — local ou distant via RemoteKill |
| `Ctrl+C` | Interrompt le job foreground en cours (local ou distant) |
| `exit` | Quitte dbash |

### Global PID

```
global_pid = (node_id << 16) | local_pid
```

Décode sans table centralisée :
```cpp
node_id   = global_pid >> 16
local_pid = global_pid & 0xFFFF
```

> ⚠️ **Limitation** : déborde si PID Unix > 65535 (Linux moderne : `pid_max = 4194304`). Fix : passer en `uint64_t` avec `node_id << 32`.

---

## Benchmark

Mesuré sur 8 machines PPTI — Sorbonne Université.

| Métrique | Valeur |
|----------|--------|
| Speedup max mesuré | **4.93×** @96 jobs — range(10⁸) |
| Speedup théorique max | ~6.5× (seuil × 1.5 retient ~19% des jobs) |
| Efficacité pratique | ~80% |
| Gain /proc/stat vs loadavg | **+62%** à 20 jobs |
| Point de bascule range(10⁸) | ~16 jobs (~6s/job) |
| Point de bascule range(10⁷) | ~42 jobs (~0.6s/job) |

D-Bash est un **répartiteur de charge**, pas un diviseur du temps par N. Entre 1 et 200 jobs, bash est multiplié par 45× tandis que D-Bash ne l'est que par 8.4×.

---

## Installation

### Dépendances

```bash
sudo apt update
sudo apt install -y libzmq3-dev libreadline-dev
```

### Compilation

```bash
# Server
g++ -std=c++17 server.cpp -o server -lzmq -pthread -Wno-unused-result

# Client
g++ -std=c++17 dbash.cpp repl.cpp parser.cpp job_table.cpp load_balancer.cpp \
    -o dbash -lzmq -lreadline -pthread -Wno-unused-result
```

### Configuration

Éditer `config_servers.txt` — une adresse TCP par ligne, une par nœud :

```
tcp://192.168.1.1:5000
tcp://192.168.1.2:5000
tcp://192.168.1.3:5000
```

---

## Lancement

Sur chaque nœud, lancer le server avec son index (0-indexé) :

```bash
# Nœud 0
./server 0

# Nœud 1
./server 1
```

Puis lancer le client sur n'importe quel nœud :

```bash
./dbash 0   # se connecte au server du nœud 0
```

### Exemple d'utilisation

```
dbash> sleep 5 &
[background] global_pid: 65548

dbash> python3 script.py
[output de script.py — exécuté sur le nœud le moins chargé]

dbash> ps
global_pid   node   command    status
65548        1      sleep 5    running

dbash> wait
[done] sleep 5 (pid 65548)

dbash> kill -9 65548
dbash> exit
Bye.
```

---

## Structure du projet

```
.
├── server.cpp          # Daemon serveur — 4 threads, receive loop, load balancer
├── dbash.cpp           # Point d'entrée client
├── repl.cpp            # REPL interactif — readline, built-ins, gestion fg/bg
├── parser.cpp          # Parsing des commandes
├── job_table.cpp       # Table des jobs — global_pid, statut, nœud
├── load_balancer.cpp   # select_target(), get_global_load()
├── message_types.hpp   # Enum des 12 types de messages
└── config_servers.txt  # Adresses TCP du cluster
```

---

## Thème choisi

**Tolérance aux fautes** — détection des pannes machines et relance automatique des processus des nœuds défaillants.

---

*Projet SAR — Master Informatique M1 — Sorbonne Université — 2025–2026*
