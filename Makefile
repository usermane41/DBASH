CXX      = g++
LOCAL    = /users/Etu0/21201780/.local
CXXFLAGS = -std=c++17 -Wall -Wextra -g -Iclient_src -I. -I$(LOCAL)/include -I/usr/local/anaconda3/include
LDFLAGS  = -L$(LOCAL)/lib -lreadline -lncurses -lpthread -lzmq -static-libstdc++ -Wl,-rpath,$(LOCAL)/lib -Wl,-rpath,/usr/lib/x86_64-linux-gnu
# Sources et objets pour le Client (anciennement SHELL)
SHELL_SRC = client_src/main.cpp client_src/repl.cpp client_src/parser.cpp client_src/job_table.cpp client_src/load_balancer.cpp
SHELL_OBJ = $(SHELL_SRC:.cpp=.o)
SHELL_BIN = dbash

# Sources et objets pour le Serveur
SERVER_SRC = server_src/server.cpp client_src/load_balancer.cpp
SERVER_OBJ = $(SERVER_SRC:.cpp=.o)
SERVER_BIN = server

.PHONY: all clean

all: $(SHELL_BIN) $(SERVER_BIN)

$(SHELL_BIN): $(SHELL_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(SERVER_BIN): $(SERVER_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Règle générique pour la compilation des fichiers objets
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f $(SHELL_OBJ) $(SERVER_OBJ) $(SHELL_BIN) $(SERVER_BIN)