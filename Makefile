CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -g -Isrc -I.
LDFLAGS  = -L./lib -lreadline -lpthread -lzmq -Wl,-rpath,'$$ORIGIN/lib'

# ---- Shell (dbash) ----
SHELL_SRC = src/main.cpp src/repl.cpp src/parser.cpp src/job_table.cpp src/load_balancer.cpp
SHELL_OBJ = $(SHELL_SRC:.cpp=.o)
SHELL_BIN = dbash

# ---- Serveur daemon ----
SERVER_SRC = proto-nicolas/server.cpp src/load_balancer.cpp
SERVER_OBJ = $(SERVER_SRC:.cpp=.o)
SERVER_BIN = server

.PHONY: all clean

all: $(SHELL_BIN) $(SERVER_BIN)

$(SHELL_BIN): $(SHELL_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(SERVER_BIN): $(SERVER_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f $(SHELL_OBJ) $(SERVER_OBJ) $(SHELL_BIN) $(SERVER_BIN)
