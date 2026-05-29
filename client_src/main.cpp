#include <iostream>
#include "repl.hpp"
#include "job_table.hpp"     
int my_node_id;
int main(int argc, char* argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <node_id>\n";
        return 1;
    }
    int node_id = std::stoi(argv[1]);
    my_node_id = node_id; 
    std::cout << "D-bash — shell réparti\n";
    std::cout << "Tape 'exit' ou Ctrl+D pour quitter.\n\n";
    
    repl_run(node_id);
    return 0;
}