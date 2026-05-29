#include "parser.hpp"
#include <sstream>
#include <algorithm>

Command parse_line(const std::string& line)
{
    Command cmd;

    std::istringstream iss(line);
    std::string token;
    while (iss >> token)
        cmd.args.push_back(token);

    if (cmd.empty()) return cmd;

    // Ligne commentaire
    if (cmd.args[0][0] == '#') {
        cmd.args.clear();
        return cmd;
    }

    // Détecter le '&' final
    if (cmd.args.back() == "&") {
        cmd.background = true;
        cmd.args.pop_back();
    }

    return cmd;
}
//pour passer de c++ a c (xecvp exige un tableau de pointeurs sur des caractères
//et pas un std::vector
std::vector<char*> Command::argv() const
{
    std::vector<char*> v;
    v.reserve(args.size() + 1);
    for (const auto& s : args)
        v.push_back(const_cast<char*>(s.c_str()));
    v.push_back(nullptr);
    return v;
}
//v.push_back(const_cast<char*>(s.c_str()));
//on extrait de manière brut la chaine de caractère pour quel 
//soit compatible en c 
//ensuite on cast le pointeur en lecteure seul qu'on recoit
//car les fonctions exec de Linux demandent un tableau de char* 
//modifiables (même si elles n'y touchent pas vraiment). Le const_cast retire la sécurité const 
//pour faire plaisir à l'appel système.