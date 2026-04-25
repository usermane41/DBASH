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

std::vector<char*> Command::argv() const
{
    std::vector<char*> v;
    v.reserve(args.size() + 1);
    for (const auto& s : args)
        v.push_back(const_cast<char*>(s.c_str()));
    v.push_back(nullptr);
    return v;
}
