#pragma once

#include <string>
#include <vector>

/*
 * Résultat du parsing d'une ligne de commande.
 */
struct Command {
    std::vector<std::string> args;  // args[0] = nom de la commande
    bool background = false;        // true si la commande se termine par '&'

    // Construit le tableau argv[] null-terminated pour execvp()
    std::vector<char*> argv() const;

    bool empty() const { return args.empty(); }
    const std::string& name() const { return args[0]; }
};

/*
 * Parse une ligne et retourne une Command.
 * Retourne une Command vide si la ligne est vide/commentaire.
 */
Command parse_line(const std::string& line);
