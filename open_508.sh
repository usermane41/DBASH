#!/bin/bash

# Connexion distante aux machines de la salle 508 - Jussieu PPTI
# Usage: ./open_508.sh [login]
# Exemple: ./open_508.sh 21201780

LOGIN=${1:-"21201780"}
GATEWAY="ssh.ufr-info-p6.jussieu.fr"
DOMAIN="ufr-info-p6.jussieu.fr"
MACHINES=(01 05 07 08 15 16)

# Détecte l'émulateur de terminal disponible
detect_terminal() {
    for term in gnome-terminal xterm konsole xfce4-terminal lxterminal tilix; do
        if command -v "$term" &>/dev/null; then
            echo "$term"
            return
        fi
    done
    echo "none"
}

TERM_EMU=$(detect_terminal)

if [ "$TERM_EMU" = "none" ]; then
    echo "❌ Aucun émulateur de terminal graphique trouvé."
    echo "   Installe xterm : sudo apt install xterm"
    exit 1
fi

echo "🖥️  Ouverture des terminaux SSH vers la salle 508..."
echo "   Login    : $LOGIN"
echo "   Terminal : $TERM_EMU"
echo "   Machines : ${MACHINES[*]}"
echo ""

for NUM in "${MACHINES[@]}"; do
    HOST="ppti-14-508-${NUM}.${DOMAIN}"
    CMD="ssh -tt -o StrictHostKeyChecking=accept-new -o ProxyJump=${LOGIN}@${GATEWAY} ${LOGIN}@${HOST}"
    TITLE="508-${NUM}"

    echo "  → Ouverture de $HOST"

    case "$TERM_EMU" in
        gnome-terminal)
            gnome-terminal --title="$TITLE" -- bash -c "$CMD; exec bash" &
            ;;
        xterm)
            xterm -title "$TITLE" -e bash -c "$CMD; exec bash" &
            ;;
        konsole)
            konsole --new-tab -p tabtitle="$TITLE" -e bash -c "$CMD; exec bash" &
            ;;
        xfce4-terminal)
            xfce4-terminal --title="$TITLE" -e "bash -c '$CMD; exec bash'" &
            ;;
        lxterminal)
            lxterminal --title="$TITLE" -e "bash -c '$CMD; exec bash'" &
            ;;
        tilix)
            tilix -t "$TITLE" -e "bash -c '$CMD; exec bash'" &
            ;;
    esac

    sleep 0.3  # petit délai pour éviter de spammer la passerelle
done

echo ""
echo "✅ ${#MACHINES[@]} terminaux lancés."
echo "   Entre ton mot de passe pour chaque fenêtre."
