#!/usr/bin/env bash
###
### Ce fichier a été fait par l'IA, il sert uniquement à lancer la plateforme complète (simulateur, pipeline Spark, dashboard) en une seule commande.
###

set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"
LOGS="$ROOT/logs"
PIDS=()

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Cleanup on Ctrl+C ────────────────────────────────────────────────────────
cleanup() {
  echo ""
  info "Arrêt de tous les processus..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null && info "Process $pid stoppé." || true
  done
  exit 0
}
trap cleanup SIGINT SIGTERM

# ── 1. Prérequis ─────────────────────────────────────────────────────────────
info "Vérification des prérequis..."

command -v java  >/dev/null 2>&1 || error "Java non trouvé. Installez Java 11+."
command -v python3 >/dev/null 2>&1 || error "Python3 non trouvé."

JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
[ "$JAVA_VER" -ge 11 ] 2>/dev/null || warn "Java $JAVA_VER détecté — Java 11+ recommandé."

info "Java : $(java -version 2>&1 | head -1)"
info "Python : $(python3 --version)"

# ── 2. Virtualenv ─────────────────────────────────────────────────────────────
if [ ! -d "$VENV" ]; then
  info "Création du virtualenv..."
  python3 -m venv "$VENV"
fi

# shellcheck source=/dev/null
source "$VENV/bin/activate"
info "Virtualenv activé : $VENV"

# ── 3. Dépendances ────────────────────────────────────────────────────────────
info "Installation des dépendances..."
pip install --quiet --upgrade pip
pip install --quiet -r "$ROOT/requirements.txt"
info "Dépendances OK."

# ── 4. Répertoires runtime ───────────────────────────────────────────────────
mkdir -p "$ROOT/data/events" "$ROOT/checkpoints" "$LOGS"

export PYTHONPATH="$ROOT"

# ── 5. Lancement des composants ───────────────────────────────────────────────
info "Démarrage du simulateur..."
python3 -m simulator.event_producer \
  > "$LOGS/simulator.log" 2>&1 &
PIDS+=($!)
info "Simulateur démarré (PID ${PIDS[-1]}) → logs/simulator.log"

info "Démarrage de la pipeline Spark..."
python3 -m pipeline.streaming \
  > "$LOGS/pipeline.log" 2>&1 &
PIDS+=($!)
info "Pipeline démarrée (PID ${PIDS[-1]}) → logs/pipeline.log"

# Laisser Spark s'initialiser avant le dashboard
sleep 5

info "Démarrage du dashboard Streamlit..."
streamlit run "$ROOT/dashboard/app.py" \
  --server.headless true \
  > "$LOGS/dashboard.log" 2>&1 &
PIDS+=($!)
info "Dashboard démarré (PID ${PIDS[-1]}) → http://localhost:8501"

# ── 6. Attente ────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Plateforme démarrée. Appuyez sur Ctrl+C pour tout arrêter.${NC}"
echo -e "${GREEN}  Dashboard → http://localhost:8501${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

wait
