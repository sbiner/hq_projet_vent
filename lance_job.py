import sys
import logging
from outils_era5_era5l import fait_stat_mens_vit_vent_era5_et_era5l

# Configuration du logging pour envoyer la sortie dans un fichier log
logging.basicConfig(
    filename='output.log',  # Nom du fichier log
    level=logging.INFO,     # Niveau de logging (INFO pour capturer les prints et erreurs)
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'            # 'w' pour écraser le fichier à chaque exécution, 'a' pour ajouter
)

# Rediriger stdout et stderr vers le logger
class StreamToLogger:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass

logging.info("Début de l'exécution de la fonction fait_stat_mens_vit_vent_era5_et_era5l")

# Rediriger stdout et stderr
sys.stdout = StreamToLogger(logging.getLogger(), logging.INFO)
sys.stderr = StreamToLogger(logging.getLogger(), logging.ERROR)

try:
    # Lancer la fonction du modèle
    fait_stat_mens_vit_vent_era5_et_era5l(source="era5", hauteur=100)
    logging.info("Exécution terminée avec succès")
except Exception as e:
    logging.error(f"Erreur lors de l'exécution : {str(e)}")
    raise