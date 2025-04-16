import os
import subprocess
import sys

print("Lancement automatique de Streamlit...")

# Vérifie que Streamlit n'est pas déjà en cours
if "streamlit" not in sys.argv:
    script_path = os.path.abspath("app.py")  # Chemin du fichier app.py
    subprocess.run([sys.executable, "-m", "streamlit", "run", script_path], check=True)
