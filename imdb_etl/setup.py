import os
import subprocess
import sys

# Nome da pasta do ambiente virtual
VENV_NAME = "env"

# Verifica se o ambiente virtual já existe
if not os.path.exists(VENV_NAME):
    print(f"Criando ambiente virtual '{VENV_NAME}'...")
    subprocess.run([sys.executable, "-m", "venv", VENV_NAME])
else:
    print(f"Ambiente virtual '{VENV_NAME}' já existe.")

# Ativa o ambiente virtual e instala as dependências
if os.name == "nt":  # Windows
    activate_script = os.path.join(VENV_NAME, "Scripts", "activate")
    pip_executable = os.path.join(VENV_NAME, "Scripts", "pip")
else:  # Linux/Mac
    activate_script = os.path.join(VENV_NAME, "bin", "activate")
    pip_executable = os.path.join(VENV_NAME, "bin", "pip")

print(f"Instalando dependências a partir de 'requirements.txt'...")
subprocess.run([pip_executable, "install", "-r", "requirements.txt"])

print("Configuração concluída. Ambiente virtual pronto para uso.")