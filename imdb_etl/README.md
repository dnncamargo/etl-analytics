# IMDB ETL Analytics

Este projeto realiza o processo de ETL (Extract, Transform, Load) utilizando dados abertos do IMDB. Siga as instruções abaixo para configurar e executar o projeto.

---

## Configuração do Ambiente Virtual

1. **Execute o script de configuração**:
   O script `setup.py` cria o ambiente virtual e instala as dependências necessárias. Execute o seguinte comando:
   ```bash
   python setup.py

2. **Ativação do Ambiente Virtual** 
   Após a criação do ambiente virtual, ative-o usando o comando abaixo:

### Linux/Mac
```bash
source env/bin/activate
```

### Windows
```bash
env\Scripts\activate
```

## Instalação das Dependências
Caso as dependências não tenham sido instaladas automaticamente, execute:

```bash
pip install -r requirements.txt
```

## Scripts Disponíveis
`script.py`: Esta é a versão mais recente e deve ser utilizada para processamento de dados. Ela contém todas as atualizações e correções de bugs.

`script_v1_legacy.py`: Esta é uma versão antiga, mantida apenas para referência histórica. Não recomendamos o uso desta versão.

## Como Executar
Para executar o script mais recente, use o seguinte comando:
```bash
python script.py
```

O script fará o download dos dados, processará o ETL e gerará os resultados.

## Estrutura do Projeto
```bash
etl-analytics/
├── env/                  # Pasta do ambiente virtual (não versionada)
├── data/                 # Pasta para armazenar dados baixados e processados
├── script.py             # Script principal (versão mais recente)
├── script_v1_legacy.py   # Versão antiga do script (apenas para referência)
├── setup.py              # Script para configurar o ambiente virtual
├── requirements.txt      # Lista de dependências
├── README.md             # Este arquivo
```

## Observações
Certifique-se de que o ambiente virtual está ativado antes de executar o script.

Se você encontrar problemas, consulte a seção de Issues ou abra uma nova issue.

## Contribuição
Contribuições são bem-vindas! Siga as etapas abaixo:

Faça um fork do projeto.

Crie uma branch para sua feature (`git checkout -b feature/nova-feature`).

Commit suas mudanças (`git commit -m 'Adiciona nova feature'`).

Faça push para a branch (`git push origin feature/nova-feature`).

Abra um Pull Request.

## Licença
Este projeto está licenciado sob a MIT License.
