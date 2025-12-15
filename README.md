# 🏎️ Pitwall Preditivo F1

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![PyCaret](https://img.shields.io/badge/PyCaret-AutoML-orange?style=for-the-badge)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow?style=for-the-badge&logo=amazon-s3&logoColor=white)
![Medallion Architecture](https://img.shields.io/badge/Data%20Architecture-Medallion-green?style=for-the-badge)

> **Predictive analytics and strategic intelligence for Formula 1.**  
> Este projeto utiliza técnicas avançadas de Machine Learning e Engenharia de Dados para prever posições finais de corrida, analisar riscos e apoiar tomadas de decisão estratégicas no pitwall.

---

## 📌 Visão Geral

O **Pitwall Preditivo F1** é uma solução completa de *Data Science* aplicada ao automobilismo. Ele ingere dados brutos, processa-os através de uma arquitetura medalhão (Bronze, Silver, Gold) e aplica modelos de Machine Learning para entregar inteligência competitiva.

### 🚀 Principais Funcionalidades

*   **Previsão de Posição Final**: Modelos preditivos (Random Forest & AutoML) que estimam a posição de chegada baseados em grid, telemetria e clima.
*   **Análise de Risco (Risk Bands)**: Notebooks dedicados para calcular faixas de risco e faixas de performance esperada para cada piloto.
*   **AutoML com PyCaret**: Comparação automática de dezenas de algoritmos para garantir a melhor escolha modelagem.
*   **Interface Web (Streamlit/App)**: Dashboard interativo para visualização de resultados e simulações.

---

## 🏗️ Arquitetura do Projeto

O projeto segue a **Arquitetura Medalhão** (Bronze -> Silver -> Gold), garantindo qualidade e rastreabilidade dos dados.

| Camada | Descrição | Localização (S3/Local) |
| :--- | :--- | :--- |
| **Bronze** | Dados brutos ingeridos de APIs e fontes externas (JSON/CSV). | `s3://{BUCKET}/bronze` |
| **Silver** | Dados limpos, tipados e convertidos para formatos otimizados (Parquet). | `s3://{BUCKET}/silver` |
| **Gold** | Dados agregados e prontos para ML (Features Engineering aplicada). | `s3://{BUCKET}/gold` |

---

## 🛠️ Tecnologias Utilizadas

*   **Linguagem**: Python 3.9+
*   **Manipulação de Dados**: Pandas, NumPy
*   **Machine Learning**: Scikit-Learn, **PyCaret** (AutoML)
*   **Cloud & Storage**: AWS S3, Boto3
*   **Visualização**: Matplotlib, Seaborn

---

## 📂 Estrutura de Pastas

```bash
pitwall-preditivo-f1/
├── notebooks/          # Notebooks Jupyter para análise e experimentação
│   ├── automl_comparison.ipynb   # Comparação de modelos com PyCaret
│   ├── risk_analysis_f1.ipynb    # Análise de Faixa de Risco (2023-2025)
│   └── ...
├── scripts/            # Scripts de execução e automação
│   ├── train_model.py  # Treinamento do modelo principal (Random Forest)
│   └── ...
├── src/                # Código fonte da aplicação e processamento
│   ├── processing/     # Pipelines de ETL e Engenharia de Features
│   ├── inference/      # Módulos de inferência e predição
│   └── web_app/        # Interface Web
├── data/               # Dados locais (amostras)
├── .env                # Variáveis de ambiente (Credenciais AWS)
└── requirements.txt    # Dependências do projeto
```

---

## 🚀 Como Executar

### 1. Pré-requisitos
Certifique-se de ter o Python instalado e um ambiente virtual configurado. Variáveis de ambiente da AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET_NAME`) devem estar no arquivo `.env`.

### 2. Instalação
```bash
# Clone o repositório
git clone https://github.com/seu-usuario/pitwall-preditivo-f1.git

# Acesse a pasta
cd pitwall-preditivo-f1

# Crie e ative o ambiente virtual
python3 -m venv .venv
source .venv/bin/activate

# Instale as dependências
pip install -r requirements.txt
```

### 3. Rodando as Análises (Notebooks)
Para executar a análise de risco ou comparar modelos:

```bash
# Inicie o Jupyter Notebook
jupyter notebook
```
> Abra `notebooks/risk_analysis_f1.ipynb` para ver a lógica de Faixa de Risco validada com dados de 2023-2025.

### 4. Treinamento de Modelo
Para rodar o pipeline de treinamento principal:
```bash
python scripts/train_model.py
```

---

## 🧠 Modelagem e Resultados

### Seleção de Modelo (AutoML)
Utilizamos o **PyCaret** para selecionar o algoritmo mais eficaz. Embora o *Extra Trees* tenha mostrado alto desempenho, o **Random Forest Regressor** foi o escolhido para produção devido à sua robustez e explicabilidade (~80% R²).

### Análise de Risco
Os dados foram divididos estrategicamente para evitar *data leakage* e garantir previsões realistas:
*   **Treino**: Temporadas 2023 e 2024.
*   **Teste (Simulação)**: Temporada 2025.

---

## ✨ Autores

*   **Italo Rufca** - *Lead Data Scientist & Developer*

---
> 🏎️ *Data Science acelerando na velocidade da F1.*
