# 🌦️ Weather Data Integration

Este é um projeto de Engenharia de Dados ponta a ponta construído em **Python** e implantado na plataforma **Databricks**. O projeto realiza a ingestão, transformação e qualidade de dados climáticos utilizando uma arquitetura medalhão (Bronze, Silver e Gold).

A infraestrutura e o ciclo de vida do código são totalmente automatizados utilizando **Databricks Asset Bundles (DABs)** e pipelines de **CI/CD** para ambientes de Desenvolvimento (DEV) e Produção (PROD).

---

## 🚀 Tecnologias Utilizadas

* **Databricks Asset Bundles (DABs):** Gerenciamento e deploy da infraestrutura como código (IaC).
* **PySpark / Delta Lake:** Processamento distribuído de dados e armazenamento confiável com transações ACID.
* **Unity Catalog:** Governança de dados, gerenciamento de catálogos e esquemas isolados por ambiente.
* **Python Wheel Packaging:** Empacotamento de códigos e funções utilitárias personalizadas para reutilização nos Jobs.
* **GitHub Actions:** Automação de testes e deploy contínuo (CI/CD) nos workspaces do Databricks.
* **Dev Containers:** Ambiente de desenvolvimento local isolado e padronizado em Docker.

---

## 📐 Arquitetura de Dados (Medalhão)

O pipeline de dados é orquestrado em etapas sequenciais para garantir a qualidade da informação:

1. **Environment Validation:** Garante que os catálogos e esquemas existam antes do início da execução.
2. **Incremental Bronze:** Ingestão dos dados brutos com histórico completo.
3. **Incremental Silver:** Limpeza, tipagem de dados e filtragem de registros inválidos que são direcionados para uma tabela de *Quarantine* (Quarentena).
4. **Data Quality Metrics:** Validação de regras de negócio essenciais.
5. **Incremental Gold:** Criação de agregados prontos para consumo por áreas de negócio e ferramentas de BI.
6. **Smoke Test:** Validação final para garantir que os dados novos foram processados corretamente.

---

## 📁 Estrutura do Projeto

```text
├── .devcontainer/          # Configuração do ambiente local isolado em Docker
├── .github/workflows/      # Pipelines de CI/CD (GitHub Actions) para DEV e PROD
├── architecture/           # Documentações e diagramas da arquitetura do projeto
├── databricks/
│   └── resources/          # Definição dos Jobs e Pipelines do Databricks em YAML
├── src/                    # Código fonte do projeto (.py, notebooks e pacotes customizados)
├── databricks.yml          # Arquivo de configuração principal do Bundle (DABs)
└── README.md               # Documentação do projeto
```

---

## 🛠️ Como Executar o Projeto Localmente

### Pré-requisitos
* Docker e VS Code instalados (com a extensão *Dev Containers*).
* Databricks CLI configurado na sua máquina.

### 1. Iniciar o Ambiente de Desenvolvimento
Abra a pasta do projeto no VS Code e clique em **"Reopen in Container"**. O ambiente vai instalar automaticamente o Python, o Databricks CLI e todas as dependências necessárias de forma isolada.

### 2. Validar as Configurações do Bundle
Antes de enviar os arquivos para a nuvem, verifique se a sintaxe do arquivo `databricks.yml` e das variáveis está correta:
```bash
databricks bundle validate --target dev
```

### 3. Realizar o Deploy para o Databricks
Para compilar o seu código (incluindo o pacote Python Wheel) e criar o Job orquestrado no Databricks, execute:
```bash
databricks bundle deploy --target dev
```

---

## 🔄 Pipeline de CI/CD (Automação)

O projeto conta com automação via GitHub Actions configurada no diretório `.github/workflows/`:

* **Ambiente de DEV:** Ativado automaticamente a cada *Pull Request* ou *Push* para a branch de desenvolvimento. Realiza testes automatizados e atualiza o bundle de testes.
* **Ambiente de PROD:** Ativado apenas quando mudanças são consolidadas na branch principal (`main`). Garante que as variáveis apontem para o catálogo produtivo (`prod_catalog`) e remove o modo de desenvolvimento do Job.

---

## ⚙️ Variáveis de Ambiente e Configuração

O arquivo `databricks.yml` gerencia o isolamento de dados injetando variáveis dinâmicas em tempo de execução:

| Variável | Padrão (DEV) | Padrão (PROD) | Descrição |
| :--- | :--- | :--- | :--- |
| `catalog` | `dev_catalog` | `prod_catalog` | Catálogo do Unity Catalog utilizado. |
| `schema` | `weather_dev` | `weather_prod` | Banco de dados / Esquema da aplicação. |
