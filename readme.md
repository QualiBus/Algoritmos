# Framework **QualiBus**

**QualiBus** é um framework que oferece quatro módulos dedicados à análise da qualidade de dados de transporte coletivo:

- **Acurácia temática**  
- **Completude**  
- **Consistência lógica**  
- **Qualidade temporal**

Para executar os módulos, o usuário deve garantir que **Python 3** e **Apache Spark** estejam instalados no ambiente.

---

### Etapas para utilização dos módulos:

1. **Carregamento do esquema de dados**

   Prepare um arquivo `.txt` contendo o esquema no seguinte formato:

        coluna_1:tipo_coluna_1

        ...

        coluna_i:tipo_coluna_i


2. **Definição do caminho para o arquivo de dados**

Em cada módulo de análise, insira o **caminho para o arquivo `.csv`** contendo os dados de transporte coletivo na linha de leitura apropriada.

3. **Execução com Apache Spark**

Com o Apache Spark instalado, execute o módulo desejado usando o comando:

```bash
spark-submit nome_do_codigo.py
