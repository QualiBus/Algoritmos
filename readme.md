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

   Prepare um arquivo `schema.txt` contendo o esquema no seguinte formato:

        coluna_1:tipo_coluna_1

        ...

        coluna_i:tipo_coluna_i
   
   Sendo o primeiro campo para o nome da coluna no schema do seu arquivo `.csv` e o segundo para seu respectivo tipo, faça isso para todas as colunas do schema.
   
2. **Modificação do campo da direita do dicionário meus_campos**

Altere os valores da direita do dicionário para os valores correspondentes no seu schema, no exemplo, devem ser alterados: "codigo_linha" e "velocidade".

        meus_campos = {
        "line_code": "codigo_linha",
        "bus_speed": "velocidade",
        }
Neste caso, em cada um dos códigos que necessitam desses campos, você encontrará um dicionário chamado `meus_campos`, que deverá ter os valores à esquerda preenchidos com os nomes dos respectivos campos no seu schema.

3. **Definição do caminho para o arquivo de dados**

Em cada módulo de análise, insira o **caminho para o arquivo `.csv`** contendo os dados de ônibus na linha de leitura apropriada, colocando o caminho correto para os dados que deseja utilizar na análise.

4. **Execução com Apache Spark**

Com o Apache Spark instalado, execute o módulo desejado usando o comando, podendo serem feitas otimizações e adaptações do comando para o seu ambiente de execução do Spark:

```bash
spark-submit nome_do_codigo.py
