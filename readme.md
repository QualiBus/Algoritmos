# **QualiBus** Framework

**QualiBus** is a framework that offers four modules dedicated to analyzing the quality of bus data:

-   **Thematic Accuracy**
-   **Completeness**
-   **Logical Consistency**
-   **Temporal Quality**

To run the modules, the user must ensure that **Python 3** and **Apache Spark** are installed in their environment.

---

### Steps to use the modules:

1.  **Loading the data schema**

    Prepare a `schema.txt` file containing the schema in the following format:

        column_1:column_type_1
        ...
        column_i:column_type_i

    The first field is for the column name in your `.csv` file's schema, and the second is for its respective type. Do this for all columns in the schema. Remember to use the official Apache Spark data types (e.g., Integer, String, Timestamp, Double).

2.  **Modify the values in the `meus_campos` dictionary**

    Navigate to the code within each module. Depending on the script, you will find the `meus_campos` dictionary, which is used to map certain variables for the scripts' execution. Change the dictionary values to the corresponding names in your schema. In the first example, "codigo_linha" and "velocidade" should be changed to the respective names of these columns in your `.csv` file.

    ```python
    meus_campos = {
        "line_code": "codigo_linha",
        "bus_speed": "velocidade",
    }
    
    meus_campos = {
        "line_code": "cl",
        "bus_direction": "sl",
    }
    ```

    The second example shows a real-world scenario, replacing the values with the corresponding column names from the SPTRANS API for `line_code` and `bus_direction`, which are "cl" and "sl", respectively. If a user were using data from this API, this substitution would be made in the dictionary.

3.  **Set the path to the data file**

    In each analysis module's code, insert the **path to the `.csv` file** containing the bus data in the appropriate read line, providing the correct path for the data you wish to analyze.

4.  **Execution with Apache Spark**

    ```bash
    spark-submit your_script_name.py
    ```

    With Apache Spark installed, run the desired module using the command above. You can make optimizations and adaptations to the command for your Spark execution environment. For example, if you want to run the script for counting null speeds in a local environment, you should run the following in your terminal:

    ```bash
    spark-submit --master "local[*]" count_null_speeds.py
    ```



