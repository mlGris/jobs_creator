import asyncio
import sys
import os  

json = {
    "tables": [
        {
            "datasourse_type": "", # Тип источника данных
            "kafka": "", # Ссылка на кафку
            "scheme": "", # Схема, в которой будут созданы таблицы
            "name": "",  # Алиас название таблицы
            "columns": [ # Cписок необходимых полей с ограничениями
                {"alias": "", "column_name": "", "constraints": []}, # Алиас, Название поля в источнике, ограничения
            ],
            "from": "", # Основная таблица
            "where": """
                where 
                """, # Условия
            "joins": [ # Список джойнов
                {
                    "join_name": "", # Алиас название джойна
                    "join_type": "", # Тип джойна
                    "join_path": "", # Таблица для присоединения
                    "join_on": """
                            """ # Условия присоединения
                },
            ]
        },
    ]       
}

class Creator:
    def __init__(self, json):
        self.json = json       

    async def get_select(self):
        columns = ""
        columns_aliases = ""
        primary_column = ""
        for column in self.columns_list:
            constraints = ""
            for constraint in column["constraints"]:
                constraints = constraints + f"{constraint}, "
            if column["alias"] == "primary":
                primary_column = column["column_name"]
                self.primary_column_alias = f"'{primary_column.split(".")[0]}Id'"
                self.column_names_for_mapping.append(
                        f"        - {self.primary_column_alias.lower().replace("'","")}: [ {constraints} jsonPath={self.table_name}.{self.primary_column_alias.replace("'","")} ]"
                    )
            else:
                columns = columns + f",\n        {column["column_name"]}"
                if column["alias"]:
                    self.column_names_for_mapping.append(
                        f"        - {column["alias"].lower()}: [ {constraints} jsonPath={self.table_name}.{column["alias"]} ]"
                    )
                    columns_aliases = columns_aliases + f",\n        '{column["alias"]}'"
                else:
                    column_name = column["column_name"].split('.')[-1]
                    column_name = column_name.replace(column_name[0],column_name[0].upper())
                    self.column_names_for_mapping.append(
                        f"        - {column_name.lower()}: [ {constraints} jsonPath={self.table_name}.{column_name} ]"
                    )
                    columns_aliases = columns_aliases + f",\n        '{column_name}'"
        sql_select = f"""
select
    {primary_column} as id,
    renameRowFields((
        {primary_column}{columns}
    ),
    array[
        {self.primary_column_alias}{columns_aliases} 
    ]) as {self.table_name}
from `{self.table_from}` as {self.table_name}"""

        return sql_select
    
    async def get_joins(self, table):
        sql_joins = ""

        for join in table["joins"]:
            join_type = join["join_type"]
            join_path = join["join_path"]
            join_name = join["join_name"]
            join_on = join["join_on"]

            sql_join = f"""
{join_type} join {join_path} as {join_name}
    on {join_on}
                    """
            
            sql_joins = (sql_joins + sql_join)
        return sql_joins

    async def get_ddl(self):
        table_name = self.table_name.lower()
        ddl_columns = ""
        for column in self.columns_list:
            constraints = ""
            column_name = ""
            for constraint in column["constraints"]:
                if "primary" == constraint:
                    constraints = constraints 
                elif "notNull" == constraint:
                    constraints = constraints + " not null"
                else:
                    constraints = constraints + f" {constraint}"
            if "notNull" not in column["constraints"]:
                constraints = constraints + " null"
            if column["alias"] == "primary":
                column_name = self.primary_column_alias.replace("'","")
            elif column["alias"]:
                column_name = column["alias"]
            else:
                column_name = column["column_name"].split('.')[-1]
            ddl_columns = ddl_columns + f"{column_name}{constraints},\n"
            create_table = f"""
create table {self.scheme}.{table_name} (
{ddl_columns.lower()}
datestartentry timestamp null,
primary key ({table_name}id)
)
distributed by ({table_name}id)
DATASOURCE_TYPE ('{self.datasourse_type}')
"""

            create_table_ext = f"""
create readable external table {self.scheme}.{table_name}_ext (
{ddl_columns.lower()}
datestartentry timestamp null,
sys_op integer 
) LOCATION '{self.kafka}{self.scheme.upper()}_{table_name.upper()}' FORMAT 'AVRO';
"""
               
        return [{"create_table": create_table}, {"create_table_ext": create_table_ext}]

    async def get_json_for_job(self):
        json_joba = f"""
{{
  "name": "{self.job_name}",
  "version": "v1",
  "status": "ACTIVE",
  "primaryKey": [
    "id"
  ],
  "module": "iis-nsud",
  "args": [],
  "sql": "",
  "outputTopicName": "{self.topic_name}"
}}"""
        return json_joba
    
    async def get_mapping(self):
        mapping = f"""
  - topic:
    - name: [{self.topic_name}]
    - deleteEnable: true
    - table: {self.table_name.lower()}
      jobs:
        - {self.job_name}
      columns: 
{"\n".join(self.column_names_for_mapping)}
        - datestartentry: [ TIMESTAMP, calculatedBy=currentSystemTimestamp ]"""

        return mapping

    async def create_sql_query(self):
        self.mapping = ""
        for table in self.json["tables"]:
            self.primary_column_alias = ""
            self.column_names_for_mapping = []
            self.datasourse_type = table["datasourse_type"]
            self.kafka = table["kafka"]
            self.scheme = table["scheme"]
            self.table_name = table["name"]
            self.job_name = f"iis-nsud-grishin-{self.table_name.replace(self.table_name[0],self.table_name[0].lower(),1)}"
            self.topic_name = f"grishin_{self.table_name.replace(self.table_name[0],self.table_name[0].lower(),1)}-to-nsud"
            self.columns_list = table["columns"]
            self.table_from = table["from"]
            sql_where = table["where"]
            sql_select = await self.get_select()
            sql_joins = await self.get_joins(table)
            self.json_joba = await self.get_json_for_job()
            self.sql_job_query = (sql_select + sql_joins + sql_where)
            self.ddl = await self.get_ddl()
            self.mapping = self.mapping + await self.get_mapping()
            await self.start_save()
 
    async def save_file(self, dirName: str, fileName: str, sql: str):
        # Получаем путь к директории, где находится исполняемый файл
        if getattr(sys, 'frozen', False):
            # Если приложение собрано в исполняемый файл
            script_dir = os.path.dirname(sys.executable)
        else:
            # Если приложение запускается как скрипт
            script_dir = os.path.dirname(os.path.abspath(__file__))

        save_dir = os.path.join(script_dir, str(dirName))
        
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            
        filepath = os.path.join(save_dir, str(fileName))
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(sql)
        except Exception as ex:
            print(f"Error writing to file: {ex}")
    
    async def start_save(self):
        #Создание файлов для джоб
        sql_for_job = self.sql_job_query
        sql_for_job = sql_for_job.replace("                where", "where")
        sql_for_job = sql_for_job.replace("                and", "and")  if "and" in sql_for_job else sql_for_job
        sql_for_job = sql_for_job.replace("                    and", "    and")  if "and" in sql_for_job else sql_for_job
        sql_for_job = "\n".join([line for line in sql_for_job.splitlines() if line.strip()])
        sql_for_job = sql_for_job + self.json_joba
        await self.save_file("JOBS", f"{self.table_name.capitalize()}.sql", sql_for_job)
        #Создание внешних таблиц
        sql_create_table_ext = self.ddl[1]["create_table_ext"]
        sql_create_table_ext = sql_create_table_ext.replace("        ", "    ") 
        sql_create_table_ext = "\n".join([line for line in sql_create_table_ext.splitlines() if line.strip()])
        await self.save_file("DDL", f"{self.table_name.capitalize()}_ext.sql", sql_create_table_ext)
        #Создание внутренних таблиц
        sql_create_table = self.ddl[0]["create_table"]
        sql_create_table = sql_create_table.replace("        ", "    ") 
        sql_create_table = "\n".join([line for line in sql_create_table.splitlines() if line.strip()])
        await self.save_file("DDL", f"{self.table_name.capitalize()}.sql", sql_create_table)
        #Создание маппинга
        all_mapping = f"""
topics:
    {self.mapping}            
"""
        all_mapping = "\n".join([line for line in all_mapping.splitlines() if line.strip()])
        await self.save_file("MAPPING", f"Mapping.yaml", all_mapping)

async def run_script(json):
    await Creator(json).create_sql_query()

asyncio.run(run_script(json)) 