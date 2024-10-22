import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

import yaml
import pytz
import pandas as pd
from datetime import datetime
from services.db_validation.queries import count_by_column, get_max_value, schema_query, check_null_limit, check_duplicates
from services.slack import send_slack_message
from services.postgres import query_to_dataframe, connect_postgres_engine,create_or_append_table


class DBValidation:
    def __init__(self,yaml_config, db_to_query: dict, db_to_save_config: dict, bot_token: str, report_channel: str):
        self.db_to_query_config = db_to_query
        self.db_to_save_config = db_to_save_config if db_to_save_config else None
        self.yaml_config = yaml_config
        self.bot_token = bot_token
        self.report_channel = report_channel

    def compare_dfs(self, database_df, datalake_df, validation):
        print('Starting compare count by column...')
        diff_df = database_df.compare(datalake_df).dropna()

        success_message = f':white_check_mark:*{validation}*: The validation was made successfully! No errors.'
        error_message = f':heavy_exclamation_mark: *{validation}*: There is {len(diff_df)} cases where contracts_by_created_at count are different in two tables.'
        
        if not diff_df.empty:
            send_slack_message(self.bot_token, self.report_channel, error_message)
        else:
            send_slack_message(self.bot_token, self.report_channel, success_message)

        print('Validation has been made with success!')

    def validate_schema(self,database_table,datalake_table,validation) -> bool:
        """Validate if both tables have the same schema (columns and their data types)."""

        db_query = schema_query.format(table=database_table)
        dl_query = schema_query.format(table=datalake_table)

        db_df = query_to_dataframe(db_query, self.db_to_query_config)
        dl_df = query_to_dataframe(dl_query, self.db_to_query_config)
        
        schema_table1 = db_df.dtypes
        schema_table2 = dl_df.dtypes

        result = schema_table1.equals(schema_table2)

        success_message = f':white_check_mark: *{validation}* The validation was made successfully! No errors.'
        error_message = f':heavy_exclamation_mark: *{validation}*: The *schemas* of {database_table} and {datalake_table} are different.'

        if result:
            send_slack_message(self.bot_token, self.report_channel, success_message)
        else:
            send_slack_message(self.bot_token, self.report_channel, error_message)

        return 
            
    def count_by_column_validation(
        self,
        count_column, 
        agg_column,database_table,
        datalake_table, 
        where_max_value_condition,max_col,
        create_table,
        validation
        ):

        if where_max_value_condition:
            max_value = query_to_dataframe(get_max_value.format(max_col=max_col, table=datalake_table),self.db_to_query_config)
            where_max_value_condition = where_max_value_condition.format(max_value = max_value['max'].iloc[0])


        dl_query = count_by_column.format(count_column=count_column, agg_column= agg_column,table = datalake_table,where_max_value_condition="")
        db_query = count_by_column.format(count_column=count_column, agg_column= agg_column,table = database_table,where_max_value_condition=where_max_value_condition)


        dl_df = query_to_dataframe(dl_query, self.db_to_query_config)
        db_df = query_to_dataframe(db_query, self.db_to_query_config)

        if create_table:
            timezone = pytz.timezone("America/Sao_Paulo")
            current_time = datetime.now(timezone)

            dl_df['data_source'] = 'datalake'
            dl_df['validation_date'] = current_time

            db_df['data_source'] = 'database'
            db_df['validation_date'] = current_time

            print('Creating validation table...')
            connect_postgres_engine(self.db_to_save_config)
            create_or_append_table(db_df,validation,self.db_to_save_config)
            create_or_append_table(dl_df,validation,self.db_to_save_config)
            print('Validation table was creating with success!')

            dl_df = dl_df.drop(['data_source', 'validation_date'], axis=1)
            db_df = db_df.drop(['data_source', 'validation_date'], axis=1)

        try:
            self.compare_dfs(db_df,dl_df,validation)
        except ValueError:
            print(f'Validation {validation} was not possible.')

    def check_null_column(self,table, null_column,where_condition,threshold, validation):
        query = check_null_limit.format(table=table, null_column=null_column, where_condition=where_condition)

        df = query_to_dataframe(query, self.db_to_query_config)
        result = df['null_count'].iloc[0]

        success_message = f':white_check_mark: *{validation}* The validation was made successfully! No errors.'
        error_message = f':heavy_exclamation_mark: *{validation}*: There are more {(result - threshold)} nulls in column {null_column} than allowed.'

        if result > threshold:
            send_slack_message(self.bot_token, self.report_channel, error_message)
        else:
            send_slack_message(self.bot_token,self.report_channel, success_message)

    def check_duplicates(self, column, table, where_condition, validation):
        where = where_condition if where_condition else ""

        query = check_duplicates.format(column=column, table=table, where_condition=where)
        df = query_to_dataframe(query, self.db_to_query_config)

        success_message = f':white_check_mark:*{validation}*: The validation was made successfully! No errors.'
        error_message = ':heavy_exclamation_mark: *{validation}*: There is {count} cases where {column} are duplicate.'
        
        if not df.empty:
            send_slack_message(self.bot_token, self.report_channel, error_message.format(validation = validation, count = df['count'].iloc[0], column=column))
        else:
            send_slack_message(self.bot_token, self.report_channel, success_message)

    def run_validations_from_yaml(self):
        """Load the validation methods from a YAML config file and execute them."""

        with open(self.yaml_config, 'r') as file:
            config = yaml.safe_load(file)

        for name, method_list in config.items():
            for method_info in method_list:
                method_name = method_info['method']
                params = method_info.get('params', {})
                method = getattr(self, method_name, None) 
                if method:
                    print(f"  Running validation: {method_name}")
                    result = method(**params) 
                else:
                    print(f"  Method {method_name} not found!")

        return config






