import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from services.db_validation.db_validation_service import DBValidation


def main():
    slack_token = os.environ.get('BOT_TOKEN')
    slack_channel = os.environ.get('CHANNEL NAME')

    config_db_dict =  {
        'host': os.environ.get('HOST'),
        'database': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'port': os.environ.get('PORT')
    }
    validator = DBValidation('contracts_tape_config.yaml',db_to_query= config_db_dict, db_to_save_config= config_db_dict, bot_token= slack_token, report_channel = slack_channel)
    validator.run_validations_from_yaml()


if __name__ == '__main__':
    main()

    

    