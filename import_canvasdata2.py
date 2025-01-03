from django.core.management.base import BaseCommand
from bloom.canvasdata.util.logging import LogManagement
from sqlalchemy import create_engine, text
import os
import datetime
import pandas as pd
import glob
from urllib.parse import quote_plus

class Command(BaseCommand):
    object_type = 'CanvasData 2 import'
    help = 'This command imports CanvasData 2 tsv files to PostgreSQL'
    log = LogManagement(object_type)

    def add_arguments(self, parser):
        parser.add_argument('--table', required=True, help='Specify the table to import data into')

    def handle(self, *args, **options):
        # Retrieve environment variables for PostgreSQL connection
        POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
        POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
        POSTGRES_PASSWORD = quote_plus(os.getenv('POSTGRES_PASSWORD', 'password'))
        POSTGRES_NAME = os.getenv('POSTGRES_NAME', 'canvasdata')
        
        # Create PostgreSQL connection string
        db_uri = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_NAME}'
        engine = create_engine(db_uri)

        allowed_import_types = {
            'users': 'canvasdata2_users',
            'courses': 'canvasdata2_courses',
            'enrollments': 'canvasdata2_enrollments',
            'pseudonyms': 'canvasdata2_pseudonyms',
            'enrollment_terms': 'canvasdata2_enrollment_terms',
            'course_sections': 'canvasdata2_course_sections'
        }

        table = options['table']
        if table not in allowed_import_types:
            self.stdout.write(self.style.ERROR(f'Table {table} not allowed.'))
            return

        base_directory = f'D:\canvasdata\{table}'

        try:
            data = self.read_csv_files_created_today(base_directory)
            if not data.empty:
                self.upsert_data(table, data, engine)
                self.stdout.write(self.style.SUCCESS(f'Successfully imported data into {table}'))
            else:
                self.stdout.write(self.style.WARNING('No data found to import.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error importing data: {e}'))

    def read_csv_files_created_today(self, base_directory):
        today_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        csv_files = glob.glob(os.path.join(base_directory, f'{today_date}-part-*.csv'))

        df = pd.concat(
            (pd.read_csv(f, na_values=['']).applymap(lambda x: None if pd.isna(x) else x) for f in csv_files),
            ignore_index=True
        ) if csv_files else pd.DataFrame()

        # Remove prefixes from columns
        prefixes_to_remove = ['value.', 'meta.', 'key.']
        df.columns = [col.split('.')[-1] for col in df.columns]

        return df

    def upsert_data(self, table, data, engine):
        sql = text(f"""
            INSERT INTO canvasdata2_{table} ({','.join(data.columns)})
            VALUES ({','.join(f':{col}' for col in data.columns)})
            ON CONFLICT (id) DO UPDATE SET
            {','.join(f'{col} = EXCLUDED.{col}' for col in data.columns if col != 'id')}
        """)

        with engine.begin() as conn:
            conn.execute(sql, data.to_dict(orient='records'))
