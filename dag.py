from airflow.sdk import dag, task, task_group, Param
import pathlib
from datetime import datetime
import os

SCHEMA="{{'scraped_quotes' if params.environment == 'production' else 'dev_joel'}}"
DAG_ROOT=pathlib.Path(__file__).parent
BUCKET="{{ 'l3-external-storage-753900908173' if params.environment == 'production' else 'l3-external-storage-753900908173-dev' }}"
S3_KEYS={
    'extract': '{{ dag.dag_id }}/extract/{{ ds }}'
    }

@dag(
    schedule='@daily',
    start_date=datetime(2025, 3, 5),
    end_date=datetime(2026, 3, 13),
    params={
        'environment': Param(
            os.getenv('environment', 'production'),
            dtype='string',
            enum=['development', 'production']
            )
        }
    )
def quotes_scraper():

    @task_group
    def extract():

        @task
        def quotes(filepath, extract_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup

            print('BUCKET:', BUCKET)
            
            # Collect quotes
            html = pathlib.Path(filepath).read_text()
            
            # Push quotes to S3
            hook = S3Hook()
            hook.load_string(
                string_data=html,
                key=extract_key + '/quotes.html',
                bucket_name=BUCKET,
                replace=True,
                )
            
            # Scrape author urls
            soup = BeautifulSoup(html, features="lxml")
            author_containers = soup.find_all('small', {'class': 'author'})
            author_urls = [x.parent.find('a').attrs['href'] for x in author_containers]

            # Push author urls
            return author_urls
        
        @task
        def authors(author_links, extract_key, BUCKET, ds):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            # Initialize S3 Hook
            hook = S3Hook()

            # Loop over items in author_links
            for link in author_links:

                # Isolate the author's name in the author link
                author_name = link.split('/')[-1]

                # Define the filepath to the html file
                filepath = (
                    pathlib.Path(__file__).parent / 
                    'authors' / 
                    (ds + '-' + author_name + '.html')
                    )
                
                # Read the html from the file
                html = filepath.read_text()

                # Define the S3 Key for the raw author html file
                key = extract_key + f'/authors/{author_name}.html'

                # Push the author html to S3
                hook.load_string(
                    string_data=html,
                    key=key,
                    bucket_name=BUCKET,
                    replace=True
                    )
        
        # Define the filepath for the quotes html file
        filepath = (DAG_ROOT / 'quotes' / 'quotes-{{ ds }}.html').as_posix()

        # Call the `quotes` task
        author_links = quotes(filepath, S3_KEYS['extract'], BUCKET)

        # Call the `authors` task
        authors(author_links, S3_KEYS['extract'], BUCKET)

    @task_group
    def transform():

        @task
        def not_implemented():
            pass

    @task_group
    def load():

        @task
        def not_implemented():
            pass

        
    extract() 
    
quotes_scraper()
