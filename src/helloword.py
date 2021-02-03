import awswrangler as wr
import pandas as pd
import time


def original():
    # ctas=False -> 10 minutes (610.72 seconds)
    # xtas=True -> 253.83 seconsd
    start = time.time()

    # s3://virgilholdings-report-data-exports-dev/report
    # default: s3://virgilholdings-data-pipeline-tasks-dev-output/report_data_exports/parquet/
    # jinita: s3://virgilholdings-data-pipeline-tasks-stg-output/jinita-exploration/job_list

    print('Start')

    queryString = """
            WITH job_trasactions AS (
            SELECT jpt.created_at, credit_type, job_id
            FROM report_glue_db_stg.job_property_company_transactions jpt
            JOIN report_glue_db_stg.property_company_transactions pt on pt.property_company_transaction_id = jpt.property_company_transaction_id
            UNION
            SELECT jpt.created_at, credit_type, job_id
            FROM report_glue_db_stg.job_property_transactions jpt
            JOIN report_glue_db_stg.property_transactions pt ON pt.property_transaction_id = jpt.property_transaction_id
        ),


        job_company_property AS (
            SELECT jpt.created_at, credit_type, jobs.job_id, prop.property_id, prop.name property_name, c.name company_name, c.company_id,
            jobs.title, jobs.state, jobs.city, jobs.archive, jobs.post_end_date, jobs.created_at job_created_date, jobs.auto_renew, jobs.description,
            jobs.employment_type, jobs.experience_level, jobs.external_link, jobs.is_anonymous, jobs.requirements, jobs.source, jobs.assessment_id
            FROM job_trasactions jpt
            JOIN report_glue_db_stg.jobs jobs on jobs.job_id = jpt.job_id
            JOIN report_glue_db_stg.properties prop ON prop.property_id = jobs.property_id
            JOIN report_glue_db_stg.companies c on c.company_id = prop.company_id
            order by jobs.job_id
    )

        SELECT * from job_company_property
    """

    #print (queryString)
    job_list = wr.athena.read_sql_query(queryString, database="report_glue_db_stg", ctas_approach=True)
    #print(job_transaction_list.shape)

    columnn_names = job_list.columns

    print(columnn_names)
    print(job_list.info)
    print ("The End")
    print('It took', time.time()-start, 'seconds.')

def get_jobs_transaction_data():
    # ctas=False -> 10 minutes (610.72 seconds)
    start = time.time()

    # s3://virgilholdings-report-data-exports-dev/report
    # default: s3://virgilholdings-data-pipeline-tasks-dev-output/report_data_exports/parquet/
    # jinita: s3://virgilholdings-data-pipeline-tasks-stg-output/jinita-exploration/job_transactions/<parquet_files>

    print('Start')

    queryString = """
            WITH job_trasactions AS (
            SELECT jpt.created_at, credit_type, job_id
            FROM report_glue_db_stg.job_property_company_transactions jpt
            JOIN report_glue_db_stg.property_company_transactions pt on pt.property_company_transaction_id = jpt.property_company_transaction_id
            UNION
            SELECT jpt.created_at, credit_type, job_id
            FROM report_glue_db_stg.job_property_transactions jpt
            JOIN report_glue_db_stg.property_transactions pt ON pt.property_transaction_id = jpt.property_transaction_id
        ),


        job_company_property AS (
            SELECT jpt.created_at, credit_type, jobs.job_id, prop.property_id, prop.name property_name, c.name company_name, c.company_id,
            jobs.title, jobs.state, jobs.city, jobs.archive, jobs.post_end_date, jobs.created_at job_created_date, jobs.auto_renew, jobs.description,
            jobs.employment_type, jobs.experience_level, jobs.external_link, jobs.is_anonymous, jobs.requirements, jobs.source, jobs.assessment_id
            FROM job_trasactions jpt
            JOIN report_glue_db_stg.jobs jobs on jobs.job_id = jpt.job_id
            JOIN report_glue_db_stg.properties prop ON prop.property_id = jobs.property_id
            JOIN report_glue_db_stg.companies c on c.company_id = prop.company_id
            order by jobs.job_id
            Limit 100
    )

        SELECT * from job_company_property
    """
    print(queryString)

    df_job_list = wr.athena.read_sql_query(queryString, database="report_glue_db_stg", ctas_approach=True, chunksize=100000)
    filep='s3://virgilholdings-data-pipeline-tasks-stg-output/jinita-exploration'
    
    
    file_count = 0
    file_name = 'job_lists'

    for chunk in df_job_list:
        file_count += 1
        parquet_file = {filep}-{file_name}-{file_count}
        print(parquet_file)
    #    wr.s3.to_parquet(
     #       df=chunk,
     #       path=parquet_file
    #    )
    #    df_job_list.to_parquet()
        
    #    print(chunk.info())
    #print(job_transaction_list.shape)
    
   # print(df_job_list.info)
    print ("The End")
    print('It took', time.time()-start, 'seconds.')

if __name__ == "__main__":
   #original()
   get_jobs_transaction_data()