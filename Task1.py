import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

# Define the schema for the BigQuery table
schema = 'Order_ID:STRING, Order_Date:STRING, Ship_Date:STRING, Customer_Name:STRING, Country:STRING, City:STRING, State:STRING, Category:STRING, Product_Name:STRING, Sales:FLOAT, Quantity:INTEGER, Profit:FLOAT'

def run_pipeline():
    input_file = 'gs://walmart_pc/Walmart.csv'
    output_table = 'walmart_data.walmart_table'
    project = 'priyankachoudhary-1679721047'
    batch_size = 1000

    # Set up the pipeline options
    pipeline_options = PipelineOptions(
        runner='DirectRunner',
        project=project,
        temp_location='gs://walmart_pc/tmp',
        staging_location='gs://walmart_pc/staging',
        autoscaling_algorithm='THROUGHPUT_BASED',
        max_num_workers=5,
        region='asia-south1',
        save_main_session=True,
        schedule = '*/15 * * * *'
    )

    # Define the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the CSV file from GCS
        lines = pipeline | 'ReadFromGCS' >> ReadFromText(input_file, skip_header_lines=1)

        # Transform the data
        records = (
            lines
            | 'ParseCSV' >> beam.Map(lambda line: line.split(','))
            | 'FormatRecord' >> beam.Map(lambda record: {
                'Order_ID': record[0],
                'Order_Date': record[1],
                'Ship_Date': record[2],
                'Customer_Name': record[3],
                'Country': record[4],
                'City': record[5],
                'State': record[6],
                'Category': record[7],
                'Product_Name': record[8],
                'Sales': int(record[9]),
                'Quantity': int(record[10]),
                'Profit': int(record[11])
            })
        )

        # Write the data to BigQuery
        records | 'WriteToBigQuery' >> WriteToBigQuery(
            output_table,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            batch_size=batch_size
        )
        
run_pipeline()
