import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatName(beam.DoFn):
      def process(self, element):
        state = element['state']
        latitude = element['latitude']
        longitude = element['longitude']

        if state not in states:
            states.append(state)
            if latitude != None: 
                record = {'state': state, 'latitude': latitude, 'longitude': longitude}
                return [record]
states = []           
def run():
    PROJECT_ID = 'still-primer-302701'
    BUCKET = 'gs://biocode-mile3/covid_data'

    options = {
    'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # executed with the Direct Runner
    p = beam.Pipeline('DirectRunner', options=opts)

    # run a BigQuery query with a LIMIT clause over the source table (< 500)
    sql = 'SELECT DISTINCT(state), latitude, longitude FROM datamart.Location limit 500'
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    # make a PCollection from the BigQuery query results
    PCollection = p | 'Read from BQ' >> beam.io.Read(bq_source)

    # run a pardo call on the results from the query
    out_pcoll = PCollection | 'Format Name' >> beam.ParDo(FormatName())

    # write the output to a local file by the name of output.txt
    out_pcoll | 'Log output' >> WriteToText('output.txt')

    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Location_Beam'
    schema_id = 'state:STRING,latitude:FLOAT,longitude:FLOAT'

    out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)

    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()