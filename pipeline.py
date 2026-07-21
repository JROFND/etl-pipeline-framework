from etl_logging.logger import get_logger
from audit.etl_run_history import ETLRunHistory


logger = get_logger("Pipeline")


class Pipeline:
    """
    ETL pipeline orchestrator.
    """


    def __init__(
        self,
        extractor,
        transformers,
        loader,
        engine,
        pipeline_name="ETL_Process"
    ):

        self.extractor = extractor
        self.transformers = transformers
        self.loader = loader


        self.audit = ETLRunHistory(
            engine=engine,
            pipeline_name=pipeline_name
        )



    def run(self):

        rows_read = 0
        rows_loaded = 0


        logger.info(
            "ETL Started"
        )


        self.audit.start()


        try:

            for chunk in self.extractor.extract():


                rows_read += len(chunk)


                logger.info(
                    f"Extracted {len(chunk):,} rows"
                )


                for transformer in self.transformers:

                    chunk = transformer.transform(
                        chunk
                    )


                loaded = self.loader.load(
                    chunk
                )


                rows_loaded += loaded



            self.audit.complete(
                rows_read,
                rows_loaded
            )


            logger.info(
                f"ETL Complete. Rows processed: {rows_loaded:,}"
            )



        except Exception as e:


            self.audit.fail(
                e
            )


            logger.exception(
                "ETL Failed"
            )


            raise


