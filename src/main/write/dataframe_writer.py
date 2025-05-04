from src.main.utility.logging_config import logger
import traceback


def write_df(df, file_path, mode="overwrite", format="parquet"):
    try:
        df.write.format(format).option("header", "true").mode(mode).save(file_path)
        logger.info(
            f"Data written into {file_path} in the {format} format with {mode} mode."
        )
    except Exception as e:
        logger.error(f"Error writing the data : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e
