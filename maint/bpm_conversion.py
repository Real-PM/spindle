import os
import subprocess as sub

from loguru import logger

import analysis.bpm as b
import db.db_update as dbu

db = dbu.database


def maintain_bpm(database: dbu.Database):
    """
    Query database for tracks that are .m4a and are missing bpm in track_data.
    Create a temporary version of the track in .wav format, analyze for bpm, update the database, and delete
    the temporary file.
    Args:
        database:

    Returns:

    """
    temp_dir = "temp"
    database.connect()
    query = """SELECT td.id, td.title, td.filepath
    FROM track_data td
    WHERE td.filepath LIKE '%.m4a' AND td.bpm IS NULL"""
    tracks = database.execute_select_query(query)
    for track_id, title, filepath in tracks:
        temp_filepath = os.path.join(temp_dir, f"{title}.wav")
        logger.debug(f"Converting {filepath} to {temp_filepath}")
        sub.run(["ffmpeg", "-i", filepath, temp_filepath])
        bpm = b.get_bpm(temp_filepath)
        if bpm:
            update_query = "UPDATE track_data SET bpm = ? WHERE id = ?"
            database.execute_query(update_query, (bpm, track_id))
            logger.info(f"Updated {title} with bpm {bpm}")
        else:
            logger.info(f"Failed to update {title} with bpm")
        os.remove(temp_filepath)
