"""
BPM (tempo) analysis for audio files using Essentia.

This module provides local BPM detection for tracks that don't have BPM data.

The primary function `get_bpm_essentia_safe` runs analysis in a subprocess
to isolate crashes (SEGV) in Essentia's C++ code from killing the main pipeline.
"""

import multiprocessing
import os

from loguru import logger

try:
    import essentia.standard as es

    ESSENTIA_AVAILABLE = True
except ImportError:
    ESSENTIA_AVAILABLE = False
    logger.warning("Essentia not installed - local BPM analysis unavailable")


def check_essentia_available() -> bool:
    """
    Check if Essentia is installed and available.

    Returns:
        True if Essentia is available, False otherwise
    """
    return ESSENTIA_AVAILABLE


def get_bpm_essentia(filepath: str) -> float | None:
    """
    Calculate BPM for an audio file using Essentia's RhythmExtractor2013.

    Args:
        filepath: Path to the audio file (supports mp3, flac, m4a, wav, etc.)

    Returns:
        BPM as float if successful, None on error

    Note:
        RhythmExtractor2013 is Essentia's recommended algorithm for BPM detection.
        It returns BPM along with confidence score, beat positions, and estimates.
    """
    if not ESSENTIA_AVAILABLE:
        logger.error("Essentia not available - cannot analyze BPM")
        return None

    if not filepath:
        logger.debug("Empty filepath provided")
        return None

    if not os.path.isfile(filepath):
        logger.debug(f"File not found: {filepath}")
        return None

    try:
        # MonoLoader handles various formats and resamples to 44100Hz mono
        loader = es.MonoLoader(filename=filepath)
        audio = loader()

        if len(audio) == 0:
            logger.warning(f"Empty audio data from file: {filepath}")
            return None

        # RhythmExtractor2013 is the recommended BPM detection algorithm
        rhythm_extractor = es.RhythmExtractor2013()
        bpm, ticks, confidence, estimates, intervals = rhythm_extractor(audio)

        # Validate BPM is in reasonable range (40-220 BPM covers most music)
        if bpm < 40 or bpm > 220:
            logger.warning(f"BPM {bpm:.2f} outside valid range for {filepath}")
            # Still return it - let the caller decide
            return float(bpm)

        logger.debug(
            f"BPM: {bpm:.2f} (confidence: {confidence:.2f}) for {os.path.basename(filepath)}"
        )
        return float(bpm)

    except RuntimeError as e:
        # Essentia raises RuntimeError for file format issues
        logger.debug(f"Essentia error processing {filepath}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error analyzing {filepath}: {e}")
        return None


def get_bpm_with_confidence(filepath: str) -> tuple[float | None, float | None]:
    """
    Calculate BPM and confidence score for an audio file.

    Args:
        filepath: Path to the audio file

    Returns:
        Tuple of (bpm, confidence), both may be None on error
    """
    if not ESSENTIA_AVAILABLE:
        return None, None

    if not filepath or not os.path.isfile(filepath):
        return None, None

    try:
        loader = es.MonoLoader(filename=filepath)
        audio = loader()

        if len(audio) == 0:
            return None, None

        rhythm_extractor = es.RhythmExtractor2013()
        bpm, ticks, confidence, estimates, intervals = rhythm_extractor(audio)

        return float(bpm), float(confidence)

    except Exception as e:
        logger.debug(f"Error analyzing {filepath}: {e}")
        return None, None


def _analyze_bpm_worker(filepath: str, result_queue: multiprocessing.Queue) -> None:
    """
    Worker function that runs in a subprocess to analyze BPM.

    This function is intentionally isolated - if Essentia crashes (SEGV),
    only this subprocess dies, not the main pipeline.

    Args:
        filepath: Path to the audio file
        result_queue: Queue to send results back to parent process
    """
    try:
        # Import essentia inside subprocess to get fresh state
        import essentia.standard as es_worker

        loader = es_worker.MonoLoader(filename=filepath)
        audio = loader()

        if len(audio) == 0:
            result_queue.put(("empty", None, None))
            return

        rhythm_extractor = es_worker.RhythmExtractor2013()
        bpm, _ticks, confidence, _estimates, _intervals = rhythm_extractor(audio)

        result_queue.put(("success", float(bpm), float(confidence)))

    except RuntimeError as e:
        # Essentia raises RuntimeError for file format issues
        result_queue.put(("error", str(e), None))
    except Exception as e:
        result_queue.put(("error", str(e), None))


def get_bpm_essentia_safe(filepath: str, timeout: float = 120.0) -> float | None:
    """
    Calculate BPM for an audio file using Essentia in a subprocess.

    This isolates Essentia crashes (SEGV) to the subprocess, preventing
    them from killing the main pipeline. If the subprocess crashes or
    times out, this function returns None and the pipeline continues.

    Args:
        filepath: Path to the audio file (supports mp3, flac, m4a, wav, etc.)
        timeout: Maximum seconds to wait for analysis (default 120)

    Returns:
        BPM as float if successful, None on error, crash, or timeout
    """
    if not ESSENTIA_AVAILABLE:
        logger.error("Essentia not available - cannot analyze BPM")
        return None

    if not filepath:
        logger.debug("Empty filepath provided")
        return None

    if not os.path.isfile(filepath):
        logger.debug(f"File not found: {filepath}")
        return None

    result_queue = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=_analyze_bpm_worker,
        args=(filepath, result_queue),
    )

    process.start()
    process.join(timeout=timeout)

    # Check if process is still running (timed out)
    if process.is_alive():
        logger.warning(f"BPM analysis timed out after {timeout}s: {os.path.basename(filepath)}")
        process.terminate()
        process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join(timeout=1)
        return None

    # Check exit code - non-zero means crash (SEGV, etc.)
    if process.exitcode != 0:
        logger.warning(
            f"BPM analysis crashed (exit code {process.exitcode}): {os.path.basename(filepath)}"
        )
        return None

    # Get result from queue
    try:
        if not result_queue.empty():
            result = result_queue.get_nowait()
            status = result[0]

            if status == "success":
                bpm, confidence = result[1], result[2]
                filename = os.path.basename(filepath)

                # Validate BPM is in reasonable range
                if bpm < 40 or bpm > 220:
                    logger.warning(f"BPM {bpm:.2f} outside valid range for {filename}")

                logger.debug(f"BPM: {bpm:.2f} (confidence: {confidence:.2f}) for {filename}")
                return bpm

            elif status == "empty":
                logger.warning(f"Empty audio data from file: {filepath}")
                return None

            else:  # error
                error_msg = result[1]
                logger.debug(f"Essentia error processing {filepath}: {error_msg}")
                return None
        else:
            logger.debug(f"No result returned for {filepath}")
            return None

    except Exception as e:
        logger.debug(f"Error getting BPM result for {filepath}: {e}")
        return None
