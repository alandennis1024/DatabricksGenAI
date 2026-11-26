import io
import librosa
import numpy as np
import pandas as pd
import soundfile as sf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, FloatType

@pandas_udf(ArrayType(FloatType()))
def extract_audio_features(audio_series: pd.Series) -> pd.Series:
    features = []
    for audio_bytes in audio_series:
        try:
            signal, sample_rate = sf.read(io.BytesIO(audio_bytes))
            if signal.ndim > 1:
                signal = signal.mean(axis=1)
            mfccs = librosa.feature.mfcc(
                y=signal, sr=sample_rate, n_mfcc=13
            )
            features.append(
                np.mean(mfccs, axis=1).astype("float32").tolist()
            )
        except (ValueError, TypeError, RuntimeError, sf.LibsndfileError):
            features.append(None)  # Quarantine; not a valid zero vector.
    return pd.Series(features)

# Keep raw audio governed; publish valid features only after QA.
df_acoustic_review = (
    spark.table("cobalt_prod.factory_bronze.acoustic_logs")
    .withColumn("mfcc_vector", extract_audio_features(col("audio_binary")))
)
# Route null vectors into a separate quarantine dataset.
