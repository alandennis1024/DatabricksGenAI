import pandas as pd
import cv2
import numpy as np
from pyspark.sql.functions import col, pandas_udf

@pandas_udf("binary")
def blur_faces_in_image(image_series: pd.Series) -> pd.Series:
    cascade = cv2.CascadeClassifier(
        cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    )
    if cascade.empty():
        raise RuntimeError("Face detector is not available")

    processed = []
    for image_bytes in image_series:
        try:
            image = cv2.imdecode(
                np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_COLOR
            )
            if image is None:
                processed.append(None)  # Quarantine decode failures.
                continue
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            for x, y, w, h in cascade.detectMultiScale(gray, 1.1, 4):
                image[y:y+h, x:x+w] = cv2.GaussianBlur(
                    image[y:y+h, x:x+w], (23, 23), 30
                )
            success, encoded = cv2.imencode(".png", image)
            processed.append(encoded.tobytes() if success else None)
        except (ValueError, TypeError, cv2.error):
            processed.append(None)
    return pd.Series(processed)

# Retain Bronze source records for governed provenance and investigation.
df_silver_review = (
    spark.table("cobalt_prod.factory_bronze.qa_image_metadata")
    .withColumn("content_redacted", blur_faces_in_image(col("content")))
)
# Split null/redacted outputs and verify PII detector coverage BEFORE
# publishing approved records to the Silver consumption table.
