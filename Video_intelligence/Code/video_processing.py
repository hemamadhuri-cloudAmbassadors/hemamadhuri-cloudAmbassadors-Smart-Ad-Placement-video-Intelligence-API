from google.cloud import videointelligence
from google.cloud import storage
import json
import time
import requests

gcs_uri = "gs://YOUR-BUCKET/YOUR-VIDEO.mp4"
output_uri = "gs://YOUR-BUCKET/output - {}.json".format(time.time())
parsed_output_uri = "gs://YOUR-BUCKET/parsed_output - {}.txt".format(time.time())

api_key = "YOUR_API_KEY"

url = "YOUR_GEMINI_API_ENDPOINT_URL"

video_client = videointelligence.VideoIntelligenceServiceClient()

features = [
    videointelligence.Feature.OBJECT_TRACKING,
    videointelligence.Feature.LOGO_RECOGNITION,
    videointelligence.Feature.FACE_DETECTION,
    videointelligence.Feature.PERSON_DETECTION,
    videointelligence.Feature.EXPLICIT_CONTENT_DETECTION
]

video_context = videointelligence.VideoContext()

CONFIDENCE_THRESHOLD = 0.6 

DURATION_THRESHOLD = 5

operation = video_client.annotate_video(
    request={
        "features": features,
        "input_uri": gcs_uri,
        "output_uri": output_uri,
        "video_context": video_context
    }
)

print("\nProcessing video...", operation)

result = operation.result(timeout=300)

print("\nFinished processing.")

filtered_results = []

for annotation_result in result.annotation_results:
    
    for object_annotation in annotation_result.object_annotations:
        entity = object_annotation.entity
        description = entity.description
        confidence = object_annotation.confidence

        if description.lower() not in ['logo', 'face', 'person', 'explicit_content'] and confidence >= CONFIDENCE_THRESHOLD:
            
            frame_times = [frame.time_offset.seconds + frame.time_offset.microseconds / 1e6 for frame in object_annotation.frames]
            if not frame_times:
                continue
            
            duration = max(frame_times) - min(frame_times)
            
            if duration >= DURATION_THRESHOLD:
                object_data = {
                    "object": description,
                    "confidence": confidence,
                    "duration": duration,
                    "frames": [
                        {
                            "time": frame.time_offset.seconds + frame.time_offset.microseconds / 1e6,
                            "bounding_box": {
                                "left": frame.normalized_bounding_box.left,
                                "top": frame.normalized_bounding_box.top,
                                "right": frame.normalized_bounding_box.right,
                                "bottom": frame.normalized_bounding_box.bottom
                            }
                        }
                        for frame in object_annotation.frames
                    ]
                }

                filtered_results.append(object_data)

parsed_output = """"""

for obj in filtered_results:
    parsed_output += "Object: {}\n".format(obj['object'])
    parsed_output += "Confidence: {:.6f}\n".format(obj['confidence'])  
    parsed_output += "Duration: {} seconds\n".format(obj['duration'])
    
    parsed_output += "Frames:\n"
    for frame in obj['frames']:
        parsed_output += "  Time: {:.6f} seconds\n".format(frame['time'])
        parsed_output += "  Bounding Box: left = {:.6f}, top = {:.6f}, right = {:.6f}, bottom = {:.6f}\n".format(
            frame['bounding_box']['left'],
            frame['bounding_box']['top'],
            frame['bounding_box']['right'],
            frame['bounding_box']['bottom']
        )
    parsed_output += "\n"


def save_to_gcs(destination_blob_name, data, content_type="text/plain"):
    storage_client = storage.Client()
    bucket_name, blob_name = destination_blob_name.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)
    print(f"Parsed output saved to GCS: {destination_blob_name}")

save_to_gcs(parsed_output_uri, parsed_output, content_type="text/plain")

print("\nParsed output saved to the parsed_output_uri.")

def save_json_to_gcs(destination_blob_name, data):
    storage_client = storage.Client()
    bucket_name, blob_name = destination_blob_name.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    print(f"Filtered results saved to GCS: {destination_blob_name}")

save_json_to_gcs(output_uri, filtered_results)

print("\nFiltered results saved to the output_uri.")

def check_ad_suitability_with_gemini(data, url, api_key, gcs_uri):
    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "text": f"Can we place Ads on these objects:{data} ? please analyze ad suitability for each object instance, Ensure **all instances** of the object are included, even if the same object appears multiple times. If yes, Provide object data in below JSON format? If No, just say No without explanation"
                                    "{{\n"
                                    '"object": "Object Name",\n'
                                    '"confidence": Confidence Value,\n'
                                    '"duration": Duration in seconds,\n'
                                    '"frames": [\n'
                                    "  {{\n"
                                    '    "time": Frame Time in seconds,\n'
                                    "  }}\n"
                                    "]\n"
                                    "}}\n"

                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, params={"key": api_key})
        response.raise_for_status() 
        result = response.json()  
        ad_suitability_text = result['candidates'][0]['content']['parts'][0]['text']
        
        save_to_gcs(gcs_uri, ad_suitability_text, content_type="text/plain")
        print(f"\nAd suitability information saved to GCS at: {gcs_uri}")

        return ad_suitability_text

    except requests.exceptions.RequestException as e:
        print(f"Error communicating with Gemini API: {e}")
        return "Error occurred"

ad_suitability_uri = "gs://YOUR-BUCKET/ad_suitability_output - {}.txt".format(time.time())
is_ad_suitable = check_ad_suitability_with_gemini(parsed_output, url, api_key, ad_suitability_uri)
