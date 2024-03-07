import concurrent.futures
import arcgis
from arcgis.gis import GIS
import os, re, csv
import time

portalURL = "https://www.arcgis.com"
username = "USER NAME INFO"
password = "PASSWORD INFO"
survey_item_id = "ID SURVEY"
save_path = r"PATH LOCAL"

start_time = time.time()

gis = GIS(portalURL, username, password)
survey_by_id = gis.content.get(survey_item_id)

rel_fs = survey_by_id.related_items('Survey2Service','forward')[0]

layers = rel_fs.layers + rel_fs.tables
success = False

def download_attachment(layer, object_id, attachment_id, attachment_path):
    try:
        layer.attachments.download(oid=object_id, attachment_id=attachment_id, save_path=os.path.dirname(attachment_path))
        return True
    except Exception as e:
        print(f"Erro ao baixar o anexo {attachment_path}: {e}")
        return False

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for layer in layers:
        if layer.properties.hasAttachments:
            feature_object_ids = layer.query(where="1=1", return_ids_only=True, order_by_fields='objectid ASC')
            for object_id in feature_object_ids['objectIds']:
                feature = layer.query(where=f"OBJECTID = {object_id}").features[0]
                icode = feature.attributes.get('CODE SURVEY')
                auditor_name = feature.attributes.get('LIST NAME SURVEY')
                attachments = layer.attachments.get_list(oid=object_id)
                if attachments:
                    for attachment in attachments:
                        attachment_id = attachment['id']
                        attachment_name = attachment['name']
                        attachment_path = os.path.join(save_path, f"{object_id}_{auditor_name}_{icode}", attachment_name)
                        os.makedirs(os.path.dirname(attachment_path), exist_ok=True)
                        if not os.path.exists(attachment_path):
                            futures.append(executor.submit(download_attachment, layer, object_id, attachment_id, attachment_path))

end_time = time.time() # Obtém o tempo de término
elapsed_time = end_time - start_time # Calcula o tempo decorrido

print(f"Tempo decorrido: {elapsed_time} segundos") # Imprime o tempo decorrido