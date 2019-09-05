from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import json
#from keyphrase import KeyPhrase

class ES(object):
    def __init__(self, es_client):
        assert isinstance(es_client, Elasticsearch)
        self.es = es_client

    def insert_bulk(self, dest_index: str, bulk_documents: list, id_field = None):
        actions = []


        for document in bulk_documents:
            action = {
                "_index": dest_index,
                "_type": "document",
                "_source": document,
                "_id": document[id_field] if id_field is not None else None
            }
            actions.append(action)


        helpers.bulk(self.es, actions)

    def keyword_list_search(self, source_index: str, keyPhrase_list: list, page_size = 500, randomize= False, print_query=True):

        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [{"match_phrase": {kp.field_name : phrase}} for phrase in kp.phrase_list ]
                            }
                        } for kp in keyPhrase_list
                    ]
                }
            }
        }
        if randomize:
            body["sort"] =  {
            "_script" : {
                "script" : "Math.random()",
                "type" : "number",
                "order" : "asc"
                }
              }
        if print_query:
            print("body", body)
        return self.search_result_generator(source_index, body=body, page_size=page_size)


    def search_result_generator(self, source_index: str, body = {}, page_size = 500):

        page = self.es.search(
                request_timeout=30,
                index = source_index,
                scroll = '2m',
                size = page_size,
                body = body
            )
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])

        while (scroll_size > 0 ):
            for hit in page['hits']['hits']:
                yield { "hit": hit, "has_more": True}

            page = self.es.scroll(scroll_id = sid, scroll = '2m')

            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])


        if(scroll_size == 0 ):
            yield { "hit":  None, "has_more": False}

    def get_by_id(self, source_index: str, id: str):
        return self.es.get_source(index = source_index, id=id, doc_type='document')

