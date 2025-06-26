import couchdb

class CouchDBClient:
    def __init__(self, url: str, username: str, password: str, db_name: str):
        self.server = couchdb.Server(url)
        self.server.resource.credentials = ('admin', 'admin')
        self.db_name = 'primary'
        if db_name not in self.server:
            self.server.create(db_name)
        self.db = self.server[db_name]

    def create_document(self, doc: dict) -> str:
        doc_id, doc_rev = self.db.save(doc)
        return doc_id

    def get_document(self, doc_id: str) -> dict:
        try:
            return self.db[doc_id]
        except couchdb.http.ResourceNotFound:
            return None

    def update_document(self, doc_id: str, doc: dict) -> bool:
        if doc_id in self.db:
            self.db[doc_id] = doc
            return True
        return False

    def delete_document(self, doc_id: str) -> bool:
        try:
            doc = self.db[doc_id]
            self.db.delete(doc)
            return True
        except couchdb.http.ResourceNotFound:
            return False 