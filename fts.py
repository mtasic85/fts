#
# ops
#
class BinOp(object):
    def __init__(self, operator, operands):
        self.operator = operator
        self.operands = operands

class AndOp(BinOp):
    def __init__(self, *operands):
        BinOp.__init__(self, 'AND', operands)

class OrOp(BinOp):
    def __init__(self, *):
        BinOp.__init__(self, 'OR', operands)

class XorOp(BinOp):
    def __init__(self, *operands):
        BinOp.__init__(self, 'XOR', operands)

#
# term
#
class Term(object):
    def __init__(self, field, value):
        self.field = field
        self.value = value

#
# fields
#
class Field(object):
    def __init__(self, name, type_, store):
        self.name = name
        self.type = type_
        self.store = store

class BoolField(Field):
    def __init__(self, name=None, store=False):
        Field.__init__(self, name, 'BOOL', store)

class IntField(Field):
    def __init__(self, name=None, store=False):
        Field.__init__(self, name, 'INT', store)

class FloatField(Field):
    def __init__(self, name=None, store=False):
        Field.__init__(self, name, 'FLOAT', store)

class StrField(Field):
    def __init__(self, name=None, store=False):
        Field.__init__(self, name, 'STR', store)

class TextField(Field):
    def __init__(self, name=None):
        Field.__init__(self, name, 'TEXT', store)

#
# storage
#
class Storage(object):
    def __init__(self):
        pass

    def add(self, doc, doc_id=None):
        return doc_id

    def get(self, doc_id):
        pass

    def delete(self, doc_id):
        pass

    def search(self, query):
        pass

class JsonStorage(Storage):
    def __init__(self, path):
        Storage.__init__(self)
        self.path = path
        
        self.schema = {
            # 'SCHEMA_NAME_0': schema0
        }

        self.index = {
            'schema': {
                # 'SCHEMA_0': {
                #     'document': {},
                #     'bool': {},
                #     'int': {},
                #     'float': {},
                #     'string': {},
                #     'text': {},
                # }
            }
        }

    def add(self, doc, doc_id=None):
        return doc_id

    def get(self, doc_id):
        pass

    def delete(self, doc_id):
        pass

    def search(self, query):
        pass

#
# model
#
class Model(object):
    def __init__(self, name, storage, **fields):
        self.name = name
        self.storage = storage
        self.fields = fields

    def add(self, doc, doc_id=None):
        return doc_id

    def get(self, doc_id):
        pass

    def delete(self, doc_id):
        pass

    def search(self, query):
        pass

class FTS(object):
    def __init__(self, storage):
        self.storage = storage

    def model(self, **fields):
        # set names from fields' keys
        for name, field in fields.items():
            field.name = name

        model = Model(**fields)
        return model

if __name__ == '__main__':
    from pprint import pprint
    from random import choice, randint
    
    storage = JsonStorage('example0.json')
    fts = FTS(storage)
    User = fts.model('User', username=TextField())
    Profile = fts.model('Profile', user_id=IntField(), name=TextField(), age=IntField())

    first_names = ['Mike', 'John', 'David', 'Rob', 'Ed']
    last_names = ['Doe', 'Timber', 'Smith', 'Gates', 'Jobs']

    for i in range(1000):
        user_id = User.add({'username': 'user{}'.format(i)}, i)
        name = '{} {}'.format(choice(first_names), choice(last_names))
        profile_id = Profile.add({'user_id': user_id, 'name': name, 'age': randint(18, 65)}, i)

    q = AndOp(
        Term('name', ''),
    )

    docs = Profile.search(q)
    pprint(docs)
