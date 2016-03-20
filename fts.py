from functools import reduce
from collections import OrderedDict


#
# score
#
class Score(object):
    pass


class FreqScore(Score):
    pass


#
# term
#
class Term(object):
    def __init__(self, field_name, value):
        self.field_name = field_name
        self.value = value.lower()

    def execute(self, model, score):
        field = model.fields[self.field_name]
        value = self.value
        vecs = model.storage.find_by_field_value(model, field, value, score)
        return vecs


#
# ops
#
class BinOp(object):
    def __init__(self, operator, operands):
        self.operator = operator
        self.operands = operands

    def execute(self, model, score):
        raise NotImplementedError


class And(BinOp):
    def __init__(self, *operands):
        BinOp.__init__(self, 'AND', operands)

    def execute(self, model, score):
        vecs_list = [n.execute(model, score) for n in self.operands]
        docs_ids = [set(n.keys()) for n in vecs_list]
        docs_ids = reduce(lambda p, c: p & c, docs_ids)
        vecs = {}

        for doc_id in docs_ids:
            for _vecs in vecs_list:
                if doc_id not in _vecs:
                    continue

                if doc_id not in vecs:
                    vecs[doc_id] = []

                vecs[doc_id].extend(_vecs[doc_id])

        return vecs


class Or(BinOp):
    def __init__(self, *operands):
        BinOp.__init__(self, 'OR', operands)

    def execute(self, model, score):
        vecs_list = [n.execute(model, score) for n in self.operands]
        docs_ids = [set(n.keys()) for n in vecs_list]
        docs_ids = reduce(lambda p, c: p | c, docs_ids)
        vecs = {}

        for doc_id in docs_ids:
            for _vecs in vecs_list:
                if doc_id not in _vecs:
                    continue

                if doc_id not in vecs:
                    vecs[doc_id] = []

                vecs[doc_id].extend(_vecs[doc_id])

        return vecs


class Xor(BinOp):
    def __init__(self, *operands):
        BinOp.__init__(self, 'XOR', operands)

    def execute(self, model, score):
        vecs_list = [n.execute(model, score) for n in self.operands]
        docs_ids = [set(n.keys()) for n in vecs_list]
        docs_ids = reduce(lambda p, c: p ^ c, docs_ids)
        vecs = {}

        for doc_id in docs_ids:
            for _vecs in vecs_list:
                if doc_id not in _vecs:
                    continue

                if doc_id not in vecs:
                    vecs[doc_id] = []

                vecs[doc_id].extend(_vecs[doc_id])

        return vecs


#
# fields
#
class Field(object):
    def __init__(self, name, type_, store, model):
        self.name = name
        self.type = type_
        self.store = store
        self.model = model


class BoolField(Field):
    def __init__(self, name=None, store=False, model=None):
        Field.__init__(self, name, 'BOOL', store, model)


class IntField(Field):
    def __init__(self, name=None, store=False, model=None):
        Field.__init__(self, name, 'INT', store, model)


class FloatField(Field):
    def __init__(self, name=None, store=False, model=None):
        Field.__init__(self, name, 'FLOAT', store, model)


class StrField(Field):
    def __init__(self, name=None, store=False, model=None):
        Field.__init__(self, name, 'STR', store, model)


class TextField(Field):
    def __init__(self, name=None, store=True, model=None):
        Field.__init__(self, name, 'TEXT', store, model)


#
# model
#
class Model(object):
    def __init__(self, _name, _storage, **_fields):
        self.name = _name
        self.storage = _storage
        self.fields = _fields

        # create model via storage
        self.storage.create_model(self)

    def add(self, doc, doc_id=None):
        return self.storage.add(self, doc, doc_id)

    def get(self, doc_id):
        return self.storage.get(self, doc_id)

    def delete(self, doc_id):
        self.storage.delete(self, doc_id)

    def search(self, query, score=None):
        vecs = self.storage.search(self, query, score)

        return vecs

    def commit(self):
        self.storage.commit(self)

    def close(self):
        self.storage.close(self)


#
# storage
#
class Storage(object):
    def __init__(self):
        pass

    def create_model(self, model):
        raise NotImplementedError

    def drop_model(self, model):
        raise NotImplementedError

    def add(self, model, doc, doc_id):
        raise NotImplementedError

    def get(self, model, doc_id):
        raise NotImplementedError

    def delete(self, model, doc_id):
        raise NotImplementedError

    def search(self, model, query, score):
        raise NotImplementedError

    def find_by_field_value(self, model, field, value, score):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class JsonStorage(Storage):
    def __init__(self, path):
        Storage.__init__(self)
        self.path = path

        self.models = {
            # 'MODEL_NAME_0': model0,
            # 'MODEL_NAME_1': model1,
        }

        self.docs = {
            # 'MODEL_NAME_0': {
            #    doc_id: doc
            # }
        }

        self.data = {
            # 'MODEL_NAME_0': {
            #     'FIELD_0': {},
            #     'FIELD_1': {},
            # }
        }

    def create_model(self, model):
        self.models[model.name] = model
        self.docs[model.name] = {}

        self.data[model.name] = {
            field_name: {}
            for field_name, field in model.fields.items()
            if field.store
        }

    def drop_model(self, model):
        del self.models[model.name]
        del self.docs[model.name]
        del self.data[model.name]

    def add(self, model, doc, doc_id):
        # add document
        self.docs[model.name][doc_id] = doc

        for field_name, value in doc.items():
            field = model.fields[field_name]

            if field.type in ('STR', 'TEXT'):
                value = value.lower()

            if not field.store:
                continue

            t = self.data[model.name][field.name]

            if field.type == 'TEXT':
                # build and add ngrams
                i = 0

                while i < len(value) - 2:
                    s = i
                    e = i + 3
                    _range = (s, e)
                    ngram = value[s:e]

                    if ngram not in t:
                        t[ngram] = {}

                    if doc_id not in t[ngram]:
                        t[ngram][doc_id] = []

                    t[ngram][doc_id].append(_range)
                    i += 1
            else:
                s = 0
                e = len(value)
                _range = (s, e)

                if value not in t:
                    t[value] = {}

                if doc_id not in t[ngram]:
                    t[value][doc_id] = []

                t[value][doc_id].append(_range)

        return doc_id

    def get(self, model, doc_id, default=NotImplemented):
        if default is NotImplemented:
            # can raise KeyError
            doc = self.docs[model.name][doc_id]
        else:
            doc = self.docs[model.name].get(doc_id, default)

        return doc

    def delete(self, model, doc_id):
        del self.docs[model.name][doc_id]

    def search(self, model, query, score):
        vecs = query.execute(model, score)
        return vecs

    def find_by_field_value(self, model, field, value, score):
        vecs = {}
        t = self.data[model.name][field.name]

        if field.type == 'TEXT':
            # build and find ngrams
            i = 0

            while i < len(value) - 2:
                ngram = value[i:i + 3]

                if ngram in t:
                    doc_ranges_map = t[ngram]

                    for doc_id, ranges in doc_ranges_map.items():
                        if doc_id not in vecs:
                            vecs[doc_id] = []

                        vecs[doc_id].extend(ranges)

                i += 1
        else:
            if value in t:
                doc_ranges_map = t[value]

                for doc_id, ranges in doc_ranges_map.items():
                    if doc_id not in vecs:
                        vecs[doc_id] = []

                    vecs[doc_id].extend(ranges)

        return vecs

    def commit(self, model):
        pass

    def close(self, model):
        pass


#
# fts
#
class FTS(object):
    def __init__(self, storage):
        self.storage = storage

        self.models = {
            # MODEL_NAME: model
        }

    def model(self, _model_name, **_fields):
        model = Model(_model_name, self.storage, **_fields)

        # set names from fields' keys
        # set fields' model from fields' keys
        for name, field in model.fields.items():
            field.name = name
            field.model = model

        self.models[model.name] = model
        return model

    def commit(self):
        for model in self.models.values():
            model.commit()

    def close(self):
        for model in self.models.values():
            model.close()


if __name__ == '__main__':
    from pprint import pprint
    from random import choice, randint

    storage = JsonStorage('example0.json')
    fts = FTS(storage)

    # create models
    User = fts.model('User',
                     username=TextField())

    Profile = fts.model('Profile',
                        user_id=IntField(),
                        name=TextField(),
                        age=IntField())

    # generate and insert some documents
    first_names = ['Mike', 'John', 'David', 'Rob', 'Ed']
    last_names = ['Doe', 'Timber', 'Smith', 'Gates', 'Jobs']

    for i in range(1000):
        user_doc = {'username': 'user-{}'.format(i)}
        user_id = User.add(user_doc, i)

        name = '{} {} {}'.format(
            choice(first_names),
            choice(first_names),
            choice(last_names),
        )

        age = randint(18, 65)
        profile_doc = {'user_id': user_id, 'name': name, 'age': age}
        profile_id = Profile.add(profile_doc, i)

    fts.commit()

    # search query
    q = And(
        Term('name', 'ohn'),
        Or(
            Term('name', 'mbe'),
            Term('name', 'obs'),
        )
    )

    vecs = Profile.search(q, score=FreqScore)
    pprint(vecs)
    docs = [Profile.get(doc_id) for doc_id in vecs]
    pprint(docs)

    fts.close()
