from marshal import loads, dumps
import sqlite3
import uuid as uuidgenerator
import sys


class Matcher:
    def __init__(self, where=None):
        self._where = where

    def docs_matches(self, a):
        doc = loads(a)
        for k in self._where:
            if k in doc and doc[k] != self._where[k]:
                return False
        return True


class Finder:
    def __init__(self, connection, name, where=None):
        self._con = connection
        self._name = name
        self._where = where

        self._cur = None
        self._matcher = Matcher(where)
        connection.create_function("docs_matches", 1, self._matcher.docs_matches)

    def __iter__(self):
        return self

    def next(self):
        if self._cur is None:
            self._cur = self._con.cursor()
            if self._where is None:
                self._cur.execute('SELECT * FROM `' + self._name + '`')
            else:
                self._cur.execute('SELECT * FROM `' + self._name + '` WHERE docs_matches(document)')

        row = self._cur.fetchone()
        if not row:
            raise StopIteration
        doc = loads(row[1])
        if type(doc) == dict:
            if 'uuid' not in doc:
                doc['uuid'] = row[0]
        return doc

    def count(self):
        cur = self._con.cursor()
        if self._where is None:
            cur.execute('SELECT COUNT(*) FROM `' + self._name + '`')
        else:
            cur.execute('SELECT COUNT(*) FROM `' + self._name + '` WHERE docs_matches(document)')

        row = cur.fetchone()
        return row[0]


class Collection:
    def __init__(self, name, connection):
        self._name = name
        self._con = connection
        connection.execute("CREATE TABLE IF NOT EXISTS `" + name + "` (uuid VARCHAR(36) PRIMARY KEY, document BLOB );")

    def insert(self, document, uuid=None):
        if uuid is None:
            uuid = uuidgenerator.uuid1()
        try:
            self._con.execute('INSERT INTO `' + self._name + '` (uuid, document) VALUES(?, ?)',
                              (str(uuid), buffer(dumps(document))))
        except sqlite3.IntegrityError:
            return False
        return uuid

    def find(self, where=None):
        return Finder(self._con, self._name, where)


class PyDocIo:
    def __init__(self, fname=":memory:"):
        self._con = sqlite3.connect(fname)

    def __getattr__(self, key):
        self.__dict__[key] = collection = Collection(key, self._con)
        return collection

    def __del__(self):
        self._con.commit()


def test_insert():
    db = PyDocIo()
    assert 1 == db.collection.insert({'foo': 'bar', 'say': True}, 1)
    assert False == db.collection.insert({'foo3': 'bar', 'say': True}, 1)
    assert 2 == db.collection.insert({'foo': 'bar2', 'say': False}, 2)
    for doc in db.collection.find({'say': False}):
        assert doc == {'uuid': '2', 'foo': 'bar2', 'say': False}
    for doc in db.collection.find({'say': True}):
        assert doc == {'uuid': '1', 'foo': 'bar', 'say': True}


def test_count():
    db = PyDocIo()
    db.collection.insert({'foo': 'bar', 'say': True})
    db.collection.insert({'foo': 'bar2', 'say': False})
    db.collection.insert({'foo': 'bar3', 'say': False})
    assert 2 == db.collection.find({'say': False}).count()
    assert 1 == db.collection.find({'say': True}).count()


def test(argv):
    import inspect

    my_name = inspect.stack()[0][3]
    for f in argv:
        globals()[f]()
    if not argv:
        fs = [globals()[x] for x in globals() if
              inspect.isfunction(globals()[x]) and x.startswith('test') and x != my_name]
        for f in fs:
            print f.__name__
            f()


if __name__ == '__main__':
    sys.exit(test(sys.argv[1:]))