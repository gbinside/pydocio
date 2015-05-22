from marshal import loads, dumps
import sqlite3
import uuid as uuidgenerator
import sys


class Matcher:
    def __init__(self):
        pass

    def docs_matches(self, a):
        doc = loads(a)
        for k in self.where:
            if k in doc and doc[k] != self.where[k]:
                return False
        return True


class Collection:
    def __init__(self, name, connection):
        self._name = name
        self._con = connection
        self._matcher = Matcher()
        connection.create_function("docs_matches", 1, self._matcher.docs_matches)
        connection.execute("CREATE TABLE IF NOT EXISTS `" + name + "` (uuid VARCHAR(36) PRIMARY KEY, document BLOB );")

    def insert(self, document, uuid=None):
        if uuid is None:
            uuid = uuidgenerator.uuid1()
        self._con.execute('INSERT INTO `' + self._name + '` (uuid, document) VALUES(?, ?)',
                          (str(uuid), buffer(dumps(document))))

    def find(self, where=None):
        cur = self._con.cursor()
        if where is None:
            cur.execute('SELECT * FROM `' + self._name + '`')
        else:
            self._matcher.where = where
            cur.execute('SELECT * FROM `' + self._name + '` WHERE docs_matches(document)')
        row = cur.fetchone()
        while row:
            yield loads(row[1])
            row = cur.fetchone()


class PyDocIo:
    def __init__(self, fname=":memory:"):
        self._con = sqlite3.connect(fname)

    def __getattr__(self, key):
        self.__dict__[key] = collection = Collection(key, self._con)
        return collection

    def __del__(self):
        self._con.commit()


def test_init():
    db = PyDocIo()
    db.collection.insert({'foo': 'bar', 'say': True})
    db.collection.insert({'foo': 'bar2', 'say': False})
    for doc in db.collection.find({'say': False}):
        assert doc == {'foo': 'bar2', 'say': False}
    for doc in db.collection.find({'say': True}):
        assert doc == {'foo': 'bar', 'say': True}

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