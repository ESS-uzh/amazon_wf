from amazon_wf.database import CursorFromConnectionPool
from amazon_wf.database import Database
import json
import pdb


class User:

    def __init__(self, name, pwd=None, _id=None):
        self.name = name
        self.pwd = pwd
        self._id = _id

    def __repr__(self):
        return f"{self.name}"

    def save_to_db(self):
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''INSERT INTO users(name)
            VALUES (%s);''', (self.name,))

    def update_user(self):
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''UPDATE users set password=%s
            WHERE name=%s;''',
            (self.pwd,  self.name))

    @classmethod
    def load_by_id(cls, _id):
        """
        Return user instance
        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''SELECT * FROM users WHERE id=%s;''',
                    (_id,))
            data = cursor.fetchone()
            return cls(_id=data[0], 
                    name=data[1],
                    pwd=data[2])

    @classmethod
    def load_by_name(cls, name):
        """
        Return user instance
        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''SELECT * FROM users WHERE name=%s;''',
                    (name,))
            data = cursor.fetchone()
            return cls(_id=data[0], 
                    name=data[1],
                    pwd=data[2])

    @staticmethod
    def get_users():
        """
        Return a list of all users
        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('SELECT name FROM users')
            return [item[0] for item in cursor.fetchall()]

if __name__ == '__main__':
    with open('../../db_amazon_credentials.json', "r") as read_file:
        db = json.load(read_file)
    Database.initialise(user=db['user'], password=db['pwd'],
                    database=db['database'], host=db['host'])
    user_db = User.load_by_name('VillamainaD')
    print(user_db._id)
