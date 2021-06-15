from amazon_wf.database import CursorFromConnectionPool
import pdb


class Location:

    def __init__(self, dirpath):
        self.dirpath = dirpath

    def __repr__(self):
        return f"{self.dirpath}"

    def save_to_db(self):
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''INSERT INTO locations(dirpath)
            VALUES (%s);''', (self.dirpath,))

    @classmethod
    def load_by_loc(cls, loc):
        """
        Return a location instance
        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''SELECT dirpath FROM locations WHERE loc=%s;''',
                    (loc,))
            location = cursor.fetchone()
            return cls(dirpath=location[0])

    @staticmethod
    def get_loc_from_dirpath(dirpath):
        """
        Return a loc corresponding to dirpath
        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''SELECT loc FROM locations where dirpath=%s;''',
                    (dirpath,))
            return cursor.fetchone()

    @staticmethod
    def get_dirpath_from_loc(loc):
        """
        Return dirpath corresponding to a loc
        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''SELECT dirpath FROM locations where loc=%s;''',
                    (loc,))
            return cursor.fetchone()[0]
