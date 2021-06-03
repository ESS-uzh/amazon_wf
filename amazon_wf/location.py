from database import connect

class Location:

    def __init__(self, dirpath):
        self.dirpath = dirpath

    def __repr__(self):
        return f"{self.dirpath}"

    @classmethod
    def load_by_loc(cls, loc):
        """
        Return dirpath corresponding to a loc
        """
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT dirpath FROM locations WHERE loc=%s;''',
                        (loc,))
                location = cursor.fetchone()
                return cls(dirpath=location[0])
