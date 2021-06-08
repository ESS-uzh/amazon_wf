from database import connect
import pdb

class Tile:

    def __init__(self, name, acquisition_date=None, filename=None, footprint=None,
            level=None, cloud_coverage=None, size_mb=None, uuid=None, available=False,
            tile_loc=None, status=None):
        self.name = name
        self.date = acquisition_date
        self.level = level
        self.cc = cloud_coverage
        self.size_mb = size_mb
        self.uuid = uuid
        self.available = available
        self.tile_loc = tile_loc
        self.fname = filename
        self.geometry = footprint
        self.status = status

    def __repr__(self):
        return f"{self.name}"


    def save_to_db(self):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''INSERT INTO tiles (name, acquisition_date, level,
                cloud_coverage, size_mb, uuid, available, tile_loc, filename,
                footprint, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''',
                (self.name, self.date,  self.level, self.cc, self.size_mb, self.uuid, 
                    self.available, self.tile_loc, self.fname, self.geometryi, self.status))

    def update_tile(self):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''UPDATE tiles set acquisition_date=%s, level=%s, 
                cloud_coverage=%s, size_mb=%s, uuid=%s, filename=%s, footprint=%s WHERE name=%s;''',
                (self.date,  self.level, self.cc, self.size_mb, self.uuid, self.fname,
                    self.geometry, self.name))


    def update_tile_loc(self, loc):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''UPDATE tiles set tile_loc=%s
                WHERE name=%s;''', (loc, self.name))


    def update_tile_availability(self, available):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''UPDATE tiles set available=%s
                WHERE name=%s;''', (available, self.name))

    def update_tile_status(self, status):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''UPDATE tiles set status=%s
                WHERE name=%s;''', (status, self.name))

    @classmethod
    def load_by_tile_name(cls, name):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM tiles where name=%s', (name,))
                tile_data = cursor.fetchone()
                return cls(name=tile_data[1],
                           acquisition_date=tile_data[2],
                           level=tile_data[3],
                           cloud_coverage=tile_data[4],
                           size_mb=tile_data[5],
                           uuid=tile_data[6],
                           available=tile_data[7],
                           tile_loc=tile_data[8],
                           filename=tile_data[9],
                           footprint=tile_data[10],
                           status=tile_data[11])

    @classmethod
    def load_by_tile_id(cls, _id):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM tiles where id=%s', (_id,))
                tile_data = cursor.fetchone()
                return cls(name=tile_data[1],
                           acquisition_date=tile_data[2],
                           level=tile_data[3],
                           cloud_coverage=tile_data[4],
                           size_mb=tile_data[5],
                           uuid=tile_data[6],
                           available=tile_data[7],
                           tile_loc=tile_data[8],
                           filename=tile_data[9],
                           footprint=tile_data[10],
                           status=tile_data[11])

    @classmethod
    def load_by_tile_uuid(cls, uuid):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM tiles where uuid=%s', (uuid,))
                tile_data = cursor.fetchone()
                return cls(name=tile_data[1],
                           acquisition_date=tile_data[2],
                           level=tile_data[3],
                           cloud_coverage=tile_data[4],
                           size_mb=tile_data[5],
                           uuid=tile_data[6],
                           available=tile_data[7],
                           tile_loc=tile_data[8],
                           filename=tile_data[9],
                           footprint=tile_data[10],
                           status=tile_data[11])

    @staticmethod
    def get_tiles():
        """
        Return a list of all the tile names
        """
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT name FROM tiles')
                return [item[0] for item in cursor.fetchall()]


    @staticmethod
    def get_tiles_incomplete(tile_loc):
        """
        Return a list of all the tile's id (for the same batch )
        with an empty status
        """
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT id from tiles WHERE
                                  tile_loc=%s AND
                                  status is NULL''', (tile_loc,))
                return [item[0] for item in cursor.fetchall()]

    @staticmethod
    def get_tiles_with_status(tile_loc, status):
        """
        Return a list of all the tile's id (for the same batch)
        with a status

        status:
         - ready
         - corrupted

        """
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT id from tiles WHERE
                                 tile_loc=%s AND
                                 status=%s;
                               ''', (tile_loc, status))
                return [item[0] for item in cursor.fetchall()]


    @staticmethod
    def get_downloadable(tile_loc):
        """
        Return a list of all the tile's uuid with equal tile_loc (same batch) field
        that are downloadable
        """
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT uuid from tiles WHERE
                                 tile_loc=%s AND
                                 uuid is NOT NULL AND
                                 available=False AND
                                 status is NULL;
                               ''', (tile_loc,))
                return [item[0] for item in cursor.fetchall()]


    @staticmethod
    def get_tiles_fname_from_id(tiles_id):
        """
        Return a list of all the tile's fname from a list of ids

        """
        with connect() as connection:
            with connection.cursor() as cursor:
                sql = '''SELECT filename from tiles WHERE
                                 id = ANY(%(param_arr)s);'''
                cursor.execute(sql, {'param_arr': tiles_id})
                return [item[0] for item in cursor.fetchall()]
