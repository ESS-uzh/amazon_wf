from database import connect
import pdb

class Biodivmap:

    def __init__(self, tile_id, raster_loc=None, out_loc=None, proc_status=None):
        self.tile_id = tile_id
        self.raster_loc = raster_loc
        self.out_loc = out_loc
        self.proc_status = proc_status

    def __repr__(self):
        return f"{self.tile_id}"


    def save_to_db(self):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''INSERT INTO biodivmap (tile_id, raster_loc, proc_status)
                VALUES (%s, %s, %s);''',
                (self.tile_id, self.raster_loc,  self.proc_status))

    @classmethod
    def load_by_tile_id(cls, tile_id):
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM tiles where tile_id=%s;', (tile_id,))
                biodvimap_data = cursor.fetchone()
                return cls(tile_id=biodivmap_data[1],
                           raster_loc=biodivmap_data[2],
                           out_loc=biodivmap_data[3],
                           proc_status=biodivmap_data[4])

    @staticmethod
    def get_proc_id(tile_id):
        """
        Return a list of all the proc id with empty status field
        """
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT proc_id from biodivmap WHERE
                                  tile_loc=%s AND
                                  proc_status is NULL;''', (tile_loc,))
                return [item[0] for item in cursor.fetchall()]

    @staticmethod
    def get_proc_id_with_proc_status(tiles_id, proc_status):
        """
        Return a list of all the proc id (for the same batch)
        with a status

        status:
         - raster
         - pca
         - done
         - error raster
         - error pca
         - error out

        """
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT proc_id from biodivmap WHERE
                                 tile_loc = ANY(%(param_arr)s) AND
                                 proc_status=%s;
                               ''', (tiles_id, status))
                return [item[0] for item in cursor.fetchall()]


    @staticmethod
    def get_tiles_id_with_any_proc_status(tiles_id):
        """
        Return a list of all the proc id (for the same batch)
        with a status
        """
        with connect() as connection:
            with connection.cursor() as cursor:
                sql = '''SELECT tile_id from biodivmap WHERE
                                 tile_id = ANY(%(param_arr)s) AND
                                 proc_status is NOT NULL;
                               '''
                cursor.execute(sql, {'param_arr': tiles_id})
                return [item[0] for item in cursor.fetchall()]
