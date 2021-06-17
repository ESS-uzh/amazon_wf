from amazon_wf.database import CursorFromConnectionPool

class Biodivmap:

    def __init__(self, tile_id, raster_loc=None, out_loc=None, proc_status=None):
        self.tile_id = tile_id
        self.raster_loc = raster_loc
        self.out_loc = out_loc
        self.proc_status = proc_status

    def __repr__(self):
        return f"{self.tile_id}"


    def save_to_db(self):
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''INSERT INTO biodivmap (tile_id, raster_loc, proc_status)
                VALUES (%s, %s, %s);''',
                (self.tile_id, self.raster_loc,  self.proc_status))

    def update_proc_status(self, proc_status):
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''UPDATE biodivmap SET proc_status=%s 
            where tile_id=%s;''',(proc_status, self.tile_id))

    def update_out_loc(self, loc):
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''UPDATE biodivmap SET out_loc=%s 
            where tile_id=%s;''',(loc, self.tile_id))


    @classmethod
    def load_by_proc_id(cls, proc_id):
        with CursorFromConnectionPool() as cursor:
            cursor.execute('SELECT * FROM biodivmap where proc_id=%s;', (proc_id,))
            biodivmap_data = cursor.fetchone()
            return cls(tile_id=biodivmap_data[1],
                       raster_loc=biodivmap_data[2],
                       out_loc=biodivmap_data[3],
                       proc_status=biodivmap_data[4])

    @staticmethod
    def get_proc_id(tile_id):
        """
        Return a list of all the proc id with empty status field
        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''SELECT proc_id from biodivmap WHERE
                              tile_loc=%s AND
                              proc_status is NULL;''', (tile_loc,))
            return [item[0] for item in cursor.fetchall()]

    @staticmethod
    def get_procs_and_tiles_id_with_proc_status(tiles_id, proc_status):
        """
        Return a list of all the (proc_id, tile_id) for the same batch
        with a status

        status:
         - raster
         - pca
         - error_pca
         - pca_ready
         - out
         - error_out

        """
        with CursorFromConnectionPool() as cursor:
            cursor.execute('''SELECT proc_id, tile_id from biodivmap WHERE
                             tile_id in %s AND
                             proc_status=%s;
                           ''', (tuple(tiles_id), proc_status))
            return [item for item in cursor.fetchall()]


    @staticmethod
    def get_tiles_id_with_any_proc_status(tiles_id):
        """
        Return a list of all the proc id (for the same batch)
        with a status
        """
        with CursorFromConnectionPool() as cursor:
            sql = '''SELECT tile_id from biodivmap WHERE
                             tile_id = ANY(%(param_arr)s) AND
                             proc_status is NOT NULL;
                           '''
            cursor.execute(sql, {'param_arr': tiles_id})
            return [item[0] for item in cursor.fetchall()]
