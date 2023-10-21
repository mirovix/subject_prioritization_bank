import tempfile


class TempDirManager(object):
    def __init__(self):
        self.tempfolder = tempfile.TemporaryDirectory()

    def get_dir_name(self):
        return self.tempfolder.name

    def __del__(self):
        self.tempfolder.cleanup()
