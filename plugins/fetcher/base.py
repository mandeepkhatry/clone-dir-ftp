import os
from abc import abstractmethod
class FetcherPlugin(object):
    def __init__(self, config, archive_path, *args, **kwargs):
        self.config = config
        self.archive_path = archive_path
        self._patterns = None
    def file_patterns(self):
        """
        :return:
        """
        if self._patterns is None:
            patterns = self.config.get('file_patterns', [])
            for pattern in patterns:
                pass
    def prepare_path(self, file_name,folder):
        """
        :param file_name:
        :return:
        """
        return os.path.join(self.archive_path,folder, file_name)
    @abstractmethod
    def run(self, progress=None):
        """
        :param progress:
        :return:
        """
    @classmethod
    def validation_config(cls):
        return []
    @classmethod
    def on_run_config(cls):
        return []
    @classmethod
    def schedulable(cls):
        return True