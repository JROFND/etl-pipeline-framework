from pathlib import Path
from typing import Iterator


class FileManager:
    """
    Handles input file discovery.
    """


    def __init__(
        self,
        input_folder: str,
        pattern: str = "*.csv"
    ):

        self.input_folder = Path(input_folder)
        self.pattern = pattern


        if not self.input_folder.exists():

            raise FileNotFoundError(
                f"Folder not found: {self.input_folder}"
            )


    def get_files(self) -> Iterator[Path]:
        """
        Return files recursively.
        """

        yield from self.input_folder.rglob(
            self.pattern
        )