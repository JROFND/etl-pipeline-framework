from abc import ABC, abstractmethod
import pandas as pd


class Transformer(ABC):
    """
    Base transformer class.
    """

    @abstractmethod
    def transform(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform dataframe.
        """
        pass