from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        """Abstract method to transform data."""
        pass