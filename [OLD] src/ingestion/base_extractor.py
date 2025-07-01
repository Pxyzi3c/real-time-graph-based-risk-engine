from abc import ABC, abstractmethod
import pandas as pd

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """Abstract method to extract data."""
        pass