import pandas as pd
from upath import UPath
import os
import pyarrow as pa
import numpy as np
import pyarrow.parquet as pq
from typing import Optional
from scipy.sparse import csr_matrix, lil_matrix
import json
import pickle
from dagster import (
    OutputContext,
    UPathIOManager,
    Output,
    MetadataValue,
    InputContext,
    Config
)


from modules import log



class PandasParquetIOManager(UPathIOManager):
    extension: str = ".parquet"
    

    def __init__(self, base_path=None, **kwargs):
        # Use "xyz" as the default base path if none is provided
        super().__init__(base_path=base_path or UPath(os.getenv("STORAGE_PATH")), **kwargs)

    @property
    def base_path(self):
        """Override base_path to use the instance's _base_path."""
        return self._base_path
    

    # def create_folder(self, folder_path, folder_name):
    #     if not os.path.isdir(folder_path + folder_name):
    #         new_path = folder_path + folder_name
    #         os.makedirs(new_path)
        

    def dump_to_path(self, context: OutputContext, obj: object, path: UPath):
        
        storage_path = os.getenv('STORAGE_PATH')
        
       
       # Determine the file extension based on the object type
        if isinstance(obj, (list, dict)):
            self.extension = ".json"
        elif isinstance(obj, str):
            self.extension = ".txt"
        elif isinstance(obj, (np.ndarray, csr_matrix, lil_matrix)):
            self.extension = ".pkl"
        else:
            self.extension = ".parquet"

        # path = UPath(storage_path)
        if context.has_partition_key:
            path =  UPath(str(storage_path) + '/' +str(context.asset_key.path[0]) + '/' + str(context.asset_partition_key)  + str(self.extension))
        else:
            path =  UPath(str(storage_path) + '/' +str(context.asset_key.path[0]) + str(self.extension))
            
        
        preview, rows, columns = self.save_files(context, obj, path)
  

        metadata = {
            "num_rows": rows,
            "column_count": columns,
            "preview": preview,
        }

        if context.metadata:
            metadata.update(context.metadata)

        context.add_output_metadata(metadata)


        return obj  




    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
    
        # 1. Identify base path (no extension yet)
        storage_path = os.getenv('STORAGE_PATH')
        if context.has_partition_key:
            # e.g. /my_storage/some_asset/partition_key
            base_path = UPath(os.path.join(storage_path, context.asset_key.path[0], context.asset_partition_key))
        else:
            # e.g. /my_storage/some_asset
            base_path = UPath(os.path.join(storage_path, context.asset_key.path[0]))

        # 2. Define recognized extensions and how to read them
        EXTENSION_READERS = {
            ".parquet": pd.read_parquet,
            ".json": lambda file: json.load(file),
            ".txt": lambda file: file.read(), # often .txt can be read as CSV with default settings
            ".pkl": pd.read_pickle,
            "": pd.read_pickle,    # no extension => assume pickle
        }

        # 3. Try each extension in turn
        for ext, read_func in EXTENSION_READERS.items():
            candidate_path = base_path.with_suffix(ext)  # e.g. base_path + ".parquet"
            if candidate_path.exists():
                # 4. Once we find a match, open and use the corresponding reader
                try:

                    with candidate_path.open("rb" if ext in [".parquet", ".csv", ".pkl", ""] else "r") as file:
                        return read_func(file)
                except:
                    log.log_info("parquet_io_manager: No File Found")
                    # If we fail to read, try the next extension
                    continue

        # If none of the candidate paths exist, handle gracefully
        # raise FileNotFoundError(f"No file found at {base_path} with any recognized extension: {list(EXTENSION_READERS.keys())}")



    def save_files(self, context: OutputContext, obj: object, path: UPath):
        
        if isinstance(obj, pd.DataFrame):
            with path.open("wb") as file:
                obj.to_parquet(file, compression="snappy")
            preview = MetadataValue.md(obj.head(100).to_markdown())
            rows, columns = obj.shape

        elif isinstance(obj, pa.Table):
            with path.open("wb") as file:
                pq.write_table(obj, file)
            preview = None
            rows, columns = obj.num_rows, obj.num_columns

        elif isinstance(obj, str):
            with path.open("w") as file:
                file.write(obj)
            preview = MetadataValue.md(obj)
            rows, columns = 1, 1

        elif isinstance(obj, int):
            preview = f"Inserted {obj} records"
            rows, columns = obj, None

        elif isinstance(obj, list):
            new_list=[]
            for i in obj:
                if isinstance(i, pd.DataFrame):
                    i=i.to_dict(orient='records')
                elif isinstance(i, np.ndarray):  # Check for NumPy array
                    i = i.tolist()        # Convert to list of lists

                new_list.append(i)

            with path.open("w") as file:
                json.dump(new_list, file)
            preview = MetadataValue.md(json.dumps(new_list, indent=2))
            rows, columns = len(new_list), None if isinstance(new_list, list) else len(new_list.keys())

        elif isinstance(obj, dict):
            new_dict = {}
            for key, value in obj.items():
                if isinstance(value, pd.DataFrame):
                    value=value.to_dict(orient='records')
                elif isinstance(value, np.ndarray):  # Check for NumPy array
                    value = value.tolist()        # Convert to list of lists
                new_dict[key] = value
                

            with path.open("w") as file:
                json.dump(new_dict, file)
            preview = MetadataValue.md(json.dumps(new_dict, indent=2))
            rows, columns = len(new_dict), None if isinstance(new_dict, list) else len(obj.keys())



        else:
            preview = ""
            rows, columns = 0, 0


        return preview, rows, columns

                    
class QueryModel(Config):
    query: Optional[str] = None



class DateRange(Config):
    start_date: Optional[str] = None
    end_date: Optional[str] = None