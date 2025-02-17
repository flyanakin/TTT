import os
from typing import Sequence

import pandas as pd
from dagster import (
    IOManager,
    io_manager,
    OutputContext,
    InputContext,
)
from TreasureTrove.resources import WAREHOUSE_PATH


class CsvIOManager(IOManager):
    """
    使用场景，单个文件，没有分区，支持增量写入
    """
    def __get_path(self, asset_path: Sequence[str]) -> tuple[str, str, str]:
        root_path = WAREHOUSE_PATH
        dir_path = None
        for _dir in asset_path[:-1]:
            if dir_path is None:
                dir_path = os.path.join(root_path, _dir)
            else:
                dir_path = os.path.join(dir_path, _dir)

        file_name = asset_path[-1]
        file_path = os.path.join(dir_path, f"{file_name}.csv")

        return dir_path, file_name, file_path

    def __column_check(self, obj: pd.DataFrame, file_path: str, context: OutputContext):
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            if set(obj.columns) != set(df.columns):
                context.log.info(f"文件{file_path}的列名不匹配，将覆盖文件")
                context.log.info(f"文件{file_path}的列名:{df.columns}")
                context.log.info(f"取数结果的列名:{obj.columns}")
                raise Exception(f"文件{file_path}的列名不匹配")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        dir_path, file_name, file_path = self.__get_path(context.asset_key.path)
        # 对已存在的文件，进行增量写入
        if os.path.exists(file_path):
            context.log.info(f"文件{file_path}已存在，将进行增量写入")
            df = pd.read_csv(file_path)
            self.__column_check(obj, file_path, context)
            df = pd.concat([df, obj], ignore_index=True)
            return df.to_csv(file_path, index=False)
        else:
            context.log.info(f"文件{file_path}不存在，将创建文件和文件夹")
            os.makedirs(dir_path, exist_ok=True)
            return obj.to_csv(file_path, index=False)

    def load_input(self, context: InputContext):
        dir_path, file_name, file_path = self.__get_path(context.upstream_output.asset_key.path)
        if not os.path.exists(dir_path):
            raise Exception(f"文件夹{dir_path}不存在")
        context.log.info(f"读取文件:{file_name}.csv \n从文件夹{dir_path}")
        return pd.read_csv(file_path)


@io_manager
def csv_io_manager():
    return CsvIOManager()
